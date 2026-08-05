/**************************************************************
*
* File:: dispatch_analysis.c
*
* Description:: Reads a fixed-length-record police dispatch file and uses
* multiple threads to compute response-time statistics per call type for the
* whole city and for two chosen neighborhoods, then prints the results as nine
* tables and as ASCII box-and-whisker plots.
*
**************************************************************/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <math.h>

#define MAX_FIELDS 64
#define NAME_LEN 64
#define BATCH 2000            /* records pulled per pread, to amortize the syscall */

/* The record layout is not hard-coded; it is described by header.txt. Storing
 * each field's byte offset lets the rest of the code jump straight to a field
 * by position instead of scanning, since the file has no separators. */
typedef struct
    {
    char name[NAME_LEN];
    int  width;
    int  offset;
    } FieldInfo;

/* A grow-as-needed array of one metric's values for one bucket. Every value is
 * kept (not just a running total) because median and quartiles can only be
 * found from the full set once it is sorted. */
typedef struct
    {
    long *values;
    int   count;
    int   capacity;
    } ValueList;

/* One call type with its three metric lists:
 * metric[0]=dispatch-received, [1]=onscene-enroute, [2]=onscene-received. */
typedef struct
    {
    char      callType[26];   /* 25-char field + terminator */
    ValueList metric[3];
    } CallTypeEntry;

/* One reporting scope (the whole city, or one neighborhood). It holds the
 * per-call-type buckets plus a TOTAL across all call types, and its own lock.
 * A separate lock per scope lets a thread updating one scope run independently
 * of a thread updating a different one. */
typedef struct
    {
    char            label[40];
    CallTypeEntry  *entries;
    int             entryCount;
    int             entryCap;
    ValueList       total[3];
    pthread_mutex_t lock;
    } Scope;

/* Global so every thread adds to the one shared structure directly, as the
 * design requires (no per-thread copies merged at the end). */
Scope scopes[3];

/* Everything a worker thread needs: the shared read-only file/field info, and
 * the half-open record range [startRecord, endRecord) that this thread alone
 * owns. Each thread gets its own copy, so reading these needs no lock. */
typedef struct
    {
    int        dataFd;
    int        recordLength;
    FieldInfo *fFinalDesc;
    FieldInfo *fOrigDesc;
    FieldInfo *fReceived;
    FieldInfo *fDispatch;
    FieldInfo *fEnroute;
    FieldInfo *fOnscene;
    FieldInfo *fArea;
    char      *areavalue1;
    char      *areavalue2;
    long       startRecord;
    long       endRecord;
    } ThreadArg;

/* The computed statistics for one list, filled once and then printed. */
typedef struct
    {
    int    count;
    long   min;
    long   max;
    double q1;
    double median;
    double q3;
    double iqr;
    double lowerBound;
    double upperBound;
    double mean;
    double stddev;
    } Stats;

/* Resolve a field by name to its layout entry. Returning NULL when the name is
 * absent forces the caller to treat a missing field as an error instead of
 * silently reading from offset 0. */
FieldInfo *findFields(FieldInfo fields[], int count, const char *name)
    {
    for (int i = 0; i < count; i++)
        if (strcmp(fields[i].name, name) == 0)
            return &fields[i];
    return NULL;
    }

/* Copy a fixed-width field out of a record and force a terminator, so the slice
 * is always a safe C string even when the stored value fills the whole width. */
void getField(const char *record, FieldInfo *f, char *dest)
    {
    memcpy(dest, record + f->offset, f->width);
    dest[f->width] = '\0';
    }

/* Leap-year rule, needed so February's length is right when counting days. */
int isLeap(int year)
    {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    }

/* Turn a fixed-format datetime into a single second count so two datetimes can
 * be subtracted to get an elapsed time. Returns -1 for a blank field so a
 * missing endpoint can be detected and that metric skipped, not counted as 0.
 * Absolute calendar accuracy is unnecessary because the values are only ever
 * subtracted from each other. */
long toSeconds(const char *s)
    {
    if (s[0] == '\0' || s[0] == ' ')
        return -1;

    int month  = (s[0]-'0')*10 + (s[1]-'0');
    int day    = (s[3]-'0')*10 + (s[4]-'0');
    int year   = (s[6]-'0')*1000 + (s[7]-'0')*100 + (s[8]-'0')*10 + (s[9]-'0');
    int hour   = (s[11]-'0')*10 + (s[12]-'0');
    int minute = (s[14]-'0')*10 + (s[15]-'0');
    int second = (s[17]-'0')*10 + (s[18]-'0');

    /* Convert the 12-hour clock to 24-hour BEFORE using it, or every afternoon
     * time would be wrong and produce negative or huge differences. */
    if (s[20] == 'P' && hour != 12)
        hour = hour + 12;
    if (s[20] == 'A' && hour == 12)
        hour = 0;

    static const int daysInMonth[12] = {31,28,31,30,31,30,31,31,30,31,30,31};

    long days = 0;
    for (int y = 1970; y < year; y++)
        days += isLeap(y) ? 366 : 365;
    for (int m = 1; m < month; m++)
        {
        days += daysInMonth[m-1];
        if (m == 2 && isLeap(year))
            days += 1;
        }
    days += day - 1;

    return days * 86400L + hour * 3600L + minute * 60L + second;
    }

/* Append one value, doubling the backing array only when it is full. Doubling
 * keeps the cost of N appends linear, so collecting every value stays cheap even
 * though the final count is unknown until all records have been read. */
void appendValue(ValueList *list, long v)
    {
    if (list->count == list->capacity)
        {
        int newCap = (list->capacity == 0) ? 16 : list->capacity * 2;
        list->values = realloc(list->values, newCap * sizeof(long));
        list->capacity = newCap;
        }
    list->values[list->count] = v;
    list->count++;
    }

/* Return the entry for a call type, creating it the first time one is seen. The
 * set of call types is not known in advance, so the bucket array is discovered
 * and grown as records stream in. A realloc here can move the array, so the
 * caller must use the returned pointer right away and never cache an old one. */
CallTypeEntry *findOrCreateCallType(Scope *scope, const char *name)
    {
    for (int i = 0; i < scope->entryCount; i++)
        if (strcmp(scope->entries[i].callType, name) == 0)
            return &scope->entries[i];

    if (scope->entryCount == scope->entryCap)
        {
        int newCap = (scope->entryCap == 0) ? 32 : scope->entryCap * 2;
        scope->entries = realloc(scope->entries, newCap * sizeof(CallTypeEntry));
        scope->entryCap = newCap;
        }

    CallTypeEntry *e = &scope->entries[scope->entryCount];
    strncpy(e->callType, name, 25);
    e->callType[25] = '\0';
    for (int m = 0; m < 3; m++)
        {
        e->metric[m].values = NULL;
        e->metric[m].count = 0;
        e->metric[m].capacity = 0;
        }
    scope->entryCount++;
    return e;
    }

/* Add one record's three time differences to a scope, both to the call-type
 * bucket and to the scope's TOTAL row. A difference of -1 means that metric was
 * missing or out-of-order for this record, so it is skipped: this is why the
 * three metrics legitimately end up with different counts. */
void addRecordToScope(Scope *scope, const char *callType, long d1, long d2, long d3)
    {
    CallTypeEntry *e = findOrCreateCallType(scope, callType);
    long diffs[3] = { d1, d2, d3 };
    for (int m = 0; m < 3; m++)
        if (diffs[m] >= 0)
            {
            appendValue(&e->metric[m], diffs[m]);
            appendValue(&scope->total[m], diffs[m]);
            }
    }

/* Put a scope in a clean empty state before any record is added. The lists
 * start NULL/zero so the first append is what triggers allocation, and the
 * mutex is created here because threads lock it as soon as they start. */
void initScope(Scope *scope, const char *label)
    {
    strncpy(scope->label, label, sizeof(scope->label) - 1);
    scope->label[sizeof(scope->label) - 1] = '\0';
    scope->entries = NULL;
    scope->entryCount = 0;
    scope->entryCap = 0;
    for (int m = 0; m < 3; m++)
        {
        scope->total[m].values = NULL;
        scope->total[m].count = 0;
        scope->total[m].capacity = 0;
        }
    pthread_mutex_init(&scope->lock, NULL);
    }

/* Ascending order for qsort; written branch-free to avoid long subtraction
 * overflowing an int return. */
int compareLong(const void *a, const void *b)
    {
    long x = *(const long *)a;
    long y = *(const long *)b;
    return (x > y) - (x < y);
    }

/* Middle value of an already-sorted run; averages the two middles for an even
 * count. Used for the median and, on each half, for Q1 and Q3. */
double medianOf(long *v, int n)
    {
    if (n == 0)
        return 0;
    if (n % 2 == 1)
        return v[n / 2];
    return (v[n / 2 - 1] + v[n / 2]) / 2.0;
    }

/* Compute every statistic for one list. Sorting once up front gives min, max,
 * median and quartiles directly by position; the bounds then mark the outlier
 * fence, and a second pass gives mean and standard deviation. The list is
 * sorted in place, which also makes the output order deterministic. */
Stats computeStats(ValueList *list)
    {
    Stats s;
    s.count = list->count;
    if (list->count == 0)
        {
        s.min = s.max = 0;
        s.q1 = s.median = s.q3 = s.iqr = 0;
        s.lowerBound = s.upperBound = s.mean = s.stddev = 0;
        return s;
        }

    qsort(list->values, list->count, sizeof(long), compareLong);
    long *v = list->values;
    int   n = list->count;

    s.min = v[0];
    s.max = v[n - 1];

    /* Quartiles use the lower and upper halves, excluding the overall median
     * when the count is odd (Tukey's method). */
    s.median = medianOf(v, n);
    s.q1 = medianOf(v, n / 2);
    s.q3 = medianOf(v + (n - n / 2), n / 2);
    s.iqr = s.q3 - s.q1;

    /* Bounds are clamped to the real data so they never claim a value that did
     * not occur. */
    double lb = s.q1 - 1.5 * s.iqr;
    s.lowerBound = (lb > s.min) ? lb : s.min;
    double ub = s.q3 + 1.5 * s.iqr;
    s.upperBound = (ub < s.max) ? ub : s.max;

    double sum = 0;
    for (int i = 0; i < n; i++)
        sum += v[i];
    s.mean = sum / n;

    double sqSum = 0;
    for (int i = 0; i < n; i++)
        sqSum += (v[i] - s.mean) * (v[i] - s.mean);
    s.stddev = sqrt(sqSum / n);

    return s;
    }

/* Order call types by name so the rows come out identical on every run no
 * matter which thread happened to see a call type first; without this the
 * values are correct but the row order would wobble between runs. */
int compareEntry(const void *a, const void *b)
    {
    return strcmp(((const CallTypeEntry *)a)->callType,
                  ((const CallTypeEntry *)b)->callType);
    }

/* Print one table row in the fixed report column order. Position
 * statistics print as whole numbers; mean and stddev keep two decimals. */
void printRow(const char *name, Stats *s)
    {
    printf("%25s |%7d |%7ld |%7.0f |%7.0f |%7.0f |%10.2f |%7.0f |%7.0f |%8ld |%7.0f |%10.2f\n",
           name, s->count, s->min, s->lowerBound, s->q1, s->median,
           s->mean, s->q3, s->upperBound, s->max, s->iqr, s->stddev);
    }

/* Print one numeric page: heading, column header, the TOTAL row, then one row
 * per call type. Stats are computed here, once, after all values are collected. */
void printPage(Scope *scope, int metric, const char *metricName)
    {
    printf("\n%s   -  %s\n\n", scope->label, metricName);
    printf("%25s |%7s |%7s |%7s |%7s |%7s |%10s |%7s |%7s |%8s |%7s |%10s\n",
           "Call Type", "count", "min", "LB", "Q1", "med", "mean",
           "Q3", "UB", "max", "IQR", "stddev");

    Stats total = computeStats(&scope->total[metric]);
    printRow("TOTAL", &total);

    for (int i = 0; i < scope->entryCount; i++)
        {
        Stats s = computeStats(&scope->entries[i].metric[metric]);
        printRow(scope->entries[i].callType, &s);
        }
    }

/* ----- ASCII box-and-whisker rendering ------------------------------------
 * Every glyph in the legend maps to a number already in Stats, so
 * this only draws; it computes nothing new. Each row's axis is scaled to that
 * row's own [lowerBound, upperBound] fence (LB on the left bar, UB on the
 * right), so the box always fills the line no matter how far the real outliers
 * reach; the outliers themselves only need a flag, so an 'o' is parked just
 * outside the fence whenever min < LB or max > UB.                            */
#define BOX_INNER 50            /* columns spanning lowerBound..upperBound      */
#define BOX_W (BOX_INNER + 4)   /* extra columns hold the outlier marks         */

/* Map a value to a column, clamped into the fence so an out-of-fence value
 * lands on the edge instead of writing past the end of the line buffer. */
int boxCol(double v, double lo, double hi)
    {
    if (hi <= lo) return 2;            /* all values equal -> no width to scale */
    if (v < lo) v = lo;
    if (v > hi) v = hi;
    return 2 + (int)((v - lo) / (hi - lo) * (BOX_INNER - 1) + 0.5);
    }

/* Draw one distribution as a single box-and-whisker line. */
void printBoxRow(const char *name, Stats *s)
    {
    if (s->count == 0)
        {
        printf("%25s | (no data)\n", name);
        return;
        }

    char line[BOX_W + 1];
    for (int i = 0; i < BOX_W; i++)
        line[i] = ' ';
    line[BOX_W] = '\0';

    double lo = s->lowerBound, hi = s->upperBound;
    int cLB = boxCol(lo, lo, hi);
    int cUB = boxCol(hi, lo, hi);
    int cQ1 = boxCol(s->q1, lo, hi);
    int cQ3 = boxCol(s->q3, lo, hi);
    int cMed = boxCol(s->median, lo, hi);

    /* Fill whiskers (bar to nearer quartile) and the box first, then stamp the
     * single-glyph markers on top so they stay visible. */
    for (int i = cLB; i <= cQ1; i++) line[i] = '-';
    for (int i = cQ1; i <= cQ3; i++) line[i] = '=';
    for (int i = cQ3; i <= cUB; i++) line[i] = '-';

    line[cLB] = '|';
    line[cUB] = '|';
    line[cQ1] = '[';
    line[cQ3] = ']';
    line[cMed] = '|';

    /* The mean is pulled by extreme values, so on skewed data it lands past the
     * fence; park it one column outside the bar so the LB/UB bar stays visible. */
    int cMean = (s->mean > hi) ? cUB + 1
              : (s->mean < lo) ? cLB - 1
              : boxCol(s->mean, lo, hi);
    line[cMean] = 'x';

    if (s->min < s->lowerBound) line[0] = 'o';
    if (s->max > s->upperBound) line[BOX_W - 1] = 'o';

    printf("%25s %s  min=%ld max=%ld\n", name, line, s->min, s->max);
    }

/* Print one box-plot page: the same TOTAL + per-call-type rows as the numeric
 * page, drawn as box-and-whisker plots. */
void printBoxPage(Scope *scope, int metric, const char *metricName)
    {
    printf("\n%s   -  %s   (Box & Whisker)\n", scope->label, metricName);
    printf("    legend: o=outlier |=LB/UB [=Q1 ]=Q3 |=median x=mean "
           "==box -=whisker\n\n");

    Stats total = computeStats(&scope->total[metric]);
    printBoxRow("TOTAL", &total);
    for (int i = 0; i < scope->entryCount; i++)
        {
        Stats s = computeStats(&scope->entries[i].metric[metric]);
        printBoxRow(scope->entries[i].callType, &s);
        }
    }

/* One worker thread. It reads its record range in batches (fewer read syscalls)
 * and does ALL the field extraction, datetime parsing, and difference math with
 * NO lock held -- that is the heavy work, and it runs fully in parallel. Only
 * the update of the shared structure is locked: the scope lock is taken to
 * find-or-create the call-type bucket and append the values, then released.
 * Doing the parsing lock-free is what lets the threads overlap; the city-wide
 * TOTAL scope, touched by every record, is the one real point of contention. */
void *processRecords(void *arg)
    {
    ThreadArg *t = (ThreadArg *)arg;
    char *buffer = malloc(BATCH * t->recordLength);

    long i = t->startRecord;
    while (i < t->endRecord)
        {
        long batch = t->endRecord - i;
        if (batch > BATCH)
            batch = BATCH;

        /* pread takes the offset as an argument, so threads share one fd safely
         * without a moving file pointer to race on. */
        pread(t->dataFd, buffer, batch * t->recordLength, i * t->recordLength);

        for (long b = 0; b < batch; b++)
            {
            char *record = buffer + b * t->recordLength;
            char finalDesc[64], areaStr[64];
            char receivedStr[64], dispatchStr[64], enrouteStr[64], onsceneStr[64];

            /* Use the final call-type description, falling back to the original
             * when the final one is blank. */
            getField(record, t->fFinalDesc, finalDesc);
            if (finalDesc[0] == '\0' || finalDesc[0] == ' ')
                getField(record, t->fOrigDesc, finalDesc);
            getField(record, t->fReceived, receivedStr);
            getField(record, t->fDispatch, dispatchStr);
            getField(record, t->fEnroute,  enrouteStr);
            getField(record, t->fOnscene,  onsceneStr);
            getField(record, t->fArea,     areaStr);

            long recvSec = toSeconds(receivedStr);
            long dispSec = toSeconds(dispatchStr);
            long enrSec  = toSeconds(enrouteStr);
            long onsSec  = toSeconds(onsceneStr);

            /* A difference counts only if both endpoints parsed and the result
             * is non-negative; otherwise it is -1 and gets skipped downstream.
             * (Equal timestamps give 0, which is valid, e.g. onview calls.) */
            long d1 = (recvSec >= 0 && dispSec >= 0 && dispSec - recvSec >= 0)
                      ? dispSec - recvSec : -1;
            long d2 = (enrSec >= 0 && onsSec >= 0 && onsSec - enrSec >= 0)
                      ? onsSec - enrSec : -1;
            long d3 = (recvSec >= 0 && onsSec >= 0 && onsSec - recvSec >= 0)
                      ? onsSec - recvSec : -1;

            /* Every record updates the city-wide TOTAL, and also a neighborhood
             * scope when its area matches. Each scope has its own lock, so the
             * two neighborhood scopes never block each other; the TOTAL lock,
             * taken for every record, is the one the threads actually contend on. */
            pthread_mutex_lock(&scopes[0].lock);
            addRecordToScope(&scopes[0], finalDesc, d1, d2, d3);
            pthread_mutex_unlock(&scopes[0].lock);

            if (strcmp(areaStr, t->areavalue1) == 0)
                {
                pthread_mutex_lock(&scopes[1].lock);
                addRecordToScope(&scopes[1], finalDesc, d1, d2, d3);
                pthread_mutex_unlock(&scopes[1].lock);
                }
            if (strcmp(areaStr, t->areavalue2) == 0)
                {
                pthread_mutex_lock(&scopes[2].lock);
                addRecordToScope(&scopes[2], finalDesc, d1, d2, d3);
                pthread_mutex_unlock(&scopes[2].lock);
                }
            }
        i += batch;
        }

    free(buffer);
    return NULL;
    }

int main (int argc, char *argv[])
    {
    /* Need the program name plus six arguments. Stopping here, before any later
     * code reads an argument that was not passed, makes the usage explicit. */
    if (argc != 7)
        {
        fprintf(stderr, "Usage %s <datafile> <headerfile> <threads> "
                "<subfield> <value1> <value2>\n", argv[0]);
        return 1;
        }

    /* Name the positional arguments so the rest of main reads by intent. */
    char *datafileName   = argv[1];
    char *headerfileName = argv[2];
    int   numThreads     = atoi(argv[3]);
    char *subField       = argv[4];
    char *areavalue1     = argv[5];
    char *areavalue2     = argv[6];

    /* atoi returns 0 on non-numeric text, so anything below 1 means a usable
     * thread count was not supplied. */
    if (numThreads < 1)
        {
        fprintf(stderr, "threads must be a positive number\n");
        return 1;
        }

    /* The sub-field chooses which record field the two values are matched
     * against, so only these two fields are supported. */
    if (strcmp(subField, "analysis_neighborhood") != 0 &&
        strcmp(subField, "police_district") != 0)
        {
        fprintf(stderr, "Subfield must be analysis_neighborhood or "
                "police_district\n");
        return 1;
        }

    int headerFd = open(headerfileName, O_RDONLY);
    if (headerFd == -1)
        {
        perror("Could not open header file");
        return 1;
        }

    /* header.txt is tiny, so one read pulls all of it. */
    char headerBuf[4096];
    int headerLen = read(headerFd, headerBuf, sizeof(headerBuf) - 1);
    close(headerFd);
    if (headerLen <= 0)
        {
        fprintf(stderr, "Header file is empty or unreadable\n");
        return 1;
        }
    headerBuf[headerLen] = '\0';   /* terminate so string functions can parse it */

    FieldInfo fields[MAX_FIELDS];
    int fieldCount = 0;
    int recordLength = 0;

    /* The record layout is defined by the header, not built into the program,
     * so derive each field's width and offset (and the total record length) at
     * runtime by walking "width: name" lines and accumulating the offset. */
    char *line = strtok(headerBuf, "\n");
    while (line != NULL && fieldCount < MAX_FIELDS)
        {
        int width = atoi(line);
        char *colon = strchr(line, ':');
        char *name = colon + 2;            /* skip ": " to the field name */

        fields[fieldCount].width = width;
        fields[fieldCount].offset = recordLength;
        strncpy(fields[fieldCount].name, name, NAME_LEN - 1);
        fields[fieldCount].name[NAME_LEN - 1] = '\0';

        recordLength += width;
        fieldCount++;
        line = strtok(NULL, "\n");
        }

    /* A record has many fields, but only these few drive the statistics, so
     * resolve just those offsets. The area field is whichever one the command
     * line named. */
    FieldInfo *fFinalDesc = findFields(fields, fieldCount, "call_type_final_desc");
    FieldInfo *fOrigDesc  = findFields(fields, fieldCount, "call_type_original_desc");
    FieldInfo *fReceived  = findFields(fields, fieldCount, "received_datetime");
    FieldInfo *fDispatch  = findFields(fields, fieldCount, "dispatch_datetime");
    FieldInfo *fEnroute   = findFields(fields, fieldCount, "enroute_datetime");
    FieldInfo *fOnscene   = findFields(fields, fieldCount, "onscene_datetime");
    FieldInfo *fArea      = findFields(fields, fieldCount, subField);

    /* If any required field is missing, every record read afterward would be
     * wrong, so fail now rather than produce garbage. */
    if (!fFinalDesc || !fOrigDesc || !fReceived ||
        !fDispatch || !fEnroute || !fOnscene || !fArea)
        {
        fprintf(stderr, "Header is missing a required field\n");
        return 1;
        }

    int dataFd = open(datafileName, O_RDONLY);
    if (dataFd == -1)
        {
        perror("Could not open data file");
        return 1;
        }

    /* The file stores no record count; since every record is the same length,
     * the count is just the file size divided by the record length. */
    struct stat st;
    if (fstat(dataFd, &st) == -1)
        {
        perror("Could not stat data file");
        close(dataFd);
        return 1;
        }
    long fileSize    = st.st_size;
    long recordCount = fileSize / recordLength;

    /* Scope 0 is the whole city; scopes 1 and 2 are the two chosen areas. */
    initScope(&scopes[0], "Total");
    initScope(&scopes[1], areavalue1);
    initScope(&scopes[2], areavalue2);

    const char *metricNames[3] =
        {
        "Dispatch Time - Received Time",
        "OnScene Time - Enroute Time",
        "OnScene Time - Received Time"
        };

    /* Start the wall-clock timer. */
    struct timespec startTime;
    struct timespec endTime;

    clock_gettime(CLOCK_REALTIME, &startTime);

    /* One thread owns a contiguous slice of records. Splitting by record (not
     * by byte) keeps each thread on whole records; the last thread takes the
     * remainder so nothing is dropped when the count does not divide evenly. */
    pthread_t threads[numThreads];
    ThreadArg args[numThreads];
    long per = recordCount / numThreads;

    for (int t = 0; t < numThreads; t++)
        {
        args[t].dataFd       = dataFd;
        args[t].recordLength = recordLength;
        args[t].fFinalDesc   = fFinalDesc;
        args[t].fOrigDesc    = fOrigDesc;
        args[t].fReceived    = fReceived;
        args[t].fDispatch    = fDispatch;
        args[t].fEnroute     = fEnroute;
        args[t].fOnscene     = fOnscene;
        args[t].fArea        = fArea;
        args[t].areavalue1   = areavalue1;
        args[t].areavalue2   = areavalue2;
        args[t].startRecord  = t * per;
        args[t].endRecord    = (t == numThreads - 1) ? recordCount : (t + 1) * per;
        pthread_create(&threads[t], NULL, processRecords, &args[t]);
        }

    /* Join before reading any stats: the structure must be fully built and no
     * thread still appending when the main thread starts to print. */
    for (int t = 0; t < numThreads; t++)
        pthread_join(threads[t], NULL);

    /* Sort each scope's call types so the row order is identical on every run. */
    for (int sc = 0; sc < 3; sc++)
        qsort(scopes[sc].entries, scopes[sc].entryCount,
              sizeof(CallTypeEntry), compareEntry);

    /* Nine tables: three scopes x three metrics. */
    for (int sc = 0; sc < 3; sc++)
        for (int m = 0; m < 3; m++)
            printPage(&scopes[sc], m, metricNames[m]);

    /* Stop the timer and report elapsed time. */
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec)
        {
        --sec;
        n_sec = n_sec + 1000000000L;
        }

    printf("Total Time was %ld.%09ld seconds for %s threads.\n", sec, n_sec, argv[3]);
    /* Box plots print AFTER the timer so the reported timing measures only
     * the threaded processing plus the nine tables, not this redisplay. */
    for (int sc = 0; sc < 3; sc++)
        for (int m = 0; m < 3; m++)
            printBoxPage(&scopes[sc], m, metricNames[m]);

    /* Release everything the program allocated and grew, and the locks. */
    close(dataFd);
    for (int sc = 0; sc < 3; sc++)
        {
        for (int m = 0; m < 3; m++)
            free(scopes[sc].total[m].values);
        for (int e = 0; e < scopes[sc].entryCount; e++)
            for (int m = 0; m < 3; m++)
                free(scopes[sc].entries[e].metric[m].values);
        free(scopes[sc].entries);
        pthread_mutex_destroy(&scopes[sc].lock);
        }

    return 0;
    }
