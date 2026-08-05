# Police Dispatch Response-Time Analysis

A multithreaded C program that analyzes police computer-aided-dispatch (CAD)
records stored as fixed-length records (FLR) and reports response-time
statistics per call type — for a whole city and for two chosen neighborhoods.
The record layout is data-driven (described by a separate spec file), so the
same program works on any dataset that follows the same format.

> A systems-programming project focused on fixed-length-record
> parsing, POSIX threads, and streaming statistics over a large file.

## Features

- **Layout-driven parsing** — field names and widths come from `header.txt`, so
  the parser is not tied to hard-coded byte offsets.
- **Multithreaded aggregation** — worker threads process the dataset in
  parallel and their partial results are merged.
- **Per-call-type statistics** — count, min/max, mean, standard deviation,
  quartiles (Q1/median/Q3), IQR, and lower/upper whisker bounds.
- **City-wide and per-neighborhood views** — the same statistics for the full
  dataset and for two neighborhoods chosen on the command line.
- **Box-and-whisker plots** — a text visualization of each distribution.
- **Scales to large inputs** — developed against a ~500,000-record (~340 MB)
  dataset; a small sample is bundled for a quick run.

## Quick start

### Prerequisites

- Linux
- GCC and GNU Make
- POSIX threads (`pthread`) and the math library (`libm`) — both standard on Linux

### Build

```bash
git clone https://github.com/build4me2/police-dispatch-analysis.git
cd police-dispatch-analysis
make
```

This produces the `dispatch_analysis` executable.

### Run

A small sample dataset (`Law5K.dat`, ~5,000 records) is included, so you can run
it immediately:

```bash
make run
```

or invoke it directly:

```bash
./dispatch_analysis Law5K.dat header.txt 1 police_district BAYVIEW MISSION
```

## Command-line arguments

```
./dispatch_analysis <data-file> <layout-file> <threads> <group-field> <area-1> <area-2>
```

| Argument | Meaning |
| --- | --- |
| `data-file` | The fixed-length-record dataset (e.g. `Law5K.dat`). |
| `layout-file` | Field layout describing each field's name and width (`header.txt`). |
| `threads` | Number of worker threads to use. |
| `group-field` | The record field used to select neighborhoods (e.g. `police_district`). |
| `area-1`, `area-2` | The two values of that field to break out (e.g. `BAYVIEW`, `MISSION`). |

## Example output

```text
Total   -  Dispatch Time - Received Time

                Call Type | count |  min |   Q1 |  med |    mean |   Q3 |    max |  stddev
                    TOTAL |  5000 |    0 |   85 |  191 | 1466.77 |  557 |  68673 | 4413.28
       416 for medication |    28 |    0 |   92 |  124 |  482.14 |  250 |   8608 | 1573.19
        ASSAULT / BATTERY |    29 |    0 |   94 |  144 | 4215.97 |  266 |  68673 | 13703.33
     ASSAULT / BATTERY DV |   460 |    0 |   98 |  150 | 1383.91 |  276 |  46140 | 4160.41
```

(Values are response times in seconds. The full output also includes the lower
and upper whisker bounds, the IQR, and box-and-whisker plots.)

## How it works

1. The field layout is read from the spec file to learn each field's name,
   width, and offset within a record.
2. The dataset is divided among worker threads, each of which scans its portion
   and accumulates, per call type, the values needed for the statistics.
3. The threads' partial results are merged into a single set of per-call-type
   distributions.
4. For each call type, summary statistics are computed and printed as a table
   for the whole dataset and for each of the two selected neighborhoods.

## Project structure

| Path | Responsibility |
| --- | --- |
| `dispatch_analysis.c` | The analyzer: parsing, threading, statistics, and output. |
| `header.txt` | Fixed-length-record field layout (name and width per field). |
| `Law5K.dat` | A small sample dataset (~5,000 records) for a quick run. |
| `Makefile` | Build and a `run` target that uses the bundled sample. |

## Notes

- The full ~500,000-record dataset (~340 MB) is not included because of its
  size; the program runs on any dataset matching the `header.txt` layout.
- The bundled data is drawn from a public police-dispatch dataset and is
  included only as a small sample to make the program runnable out of the box.
