#**************************************************************
#* File:: Makefile
#*
#* Description:: Builds the police-dispatch response-time analyzer and provides
#* a `run` target that analyzes the bundled sample dataset.
#*
#**************************************************************

CC = gcc
CFLAGS = -Wall -g -I.
LIBS = -lpthread -lm

TARGET = dispatch_analysis

# Sample run against the bundled dataset: data file, field-layout file, thread
# count, the field to group two neighborhoods by, and the two neighborhoods.
RUNOPTIONS = Law5K.dat header.txt 1 police_district BAYVIEW MISSION

$(TARGET): $(TARGET).o
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)

run: $(TARGET)
	./$(TARGET) $(RUNOPTIONS)

clean:
	rm -f *.o $(TARGET)
