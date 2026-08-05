# Police Dispatch Response-Time Analysis

A multithreaded C program that analyzes police computer-aided-dispatch (CAD)
records stored as fixed-length records (FLR) and reports response-time
statistics per call type — for the whole city and for two chosen neighborhoods.

## What it does

- Reads a fixed-length-record dataset whose field layout is described by a
  separate spec file (`header.txt`), so the parser is driven by the layout
  rather than hard-coded offsets.
- Uses multiple worker threads to compute, per call type, statistics such as
  the count and the response-time distribution (min, quartiles, median, max).
- Produces the results as a set of tables for the city overall and for two
  neighborhoods selected on the command line, plus box-and-whisker style plots.

## Build

```
make
```
Produces the `dispatch_analysis` executable. Requires `pthread` and `libm`.

## Run

A small sample dataset (`Law5K.dat`, ~5,000 records) is included, so you can run
it immediately:

```
make run
```

or directly:

```
./dispatch_analysis Law5K.dat header.txt 1 police_district BAYVIEW MISSION
```

Arguments: the data file, the field-layout file, the number of worker threads,
the field used to group neighborhoods, and the two neighborhoods to break out.

## Files

- `dispatch_analysis.c` — the analyzer.
- `header.txt` — the fixed-length-record field layout (name and width per field).
- `Law5K.dat` — a small sample dataset for a quick run.

> The full dataset used during development (~500,000 records, ~340 MB) is not
> included here because of its size; the program runs on any dataset that
> matches the `header.txt` layout.
