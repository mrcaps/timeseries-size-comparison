#timeseries-size-comparison

Size comparison experiment

This is a preliminary experiment that compares the size of fixed-width integer data in SQLite and OpenTSDB. Due to license restrictions we cannot include the FinanceDB evaluation.

To build (tested on Ubuntu 12.04 and Windows 7):
```bash
$ cmake .
$ make
```

To run a test with a small dataset:

```bash
$ cd bin
$ ./comparison test
```

OpenTSDB insert format will be printed to stdout and a SQLite DB will be created.

For additional documentation run `src/comparison`