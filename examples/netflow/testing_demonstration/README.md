# netflow testing demonstration

This directory implements the `netflow` model as a complete python package.
It implements the math code and the unit tests both, and demonstrates how `ticdat` can be
used to facilitate adding a testing model to the unit tests.

A python package isn't considered mature until it contains a set of unit tests to exercise the actual production
code. Similarly, an optimization engine is difficult to understand without sample data to demonstrate and verify
it's behavior. One of the great things about python is the ease with which you can use the `unittest` package to
implement a test suite for a given solver. `ticdat` facilitates this functionality by making it easy to
anonymize and archive your testing data.

This directory is meant to demonstrate a standalone repo. As such, it duplicates the `netflowmodel.py` file.

To run the `testnetflow.py` you need your Python path to include the directory that contains this README file,
or you need your current directory to symbolically link to the `netflow` subdirectory.

