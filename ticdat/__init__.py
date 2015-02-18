"""
ticDat module for reading and writing data modules in ticDat format.

ticDat is a simple, easy-to-read format for tabular data. It's inspired, partly by the csv.DictReader
and csv.DictWriter.  When primary keys are specified, each table is a dictionary of dictionaries.
Otherwise, each table is an enumerable of dictionaries (as in DictReader/DictWriter). When foreign keys are
specified, they are used to create links between the "row dictionaries" of the parent table to the matching
"row dictionaries" of the child objects.

ticDat was designed with Mixed Integer Programming data sets in mind, although in can also be used for
rapidly developing mathematical engines in general. It facilitates creating one definition of your
input data schema and one solve module, and reusing this same code, unchanged, on data from different
sources. This "seperation of model from data" enables a user to move easily from toy, hard coded data to
larger, more realistic data sets. In addition, Opalytics Inc. (the developer of the ticDat library) can support
cloud deployments of solve engines coded to read/write with the ticDat library.

The ticDat library is distributed freely and without liability. ??LEGAL BALLIWICK??
"""