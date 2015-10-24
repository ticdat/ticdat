# ticdat

`ticdat` is an easy-to-use, lightweight, relational, data library. It provides a simple interface for defining a data schema, and a factory class for creating `TicDat` data objects that confirm to this schema.

It is primarily intended to simplify the process of developing proof-of-concept mathematical engines that read from one schema and write to another. It provides easy routines for reading and/or writing an entire data set for a range of stand-alone file types (Excel, .csv, Access or SQLite). For Access or SQLite, it can be used as a very condensed represeentation of the database schema.

For archiving test suites, `ticdat` is a useful way to convert data instances into .sql text files that can be archived in source code control systems.

When primary keys are specified, each table is a dictionary of dictionaries.
Otherwise, each table is an enumerable of dictionaries (as in DictReader/DictWriter). 

When foreign keys are specified, they can be used to for a variety of purposes.
  * `find_foreign_key_failures` can be find the data rows in child tables that fail to cross reference with their parent table
  * `obfusimplify` can be used to cascade entity renaming throughout the data set. This can facilitate troubleshootng by shorttening and simplifying entity names. It can also be used to anonymize data sets in order to remove propieatary information.
  * When `enable_foreign_key_links` is true, links are automatically created between the "row dictionaries" of the parent table and the matching "row dictionaries" of the child table.

When default values are provided, unfrozen `TicDat` objects will use them, as needed, when new rows are added. In general, unfrozen `TicDat` data tables behave like `defaultdict`s.  There are a variety of overrides to facilitate the addition of new data rows.

Alternately, `TicDat` data objects can be frozen. This facilitates good software development by insuring that code that is supposed to read from a data set without editing it behaves properly.

Although `ticdat` was specifically designed with Mixed Integer Programming data sets in mind, it can be used for
rapidly developing a wide variety of mathematical engines. It facilitates creating one definition of your
input data schema and one solve module, and reusing this same code, unchanged, on data from different
sources. This "separation of model from data" enables a user to move easily from toy, hard coded data to
larger, more realistic data sets. In addition, [Opalytics Inc](http://www.opalytics.com/) (the developer of the ticDat library) can support cloud deployments of solve engines that use the ticDat library.
The ticDat library is distributed under the BSD2 open source license.

To explore more, we reocmmned examining a few example notebooks. We show how `ticdat` can be used to [explore](https://github.com/opalytics/opalytics-ticdat/blob/master/examples/notebooks/diet.ipynb) a data set that has minor integrity problems. We also use `ticdat` to facilitate the diagnosis of [infeasible](https://github.com/opalytics/opalytics-ticdat/blob/master/examples/notebooks/netflow.ipynb) models.
