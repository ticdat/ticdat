# ticdat

Go [here](https://github.com/opalytics/opalytics-ticdat/wiki/ticdat-status) for project status and installation instructions.

`ticdat` is an easy-to-use, lightweight, relational, data library. It provides a simple interface for defining a data schema, and a factory class for creating `TicDat` data objects that confirm to this schema.

It is primarily intended to simplify the process of developing proof-of-concept mathematical engines that read from one schema and write to another. It provides easy routines for reading and/or writing an entire data set for a range of stand-alone file types (Excel, .csv, Access or SQLite). For Access or SQLite, it can be used as a very condensed representation of the database schema.

For archiving test suites, `ticdat` is a useful way to convert data instances into .sql text files that can be archived in source code control systems.

When primary keys are specified, each table is a dictionary of dictionaries.
Otherwise, each table is an enumerable of dictionaries. The inner dictionaries are data rows indexed by field names (as in `csv.DictReader/csv.DictWriter`). 

When foreign keys are specified, they can be used for a variety of purposes.
  * `find_foreign_key_failures` can find the data rows in child tables that fail to cross reference with their parent table.
  * `obfusimplify` can be used to cascade entity renaming throughout the data set. This can facilitate troubleshooting by shortening and simplifying entity names. It can also be used to anonymize data sets in order to remove proprietary information.
  * When `enable_foreign_key_links` is true, links are automatically created between the data rows of the parent table and the matching data rows of the child table.
    * For example, `dat.foods["bacon"].nutritionQuantities` is an easy way to find all the nutritional properties of bacon. 
    * Essentially, the option allows `ticdat` to automatically perform the inner joins most common to your data set.

When default values are provided, unfrozen `TicDat` objects will use them during the addition of new rows. In general, unfrozen `TicDat` data tables behave like `defaultdict`s.  There are a variety of other overrides to facilitate the addition of new data rows.

Alternately, `TicDat` data objects can be frozen. This facilitates good software development by insuring that code that is supposed to read from a data set without editing it behaves properly.

Although `ticdat` was specifically designed with Mixed Integer Programming data sets in mind, it can be used for
rapidly developing a wide variety of mathematical engines. It facilitates creating one definition of your
input data schema and one solve module, and reusing this same code, unchanged, on data from different
sources. This "separation of model from data" enables a user to move easily from small, testing data sets to larger, more realistic examples. In addition, [Opalytics Inc](http://www.opalytics.com/) (the developer of  `ticdat`) can support cloud deployments of solve engines that use `ticdat` data objects library. Go  [here](https://github.com/opalytics/example-diet) or [here](https://github.com/opalytics/example-netflow) for examples of `ticdat` being used to create "deployment ready" Python packages.

The `ticdat` library is distributed under the BSD2 open source license.





