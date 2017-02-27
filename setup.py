from setuptools import setup, find_packages
setup(
	name = 'ticdat',
	packages = find_packages(),
	version = '0.2.5',
	description = ("An easy-to-use data library for developing mathematical engines"),
	long_description = """

**ticdat** is an easy-to-use, lightweight, relational, data library. It
provides a simple interface for defining a data schema, and a factory
class for creating ``TicDat`` data objects that confirm to this schema.

It is primarily intended to simplify the process of developing
proof-of-concept mathematical engines that read from one schema and
write to another. It provides easy routines for reading and/or writing
an entire data set for a range of stand-alone file types (Excel, .csv,
Access or SQLite). For Access or SQLite, it can be used as a very
condensed representation of the database schema.

For archiving test suites, ``ticdat`` is a useful way to convert data
instances into .sql text files that can be archived in source code
control systems.

When primary keys are specified, each table is a dictionary of
dictionaries. Otherwise, each table is an enumerable of dictionaries.
The inner dictionaries are data rows indexed by field names (as in
``csv.DictReader/csv.DictWriter``).

When foreign keys are specified, they can be used for a variety of purposes:
  - ``find_foreign_key_failures`` can find the data rows in child tables that fail
    to cross reference with their parent table.
  - ``obfusimplify`` can be used to cascade entity renaming throughout the data set.
    This can facilitate troubleshooting by shortening and simplifying entity
    names. It can also be used to anonymize data sets in order to remove
    proprietary information.
  - When ``enable_foreign_key_links`` is true, links are automatically created between
    the data rows of the parent table and the matching data rows of the child table.

When default values are provided, unfrozen ``TicDat`` objects will use
them during the addition of new rows. In general, unfrozen ``TicDat``
data tables behave like a ``defaultdict``. There are a variety of other
overrides to facilitate the addition of new data rows.

Alternately, ``TicDat`` data objects can be frozen. This facilitates
good software development by insuring that code that is supposed to read
from a data set without editing it behaves properly.

Finally, the "dict-of-dicts" representation of a table can be eschewed
entirely in favor of ``pandas.DataFrame``. In this case, ``ticdat`` can
be used as a shim library that facilitates schema level definitions and
query abstraction for ``pandas`` developers.

Although ``ticdat`` was specifically designed with Mixed Integer
Programming data sets in mind, it can be used for rapidly developing a
wide variety of mathematical engines. It facilitates creating one
definition of your input data schema and one solve module, and reusing
this same code, unchanged, on data from different sources. This
"separation of model from data" enables a user to move easily from
small, testing data sets to larger, more realistic examples. In
addition, `Opalytics Inc <http://www.opalytics.com/>`__ (the developer
of ``ticdat``) can support cloud deployments of solve engines that use
``ticdat`` data objects.
	""",
	license = 'BSD 2-Clause',
	author = 'Opalytics Inc',
	author_email= 'pcacioppi@opalytics.com',
	maintainer_email = 'snelson@opalytics.com',
	url = 'https://github.com/opalytics/opalytics-ticdat',
	classifiers = [
		"Development Status :: 5 - Production/Stable",
		"Intended Audience :: Developers",
		"Intended Audience :: Science/Research",
		"Operating System :: OS Independent",
		"Programming Language :: Python",
		"Programming Language :: Python :: 2",
		"Programming Language :: Python :: 2.7",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.4",
		"Programming Language :: Python :: 3.5",
		"Topic :: Scientific/Engineering",
		"Topic :: Scientific/Engineering :: Mathematics"
	],
	platforms = 'any'
)
