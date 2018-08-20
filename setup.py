import ticdat
from setuptools import setup, find_packages
setup(
	name = 'ticdat',
	packages = find_packages(),
	version = ticdat.__version__,
	description = ("An easy-to-use data library for developing mathematical engines"),
	long_description = """

# ticdat

Go [here](https://github.com/opalytics/opalytics-ticdat/wiki/ticdat-status) for project status and installation
instructions.

`ticdat` is a Python package that provides lightweight, ORM style functionality around either a dict-of-dicts or
`pandas.DataFrame` representation of tables. It is well suited for defining and validating the input data for complex
solve engines (i.e. optimization and scheduling-type problems).

`ticdat` functionality is organized around two classes - `TicDatFactory` and `PanDatFactory`. Both classes define a
simple database style schema on construction. Data integrity rules can then be added in the form of foreign key
relationships, data field types (to include numerical ranges and allowed strings) and row predicates
(functions that check if a given row violates a particular data condition).  The factory classes can then be used to
construct `TicDat`/`PanDat` objects that contain tables consistent with the defined schema. By design,
`ticdat`, allows these data objects to violate the data integrity rules while providing convenient bulk query functions
to determine where those violations occur.

`TicDat` objects (created by a `TicDatFactory`) contain tables in a dict-of-dict format. The outer dictionary maps
primary key values to data rows. The inner dictionaries are data rows indexed by field names (similar to
`csv.DictReader/csv.DictWriter`). Tables that do not have primary keys are rendered as a list of data row dictionaries.

`PanDat` objects (created by `PanDatFactory`) render tables as `pandas.DataFrame` objects. The columns in each
`DataFrame` will contain all of the primary key and data fields that were defined in the `PanDatFactory` schema. The
`PanDatFactory` code can be thought of as implementing a shim library that organizes `DataFrame` objects into a
predefined schema, and facilitates rich integrity checks based on schema defined rules.

The `ticdat` example library is focused on two patterns for building optimization engines - using `TicDatFactory` in
conjunction with `gurobipy` and using `PanDatFactory` in conjunction with `amplpy`. That said, `ticdat` can also be
used with libraries like `pyomo`, `pulp`, `docplex` and `xpress`. It also has functionality to support the OPL and
LINGO modeling languages, although the AMPL support is far more mature.

`ticdat`'s association with AMPL is particularly strong. The design goals of `ticdat` were inspired by AMPL's pattern of cleanly separating model from data, as well as its ability to thoroughly validate input data prior to solving.
AMPL Optimization has endorsed `ticdat` as their preferred library for bridging the world's leading modeling language
(AMPL) with Python's most powerful data analysis package (`pandas`).

The `ticdat` library is distributed under the BSD2 open source license.

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
