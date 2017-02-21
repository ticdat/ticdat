#!/usr/bin/python

# Copyright 2015, 2016 Opalytics, Inc.
#

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python diet.py -i input_data.xlsx -o solution_data.xlsx
# will read from a model stored in the file input_data.xlsx and write the solution
# to solution_data.xlsx.
#
# Note that file requires diet.mod to be in the same directory

from ticdat import TicDatFactory, standard_main, opl_run

# ------------------------ define the input schema --------------------------------
# There are three input tables, with 4 primary key fields and 4 data fields.
input_schema = TicDatFactory (
     categories = [["name"],["min_nutrition", "max_nutrition"]],
     foods  = [["name"],["cost"]],
     nutrition_quantities = [["food", "category"], ["qty"]])

# the foreign key relationships are pretty much what you'd expect
input_schema.add_foreign_key("nutrition_quantities", "foods", ["food", "name"])
input_schema.add_foreign_key("nutrition_quantities", "categories",
                            ["category", "name"])

# We set the most common data type - a non-negative, non-infinite number
# that has no integrality restrictions.
for table, fields in input_schema.data_fields.items():
    for field in fields:
        input_schema.set_data_type(table, field)
# We override the default data type for max_nutrition which can accept infinity
input_schema.set_data_type("categories", "max_nutrition", max=float("inf"),
                          inclusive_max=True)
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 2 primary key fields and 3 data fields.
solution_schema = TicDatFactory(
        parameters = [["parameter_name"],["parameter_value"]],
        buy_food = [["food"],["qty"]],
        consume_nutrition = [["category"],["qty"]])

# We set the most common data type - a non-negative, non-infinite number
# that has no integrality restrictions.
for table, fields in solution_schema.data_fields.items():
    for field in fields:
        solution_schema.set_data_type(table, field)
# ---------------------------------------------------------------------------------


# ------------------------ create a solve function --------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    return opl_run("diet.mod", input_schema, dat, solution_schema)
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------