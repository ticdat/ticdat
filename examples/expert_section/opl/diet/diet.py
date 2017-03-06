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
     nutrition_quantities = [["food", "category"], ["quantity"]])

# Define the data types
input_schema.set_data_type("categories", "min_nutrition", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("categories", "max_nutrition", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("foods", "cost", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("nutrition_quantities", "quantity", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)

# We also want to insure that Max Nutrition doesn't fall below Min Nutrition
input_schema.add_data_row_predicate(
    "categories", predicate_name="Min Max Check",
    predicate=lambda row : row["max_nutrition"] >= row["min_nutrition"])

# The default-default of zero makes sense everywhere except for Max Nutrition
input_schema.set_default_value("categories", "max_nutrition", float("inf"))
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 2 primary key fields and 3 data fields.
solution_schema = TicDatFactory(
        parameters = [["parameter_name"],["parameter_value"]],
        buy_food = [["food"],["qty"]],
        consume_nutrition = [["category"],["qty"]])

# Need to define solution data types to insure ticdat-OPL connection.
# Also useful for debugging in general.
solution_schema.set_data_type("parameters", "parameter_value", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
solution_schema.set_data_type("buy_food", "qty", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
solution_schema.set_data_type("consume_nutrition", "qty", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
# ---------------------------------------------------------------------------------


# ------------------------ create a solve function --------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    return opl_run("diet.mod", input_schema, dat, solution_schema)
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------