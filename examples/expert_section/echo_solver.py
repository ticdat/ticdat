#!/usr/bin/python

# Copyright 2015, 2016, 2017 Opalytics, Inc.
#

# Simple toy solver to begin playing around with Python and ticdat.
# This solver does nothing but validate the input data passed sanity checks,
# and writes this same data back out as the 'solution'. This solver serves
# no purpose other than to provide an example for how to get started with
# ticdat.

# Provides command line interface via ticdat.standard_main
# For example, typing
#   python echo_solver.py -i input_data.xlsx -o input_copy_dir
# will read from a model stored in the file input_data.xlsx and write the solution
# to .csv files in created directory input_copy_dir

from ticdat import TicDatFactory, standard_main, Model

# ------------------------ define the input schema --------------------------------
# NOTE - defining the diet schema here.
# ***You should rewrite this section to define your own schema.***
# Please try to implement all the data integrity rules. Primary key fields vs
# data fields, data types for data fields, and foreign key relationships.

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
# Since this solver does nothing other than echo the input data back out as a
# solution, the solution schema is the same as the input schema. For real solvers
# you'd define a proper solution schema. This task would be similar to, and
# usually much easier than, defining the input schema. (For example, there is generally no
# need to define data types and foreign key rules for the solution schema).
solution_schema = input_schema
# ---------------------------------------------------------------------------------


# ------------------------ create a solve function --------------------------------

def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    This is a dummy solver. We just return a copy of the input data as the solution.
    No need to edit this.
    """
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)

    return solution_schema.TicDat(**{t: getattr(dat, t)
                                     for t in input_schema.all_tables})
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
