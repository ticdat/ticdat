#!/usr/bin/python

# Copyright 2015, 2016 Opalytics, Inc.

# Solve a multi-commodity flow problem as python package.

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python netflow.py -i csv_data -o solution_csv_data
# will read from a model stored in .csv files in the csv_data directory
# and write the solution to .csv files in the solution_csv_data directory
#
# Note that file requires diet.mod to be in the same directory

from ticdat import TicDatFactory, standard_main, opl_run

# ------------------------ define the input schema --------------------------------
input_schema = TicDatFactory (
     commodities = [["name"],[]],
     nodes  = [["name"],[]],
     arcs = [["source", "destination"],["capacity"]],
     cost = [["commodity", "source", "destination"], ["cost"]],
     inflow = [["commodity", "node"],["quantity"]]
)

# add foreign key constraints
input_schema.add_foreign_key("arcs", "nodes", ['source', 'name'])
input_schema.add_foreign_key("arcs", "nodes", ['destination', 'name'])
input_schema.add_foreign_key("cost", "nodes", ['source', 'name'])
input_schema.add_foreign_key("cost", "nodes", ['destination', 'name'])
input_schema.add_foreign_key("cost", "commodities", ['commodity', 'name'])
input_schema.add_foreign_key("inflow", "commodities", ['commodity', 'name'])
input_schema.add_foreign_key("inflow", "nodes", ['node', 'name'])

input_schema.set_data_type("arcs", "capacity",  max=float("inf"),
                           inclusive_max=True)
input_schema.set_data_type("cost", "cost")
input_schema.set_data_type("inflow", "quantity", min=-float("inf"),
                          inclusive_min=False)
# ---------------------------------------------------------------------------------

# ------------------------ define the output schema -------------------------------
solution_schema = TicDatFactory(
        flow = [["commodity", "source", "destination"], ["quantity"]],
        parameters = [["paramKey"],["value"]])
solution_schema.set_data_type("flow","quantity")
solution_schema.set_data_type("parameters","value")
# ---------------------------------------------------------------------------------

# ------------------------ solving section-----------------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    return opl_run("netflow.mod", input_schema, dat, solution_schema)

# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
