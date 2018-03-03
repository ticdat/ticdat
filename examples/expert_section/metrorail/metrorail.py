#
# Models Tallys Yunes Metrorail tickets problem.
# https://orbythebeach.wordpress.com/2018/03/01/buying-metrorail-tickets-in-miami/
#
# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python metrorail.py -i metro_rail_sample_data.json -o metro_rail_solution_data.json
# will read from a model stored in the file metro_rail_sample_data.json and write the
# solution to metro_rail_solution_data.json.

# this version of the file uses Gurobi

import gurobipy as gu
from ticdat import TicDatFactory, standard_main, gurobi_env
from itertools import product

# ------------------------ define the input schema --------------------------------
# There are three input tables, with 4 primary key fields and 4 data fields.
input_schema = TicDatFactory (
    parameters = [["Key"], ["Value"]],
    load_amounts = [["Amount"],[]],
    number_of_one_way_trips  = [["Number"],[]],
    amount_leftover = [["Amount"], []])

input_schema.set_data_type("parameters", "Key", number_allowed=False,
                           strings_allowed=["One Way Price"])
input_schema.set_data_type("parameters", "Value", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=False)

input_schema.set_data_type("load_amounts", "Amount", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=False)

input_schema.set_data_type("number_of_one_way_trips", "Number", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=False, must_be_int=True)

input_schema.set_data_type("amount_leftover", "Amount", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=False)

default_one_way_price = 2.25
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 2 primary key fields and 3 data fields.
solution_schema = TicDatFactory(
    load_amount_details = [["Number One Way Trips", "Amount Leftover", "Load Amount"],
                           ["Number Of Visits"]],
    load_amount_summary = [["Number One Way Trips", "Amount Leftover"],["Number Of Visits"]])
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

    sln = solution_schema.TicDat()

    for number_trips, amount_leftover in product(dat.number_of_one_way_trips, dat.amount_leftover):

        # using env=gurobi_env() just makes the code ready for Opalytics deployment
        mdl = gu.Model("metrorail", env=gurobi_env())

        # Create decision variables
        number_vists = {la:mdl.addVar(vtype = gu.GRB.INTEGER, name="load_amount_%s"%la)
                        for la in dat.load_amounts}

        one_way_price = default_one_way_price
        if "One Way Price" in dat.parameters:
            one_way_price = dat.parameters["One Way Price"]["Value"]
         # Constraint that we need enough money
        mdl.addConstr(gu.quicksum(la * number_vists[la] for la in dat.load_amounts)
                      >= one_way_price * number_trips,
                      name = "need_enough_money")
        # Constraint that we can't allow too much left over
        mdl.addConstr(gu.quicksum(la * number_vists[la] for la in dat.load_amounts) -
                      one_way_price * number_trips <= amount_leftover,
                      name = "limit_amount_leftover")

        # minimize the total number of visits to the ticket office
        mdl.setObjective(gu.quicksum(number_vists.values()), sense=gu.GRB.MINIMIZE)
        mdl.optimize()

        if mdl.status in [gu.GRB.OPTIMAL, gu.GRB.SUBOPTIMAL]:
            for la,x in number_vists.items():
                if x.x > 0:
                    sln.load_amount_details[number_trips, amount_leftover, la] = round(x.x)
                    sln.load_amount_summary[number_trips, amount_leftover] += round(x.x)
    return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
