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

from ticdat import TicDatFactory, standard_main, Model

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
        parameters = [[],["total_cost"]],
        buy_food = [["food"],["qty"]],
        consume_nutrition = [["category"],["qty"]])
# ---------------------------------------------------------------------------------


# ------------------------ create a solve function --------------------------------
_model_type = "gurobi" # could also be 'cplex' or 'xpress'
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)

    mdl = Model(_model_type, "diet")

    nutrition = {c:mdl.add_var(lb=n["min_nutrition"], ub=n["max_nutrition"], name=c)
                for c,n in dat.categories.items()}

    # Create decision variables for the foods to buy
    buy = {f:mdl.add_var(name=f) for f in dat.foods}

     # Nutrition constraints
    for c in dat.categories:
        mdl.add_constraint(mdl.sum(dat.nutrition_quantities[f,c]["qty"] * buy[f]
                             for f in dat.foods)
                           == nutrition[c],
                           name = c)

    mdl.set_objective(mdl.sum(buy[f] * c["cost"] for f,c in dat.foods.items()))

    if mdl.optimize():
        sln = solution_schema.TicDat()
        for f,x in buy.items():
            if mdl.get_solution_value(x) > 0.0001:
                sln.buy_food[f] = mdl.get_solution_value(x)
        for c,x in nutrition.items():
            sln.consume_nutrition[c] = mdl.get_solution_value(x)
        sln.parameters.append(sum(dat.foods[f]["cost"] * r["qty"]
                                  for f,r in sln.buy_food.items()))
        return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
