#!/usr/bin/python

# Copyright 2015, 2016 Opalytics, Inc.
#

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.

# this version of the file uses CPLEX

from docplex.mp.model import Model
from ticdat import TicDatFactory, freeze_me

# ------------------------ define the input schema --------------------------------
# There are three input tables, with 4 primary key fields and 4 data fields.
dataFactory = TicDatFactory (
     categories = [["name"],["minNutrition", "maxNutrition"]],
     foods  = [["name"],["cost"]],
     nutritionQuantities = [["food", "category"], ["qty"]])

# the foreign key relationships are pretty much what you'd expect
dataFactory.add_foreign_key("nutritionQuantities", "foods", ["food", "name"])
dataFactory.add_foreign_key("nutritionQuantities", "categories",
                            ["category", "name"])

# We set the most common data type - a non-negative, non-infinite number
# that has no integrality restrictions.
for table, fields in dataFactory.data_fields.items():
    for field in fields:
        dataFactory.set_data_type(table, field)
# We override the default data type for maxNutrition which can accept infinity
dataFactory.set_data_type("categories", "maxNutrition", max=float("inf"),
                          inclusive_max=True)
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 2 primary key fields and 3 data fields.
solutionFactory = TicDatFactory(
        parameters = [[],["totalCost"]],
        buyFood = [["food"],["qty"]],
        consumeNutrition = [["category"],["qty"]])
# ---------------------------------------------------------------------------------


# ------------------------ create a solve function --------------------------------
def solve(dat):
    assert dataFactory.good_tic_dat_object(dat)
    # Model
    mdl = Model("diet")


    # Create decision variables for the nutrition information,
    # which we limit via bounds
    nutrition = {}
    for c,n in dat.categories.items() :
        nutrition[c] = mdl.continuous_var(lb=n["minNutrition"],
                                          ub=n["maxNutrition"], name=c)

    # Create decision variables for the foods to buy
    buy = {}
    for f in dat.foods:
        buy[f] = mdl.continuous_var(name=f)

     # Nutrition constraints
    for c in dat.categories:
        mdl.add_constraint(mdl.sum(dat.nutritionQuantities[f,c]["qty"] * buy[f]
                             for f in dat.foods)
                           == nutrition[c],
                           ctname = c)

    mdl.minimize(mdl.sum(buy[f] * c["cost"] for f,c in dat.foods.items()))

    if mdl.solve():
        sln = solutionFactory.TicDat()
        cplex_soln = mdl.solution
        sln.parameters.append(cplex_soln.get_objective_value())
        for f,x in buy.items():
            if cplex_soln.get_value(x) > 0.0001:
                sln.buyFood[f] = cplex_soln.get_value(x)
        for c,x in nutrition.items():
            sln.consumeNutrition[c] = cplex_soln.get_value(x)
        return freeze_me(sln)
# ---------------------------------------------------------------------------------