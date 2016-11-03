#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.
#
# edited with permission from Gurobi Optimization, Inc.

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python dietmodel.py -i input_data.xlsx -o solution_data.xlsx
# will read from a model stored in the file input_data.xlsx and write the solution
# to solution_data.xlsx.

# this version of the file uses Gurobi

from gurobipy import *
from ticdat import TicDatFactory, freeze_me, standard_main

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
    assert not dataFactory.find_foreign_key_failures(dat)
    assert not dataFactory.find_data_type_failures(dat)
    
    # Model
    m = Model("diet")

    # Create decision variables for the nutrition information,
    # which we limit via bounds
    nutrition = {}
    for c,n in dat.categories.items() :
        nutrition[c] = m.addVar(lb=n["minNutrition"], ub=n["maxNutrition"], name=c)

    # Create decision variables for the foods to buy
    buy = {}
    for f,c in dat.foods.items():
        buy[f] = m.addVar(obj=c["cost"], name=f)

    # The objective is to minimize the costs
    m.modelSense = GRB.MINIMIZE

    # Update model to integrate new variables
    m.update()


    # Nutrition constraints
    for c in dat.categories:
        m.addConstr(quicksum(dat.nutritionQuantities[f,c]["qty"] * buy[f]
                             for f in dat.foods)
                    == nutrition[c],
                    c)

    # Solve
    m.optimize()

    if m.status == GRB.status.OPTIMAL:
        sln = solutionFactory.TicDat()
# when writing into tables with just one data row, the field name can be omitted
        sln.parameters.append(m.objVal)
        for f in dat.foods:
            if buy[f].x > 0.0001:
                sln.buyFood[f] = buy[f].x
        for c in dat.categories:
            sln.consumeNutrition[c] = nutrition[c].x
        return freeze_me(sln)
# ---------------------------------------------------------------------------------


# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/mdb files
if __name__ == "__main__":
    standard_main(dataFactory, solutionFactory, solve)
# ---------------------------------------------------------------------------------
