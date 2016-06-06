#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.
#
# edited with permission from Gurobi Optimization, Inc.

# Solve the classic diet model.  This file implements
# a function that formulates and solves the model,
# but it contains no model data.  The data is
# passed in by the calling program.  Run example 'dietstatic.py',
# 'dietxls.py', or 'dietcsv.py' to invoke this function.

from gurobipy import *
from ticdat import TicDatFactory, freeze_me

# define the input schema. There are three input tables, with 4 primary key fields
# and 4 data fields amongst them.
dataFactory = TicDatFactory (
     categories = [["name"],["minNutrition", "maxNutrition"]],
     foods  = [["name"],["cost"]],
     nutritionQuantities = [["food", "category"], ["qty"]])

# define the solution schema. There are three solution tables, with 2 primary key fields
# and 3 data fields amongst them.
solutionFactory = TicDatFactory(
        parameters = [[],["totalCost"]],
        buyFood = [["food"],["qty"]],
        consumeNutrition = [["category"],["qty"]])


def solve(dat):
    assert dataFactory.good_tic_dat_object(dat)
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
