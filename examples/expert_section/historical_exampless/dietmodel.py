# This file was created because the original link
# (https://www.gurobi.com/documentation/5.0/examples/node134.html)
# is no longer available.
#
# The following code closely mirrors that original example. It was reconstructed
# partly from memory and partly from my notes, so it may not be perfectly
# historically accurate, but it does work.

import gurobipy as gp
from gurobipy import GRB


def solve(categories, minNutrition, maxNutrition, foods, cost, nutritionValues):
    m = gp.Model("diet")

    # Create decision variables for the foods to buy
    buy = {}
    for f in foods:
        buy[f] = m.addVar(name=f)

    # Create decision variables for the nutrition consumed
    nutrition = {}
    for c in categories:
        nutrition[c] = m.addVar(lb=minNutrition[c], ub=maxNutrition[c], name=c)

    # The objective is to minimize the costs
    m.setObjective(sum(buy[f]*cost[f] for f in foods), GRB.MINIMIZE)

    # Nutrition constraints
    for c in categories:
        m.addConstr(gp.quicksum(nutritionValues[f, c] * buy[f] for f in foods) == nutrition[c],
                     name=c)

    m.optimize()
    # just printing the solution out here, returning a solution might be more appropriate in general
    if m.status == GRB.OPTIMAL:
        print(f"\nCost: {m.ObjVal:g}")
        print("\nBuy:")
        for f in foods:
            if buy[f].X > 0.0001:
                print(f"{f} {buy[f].X:g}")
    else:
        print("No solution")