# This file was created because the original link
# (https://www.gurobi.com/documentation/5.0/examples/node130.html)
# is no longer available.
#
# The following code closely mirrors that original example, which inspired
# using dict-of-dicts for standardizing data tables. It was reconstructed
# partly from memory and partly from my notes, so it may not be perfectly
# historically accurate, but it does work.

# Solve the classic diet model
import gurobipy as gp
from gurobipy import GRB


# Nutrition guidelines, based on
# USDA Dietary Guidelines for Americans, 2005
# http://www.health.gov/DietaryGuidelines/dga2005/

categories, minNutrition, maxNutrition = gp.multidict(
    {
        "calories": [1800, 2200],
        "protein": [91, GRB.INFINITY],
        "fat": [0, 65],
        "sodium": [0, 1779],
    }
)

foods, cost = gp.multidict(
    {
        "hamburger": 2.49,
        "chicken": 2.89,
        "hot dog": 1.50,
        "fries": 1.89,
        "macaroni": 2.09,
        "pizza": 1.99,
        "salad": 2.49,
        "milk": 0.89,
        "ice cream": 1.59,
    }
)

# Nutrition values for the foods
nutritionValues = {
    ("hamburger", "calories"): 410,
    ("hamburger", "protein"): 24,
    ("hamburger", "fat"): 26,
    ("hamburger", "sodium"): 730,
    ("chicken", "calories"): 420,
    ("chicken", "protein"): 32,
    ("chicken", "fat"): 10,
    ("chicken", "sodium"): 1190,
    ("hot dog", "calories"): 560,
    ("hot dog", "protein"): 20,
    ("hot dog", "fat"): 32,
    ("hot dog", "sodium"): 1800,
    ("fries", "calories"): 380,
    ("fries", "protein"): 4,
    ("fries", "fat"): 19,
    ("fries", "sodium"): 270,
    ("macaroni", "calories"): 320,
    ("macaroni", "protein"): 12,
    ("macaroni", "fat"): 10,
    ("macaroni", "sodium"): 930,
    ("pizza", "calories"): 320,
    ("pizza", "protein"): 15,
    ("pizza", "fat"): 12,
    ("pizza", "sodium"): 820,
    ("salad", "calories"): 320,
    ("salad", "protein"): 31,
    ("salad", "fat"): 12,
    ("salad", "sodium"): 1230,
    ("milk", "calories"): 100,
    ("milk", "protein"): 8,
    ("milk", "fat"): 2.5,
    ("milk", "sodium"): 125,
    ("ice cream", "calories"): 330,
    ("ice cream", "protein"): 8,
    ("ice cream", "fat"): 10,
    ("ice cream", "sodium"): 180,
}

# Model
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


def printSolution():
    if m.status == GRB.OPTIMAL:
        print(f"\nCost: {m.ObjVal:g}")
        print("\nBuy:")
        for f in foods:
            if buy[f].X > 0.0001:
                print(f"{f} {buy[f].X:g}")
    else:
        print("No solution")


# Solve
m.optimize()
printSolution()
