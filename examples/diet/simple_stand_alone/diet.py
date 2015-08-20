#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.

# edited with permission from Gurobi Optimization, Inc.

from gurobipy import *

# Nutrition guidelines, based on
# USDA Dietary Guidelines for Americans, 2005
# http://www.health.gov/DietaryGuidelines/dga2005/


# for demonstration purposes, we create a ticDat object without even using
# ticDat library. ticDat is both a library and a protocol.
class _(object) :
    pass

dat = _() # simplest object with ticDat attributes from monkey-patching
          # note that monkey-patching is a poor practice for industrial
          # software, but fine for a small, self-contained example

dat.categories = {
  'calories': {"minNutrition": 1800, "maxNutrition" : 2200},
  'protein':  {"minNutrition": 91,   "maxNutrition" : GRB.INFINITY},
  'fat':      {"minNutrition": 0,    "maxNutrition" : 65},
  'sodium':   {"minNutrition": 0,    "maxNutrition" : 1779}}

dat.foods = {
  'hamburger': {"cost": 2.49},
  'chicken':   {"cost": 2.89},
  'hot dog':   {"cost": 1.50},
  'fries':     {"cost": 1.89},
  'macaroni':  {"cost": 2.09},
  'pizza':     {"cost": 1.99},
  'salad':     {"cost": 2.49},
  'milk':      {"cost": 0.89},
  'ice cream': {"cost": 1.59}}


dat.nutritionQuantities = {
  ('hamburger', 'calories'): {"qty" : 410},
  ('hamburger', 'protein'):  {"qty" : 24},
  ('hamburger', 'fat'):      {"qty" : 26},
  ('hamburger', 'sodium'):   {"qty" : 730},
  ('chicken',   'calories'): {"qty" : 420},
  ('chicken',   'protein'):  {"qty" : 32},
  ('chicken',   'fat'):      {"qty" : 10},
  ('chicken',   'sodium'):   {"qty" : 1190},
  ('hot dog',   'calories'): {"qty" : 560},
  ('hot dog',   'protein'):  {"qty" : 20},
  ('hot dog',   'fat'):      {"qty" : 32},
  ('hot dog',   'sodium'):   {"qty" : 1800},
  ('fries',     'calories'): {"qty" : 380},
  ('fries',     'protein'):  {"qty" : 4},
  ('fries',     'fat'):      {"qty" : 19},
  ('fries',     'sodium'):   {"qty" : 270},
  ('macaroni',  'calories'): {"qty" : 320},
  ('macaroni',  'protein'):  {"qty" : 12},
  ('macaroni',  'fat'):      {"qty" : 10},
  ('macaroni',  'sodium'):   {"qty" : 930},
  ('pizza',     'calories'): {"qty" : 320},
  ('pizza',     'protein'):  {"qty" : 15},
  ('pizza',     'fat'):      {"qty" : 12},
  ('pizza',     'sodium'):   {"qty" : 820},
  ('salad',     'calories'): {"qty" : 320},
  ('salad',     'protein'):  {"qty" : 31},
  ('salad',     'fat'):      {"qty" : 12},
  ('salad',     'sodium'):   {"qty" : 1230},
  ('milk',      'calories'): {"qty" : 100},
  ('milk',      'protein'):  {"qty" : 8},
  ('milk',      'fat'):      {"qty" : 2.5},
  ('milk',      'sodium'):   {"qty" : 125},
  ('ice cream', 'calories'): {"qty" : 330},
  ('ice cream', 'protein'):  {"qty" : 8},
  ('ice cream', 'fat'):      {"qty" : 10},
  ('ice cream', 'sodium'):   {"qty" : 180} }


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
    m.addConstr(
      quicksum(dat.nutritionQuantities[f,c]["qty"] * buy[f] for f in dat.foods) == nutrition[c],
               c)

# Solve
m.optimize()

# print solution
if m.status == GRB.status.OPTIMAL:
    print('\nCost: %g' % m.objVal)
    print('\nBuy:')
    for f in dat.foods:
        if buy[f].x > 0.0001:
            print('%s %g' % (f, buy[f].x))
    print('\nNutrition:')
    for c in dat.categories:
        print('%s %g' % (c, nutrition[c].x))
else:
    print('No solution')


