#
# Using the standard diet problem to demonstrate the ticdat principles of Tidy, Tested, Safe.
# Please refer to the following for an introduction.
# https://github.com/ticdat/ticdat/wiki/1-Beginner-ticdat-intro
#
# This file is training material meant to guide you towards a well organized GitHub repository.
# https://github.com/ticdat/tts_diet
#
# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python diet.py -i diet_sample_data -o diet_solution_data
# will read from a model stored in the directory diet_sample_data and write the solution
# to a directory called diet_solution_data. These data directories contain .csv files.

# this version of the file uses Gurobi

try: # if you don't have gurobipy installed, the code will still load and then fail on solve
    import gurobipy as gp
except:
    gp = None

from ticdat import TicDatFactory, standard_main

# ------------------------ define the input schema --------------------------------
# There are three input tables, with 4 primary key fields and 4 data fields.
input_schema = TicDatFactory (
    categories = [["Name"],["Min Nutrition", "Max Nutrition"]],
    foods  = [["Name"],["Cost"]],
    nutrition_quantities = [["Food", "Category"], ["Quantity"]])

# Define the foreign key relationships
input_schema.add_foreign_key("nutrition_quantities", "foods", ["Food", "Name"])
input_schema.add_foreign_key("nutrition_quantities", "categories",
                            ["Category", "Name"])

# Define the data types
input_schema.set_data_type("categories", "Min Nutrition", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("categories", "Max Nutrition", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("foods", "Cost", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("nutrition_quantities", "Quantity", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)

# We also want to insure that Max Nutrition doesn't fall below Min Nutrition
input_schema.add_data_row_predicate(
    "categories", predicate_name="Min Max Check",
    predicate=lambda row : row["Max Nutrition"] >= row["Min Nutrition"])

# The default-default of zero makes sense everywhere except for Max Nutrition
input_schema.set_default_value("categories", "Max Nutrition", float("inf"))
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 3 primary key fields and 3 data fields.
solution_schema = TicDatFactory(
    parameters = [["Parameter"],["Value"]],
    buy_food = [["Food"],["Quantity"]],
    consume_nutrition = [["Category"],["Quantity"]])
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

    if gp is None: # even if you don't have gurobipy installed, you can still import this file for other uses
        print("*****\ngurobipy needs to be installed for this example code to solve!\n*****\n")
    mdl = gp.Model("diet")

    nutrition = {c:mdl.addVar(lb=n["Min Nutrition"], ub=n["Max Nutrition"], name=c)
                for c,n in dat.categories.items()}

    # Create decision variables for the foods to buy
    buy = {f:mdl.addVar(name=f) for f in dat.foods}

     # Nutrition constraints
    for c in dat.categories:
        mdl.addConstr(gp.quicksum(dat.nutrition_quantities[f, c]["Quantity"] * buy[f]
                                  for f in dat.foods) == nutrition[c],
                      name = c)

    mdl.setObjective(gp.quicksum(buy[f] * c["Cost"] for f, c in dat.foods.items()),
                     sense=gp.GRB.MINIMIZE)
    mdl.optimize()

    if mdl.status == gp.GRB.OPTIMAL:
        sln = solution_schema.TicDat()
        for f,x in buy.items():
            if x.x > 0:
                sln.buy_food[f] = x.x
        for c,x in nutrition.items():
            sln.consume_nutrition[c] = x.x
        sln.parameters['Total Cost'] = sum(dat.foods[f]["Cost"] * r["Quantity"]
                                           for f,r in sln.buy_food.items())
        return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------

