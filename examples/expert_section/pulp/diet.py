#
# Solve the standard diet problem
#
# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python diet.py -i diet_sample_data.sql -o diet_solution_data.sql
# will read from a model stored in the file diet_sample_data.sql and write the solution
# to diet_solution_data.sql.

# this version of the file uses pulp

import pulp
from ticdat import TicDatFactory,  standard_main

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

    model = pulp.LpProblem(name="diet", sense=pulp.LpMinimize)

    nutrition = {
            c: pulp.LpVariable(
            name=f"{c}_var",
            cat=pulp.LpContinuous,
            lowBound=n["Min Nutrition"],
            upBound=n["Max Nutrition"])
        for c,n in dat.categories.items()}


    # Create decision variables for the foods to buy
    buy = {f: pulp.LpVariable(
            name=f"{f}_var",
            cat=pulp.LpContinuous,
            lowBound=0) for f in dat.foods}

     # Nutrition constraints
    nutrition_constraint= {}
    for c in dat.categories:
        nutrition_constraint[c] = model.addConstraint(
            pulp.LpConstraint(e=pulp.lpSum(dat.nutrition_quantities[f,c]["Quantity"] * buy[f]
                                    for f in dat.foods.keys()) - nutrition[c],
                              sense=pulp.LpConstraintEQ,
                              name=f"{c}_constraint",
                              rhs=0))


    model.setObjective(pulp.lpSum(buy[f] * c["Cost"] for f, c in dat.foods.items()))

    model.solve()

    if pulp.LpStatus[model.status] == 'Optimal':
        sln = solution_schema.TicDat()
        for f,x in buy.items():
            if x.varValue > 0:
                sln.buy_food[f] = x.varValue
        for c,x in nutrition.items():
            sln.consume_nutrition[c] = x.varValue
        sln.parameters['Total Cost'] = sum(dat.foods[f]["Cost"] * r["Quantity"]
                                           for f,r in sln.buy_food.items())
        return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------

