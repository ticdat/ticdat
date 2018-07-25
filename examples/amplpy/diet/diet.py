#!/usr/bin/python

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python diet_ampl.py -i input_data.xlsx -o solution_data.xlsx
# will read from a model stored in the file input_data.xlsx and write the solution
# to solution_data.xlsx.
#

from amplpy import AMPL
from ticdat import PanDatFactory, standard_main

# ------------------------ define the input schema --------------------------------
# There are three input tables, with 4 primary key fields and 4 data fields.
input_schema = PanDatFactory (
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
solution_schema = PanDatFactory(
    parameters = [["Parameter"],["Value"]],
    buy_food = [["Food"],["Quantity"]],
    consume_nutrition = [["Category"],["Quantity"]])
# ---------------------------------------------------------------------------------

# ------------------------ create a solve function --------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good pandat for the input_schema
    :return: a good pandat for the solution_schema, or None
    """
    assert input_schema.good_pan_dat_object(dat)
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    # copy the tables to amplpy.DataFrame objects, renaming the data fields as needed
    dat = input_schema.copy_to_ampl(dat, field_renamings={
            ("foods", "Cost"): "cost",
            ("categories", "Min Nutrition"): "n_min",
            ("categories", "Max Nutrition"): "n_max",
            ("nutrition_quantities", "Quantity"): "amt"})

    # build the AMPL math model
    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    set CAT;
    set FOOD;

    param cost {FOOD} > 0;

    param n_min {CAT} >= 0;
    param n_max {i in CAT} >= n_min[i];

    param amt {FOOD, CAT} >= 0;

    var Buy {j in FOOD} >= 0;
    var Consume {i in CAT } >= n_min [i], <= n_max [i];

    minimize Total_Cost:  sum {j in FOOD} cost[j] * Buy[j];

    subject to Diet {i in CAT}:
       Consume[i] =  sum {j in FOOD} amt[j,i] * Buy[j];
    """)

    # load the amplpy.DataFrame objects into the AMPL model, explicitly identifying how to populate the AMPL sets
    input_schema.set_ampl_data(dat, ampl, {"categories": "CAT", "foods": "FOOD"})

    # solve and recover the solution if feasible
    ampl.solve()
    if ampl.getValue("solve_result") != "infeasible":
        # solution tables are populated by mapping solution (table, field) to AMPL variable
        sln = solution_schema.copy_from_ampl_variables(
            {("buy_food", "Quantity"): ampl.getVariable("Buy"),
            ("consume_nutrition", "Quantity"): ampl.getVariable("Consume")})
        # append the solution KPI results to the solution parameters table
        sln.parameters.loc[0] = ['Total Cost', ampl.getObjective('Total_Cost').value()]

        return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/json/db files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------