#!/usr/bin/python

# Copyright 2015, 2016, 2017, 2018 Opalytics, Inc.
#

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

# diet_ampl.py doesn't require a separate .mod file. Instead, we use amplpy.AMPL
from amplpy import AMPL
import amplpy
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
# There are three solution tables, with 2 primary key fields and 3 data fields.
solution_schema = TicDatFactory(
    parameters = [["Key"],["Value"]],
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

    # pandas DataFrames make it easiest to populate amplpy.DataFrames
    dat = input_schema.copy_to_pandas(dat, drop_pk_columns=False)

    df_cat = amplpy.DataFrame(index=('CAT',))
    df_cat.setColumn('CAT', list(dat.categories["Name"]))
    df_cat.addColumn('n_min', list(dat.categories["Min Nutrition"]))
    df_cat.addColumn('n_max', list(dat.categories["Max Nutrition"]))

    df_food = amplpy.DataFrame(index=('FOOD',))
    df_food.setColumn('FOOD', list(dat.foods["Name"]))
    df_food.addColumn('cost', list(dat.foods["Cost"]))

    df_amt = amplpy.DataFrame(index=('NUTR', 'FOOD'))
    df_amt.setColumn('NUTR', list(dat.nutrition_quantities["Category"]))
    df_amt.setColumn('FOOD', list(dat.nutrition_quantities["Food"]))
    df_amt.addColumn('amt', list(dat.nutrition_quantities["Quantity"]))

    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    set CAT;
    set FOOD;

    param cost {FOOD} > 0;

    param n_min {CAT} >= 0;
    param n_max {i in CAT} >= n_min[i];

    param amt {CAT,FOOD} >= 0;

    var Buy {j in FOOD} >= 0;
    var Consume {i in CAT } >= n_min [i], <= n_max [i];

    minimize Total_Cost:  sum {j in FOOD} cost[j] * Buy[j];

    subject to Diet {i in CAT}:
       Consume[i] =  sum {j in FOOD} amt[i,j] * Buy[j];
    """)

    ampl.setData(df_cat, 'CAT')
    ampl.setData(df_food, 'FOOD')
    ampl.setData(df_amt)

    ampl.solve()

    # TO DO : check solution success somehow

    buy = ampl.getVariable('Buy').getValues().toPandas().rename(columns={'Buy.val':"Quantity"})
    buy.index.rename("Food", inplace=True)
    consume = ampl.getVariable('Consume').getValues().toPandas().rename(columns={'Consume.val':"Quantity"})
    consume.index.rename("Category", inplace=True)

    sln = solution_schema.TicDat(buy_food = buy[buy["Quantity"] > 0], 
                                 consume_nutrition = consume[consume["Quantity"] > 0])
    sln.parameters['Total Cost'] = ampl.getObjective('Total_Cost').value()

    return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/sql/mdb files;
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
