# Simplest diet example using amplpy and ticdat

from amplpy import AMPL
from ticdat import PanDatFactory, standard_main

input_schema = PanDatFactory (
    categories = [["Name"],["Min Nutrition", "Max Nutrition"]],
    foods  = [["Name"],["Cost"]],
    nutrition_quantities = [["Food", "Category"], ["Quantity"]])

# There are three solution tables, with 3 primary key fields and 3 data fields.
solution_schema = PanDatFactory(
    parameters = [["Parameter"],["Value"]],
    buy_food = [["Food"],["Quantity"]],
    consume_nutrition = [["Category"],["Quantity"]])

def solve(dat):
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
    # copy the tables to amplpy.DataFrame objects, renaming the data fields as needed
    dat = input_schema.copy_to_ampl(dat, field_renamings={
            ("foods", "Cost"): "cost",
            ("categories", "Min Nutrition"): "n_min",
            ("categories", "Max Nutrition"): "n_max",
            ("nutrition_quantities", "Quantity"): "amt"})
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

# when run from the command line, will read/write xls/csv/json/db files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)