#
# Models Tallys Yunes Metrorail tickets problem.
# https://orbythebeach.wordpress.com/2018/03/01/buying-metrorail-tickets-in-miami/
#
# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python metrorail.py -i metrorail_sample_data.json -o metrorail_solution_data.json
# will read from a model stored in the file metrorail_sample_data.json and write the
# solution to metrorail_solution_data.json.

# this version of the file uses amplpy and Gurobi
import gurobipy as gu
from ticdat import TicDatFactory, standard_main

# ------------------------ define the input schema --------------------------------
# simple for demonstration purposes for now
input_schema = TicDatFactory (load_amounts=[["Amount"],[]])
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
solution_schema = TicDatFactory() # empty for demonstration purposes for now
# ---------------------------------------------------------------------------------

# ------------------------ create a solve function --------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_data_type_failures(dat)

    sln = solution_schema.TicDat() # create an empty solution'


    mdl = gu.Model("metrorail")

    # Create decision variables
    number_vists = {la:mdl.addVar(vtype = gu.GRB.INTEGER, name="load_amount_%s"%la)
                    for la in dat.load_amounts}
    amount_leftover_var = mdl.addVar(name="amount_leftover")

    amount_leftover_var.lb = amount_leftover_var.ub = 1.75 # DELIBERATELY HARDCODING FOR THIS EXAMPLE!

    # add a constraint to set the amount leftover
    # DELIBERATELY HARD-CODING 2.25 as the one way price, and 12 as the number of trips below
    mdl.addConstr(amount_leftover_var ==
                  gu.quicksum(la * number_vists[la] for la in dat.load_amounts) -
                  2.25 * 12,
                  name="set_amount_leftover")


    # minimize the total number of visits to the ticket office
    mdl.setObjective(gu.quicksum(number_vists.values()), sense=gu.GRB.MINIMIZE)
    mdl.optimize()

    if mdl.status in [gu.GRB.OPTIMAL, gu.GRB.SUBOPTIMAL]:
        number_total_visits = 0
        # store the results if and only if the model is feasible
        for la,x in number_vists.items():
            if x.x > 0:
                number_total_visits += round(x.x)
        print "Total number of visits %s"%number_total_visits
        return solution_schema.TicDat()
    else :
        print "failed to solve!"
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
