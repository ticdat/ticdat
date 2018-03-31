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
from amplpy import AMPL
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

    dat = input_schema.copy_to_ampl(dat)
    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    set LOAD_AMTS;
    var Num_Visits {LOAD_AMTS} integer >= 0;

    var Amt_Leftover >= 1.75, <= 1.75;

    minimize Total_Visits:
       sum {la in LOAD_AMTS} Num_Visits[la];

    subj to Set_Amt_Leftover:
       Amt_Leftover = sum {la in LOAD_AMTS} la * Num_Visits[la] - 2.25 * 12;

    """)

    input_schema.set_ampl_data(dat, ampl, {"load_amounts": "LOAD_AMTS"})
    ampl.solve()

    sln = solution_schema.TicDat() # create an empty solution

    if ampl.getValue("solve_result") != "infeasible":
        number_total_visits = 0
        for la,x in ampl.getVariable("Num_Visits").getValues().toDict().items():
            if round(x[0]) > 0:
                number_total_visits += round(x[0])
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
