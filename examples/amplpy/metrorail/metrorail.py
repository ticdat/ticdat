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
from ticdat import PanDatFactory, standard_main
from itertools import product
from pandas import DataFrame

# ------------------------ define the input schema --------------------------------
input_schema = PanDatFactory (
    parameters=[["Parameter"], ["Value"]],
    load_amounts=[["Amount"],[]],
    number_of_one_way_trips=[["Number"],[]],
    amount_leftover=[["Amount"], []])

input_schema.set_data_type("load_amounts", "Amount", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=False)

input_schema.set_data_type("number_of_one_way_trips", "Number", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=False, must_be_int=True)

input_schema.set_data_type("amount_leftover", "Amount", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)


default_parameters = {"One Way Price": 2.25, "Amount Leftover Constraint": "Upper Bound"}
def _good_parameter_key_value(key, value):
    if key == "One Way Price":
        try:
            return 0 < value < float("inf")
        except:
            return False
    if key == "Amount Leftover Constraint":
        return value  in ["Equality", "Upper Bound"]

assert all(_good_parameter_key_value(k,v) for k,v in default_parameters.items())

input_schema.set_data_type("parameters", "Parameter", number_allowed=False,
                           strings_allowed=default_parameters)
input_schema.add_data_row_predicate("parameters", predicate_name="Good Parameter Value",
    predicate=lambda row : _good_parameter_key_value(row["Parameter"], row["Value"]))
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
solution_schema = PanDatFactory(
    load_amount_details=[["Number One Way Trips", "Amount Leftover", "Load Amount"],
                           ["Number Of Visits"]],
    load_amount_summary=[["Number One Way Trips", "Amount Leftover"],["Number Of Visits"]])
# ---------------------------------------------------------------------------------


# ------------------------ create a solve function --------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    assert input_schema.good_pan_dat_object(dat)
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)
    # use default parameters, unless they are overridden by user-supplied parameters
    full_parameters = dict(default_parameters)
    for k in default_parameters:
        if k in set(dat.parameters["Parameter"]):
            full_parameters[k] = dat.parameters[dat.parameters["Parameter"] == k]["Value"][0]

    ampl_dat = input_schema.copy_to_ampl(dat, excluded_tables=
                   set(input_schema.all_tables).difference({"load_amounts"}))
    load_amount_details = DataFrame(columns=['Number One Way Trips', 'Amount Leftover', 'Load Amount',
                                             'Number Of Visits'])
    # solve a distinct MIP for each pair of (# of one-way-trips, amount leftover)
    for number_trips, amount_leftover in product(list(dat.number_of_one_way_trips["Number"]),
                                                 list(dat.amount_leftover["Amount"])):

        ampl = AMPL()
        ampl.setOption('solver', 'gurobi')
        ampl.eval("""
        param amount_leftover_lb >= 0;
        param amount_leftover_ub >= amount_leftover_lb;
        param one_way_price >= 0;
        param number_trips >= 0;
        set LOAD_AMTS;
        var Num_Visits {LOAD_AMTS} integer >= 0;
        var Amt_Leftover >= amount_leftover_lb, <= amount_leftover_ub;
        minimize Total_Visits:
           sum {la in LOAD_AMTS} Num_Visits[la];
        subj to Set_Amt_Leftover:
           Amt_Leftover = sum {la in LOAD_AMTS} la * Num_Visits[la] - one_way_price * number_trips;""")

        ampl.param['amount_leftover_lb'] = amount_leftover \
            if full_parameters["Amount Leftover Constraint"] == "Equality" else 0
        ampl.param['amount_leftover_ub'] = amount_leftover
        ampl.param['number_trips'] = number_trips
        ampl.param['one_way_price'] = full_parameters["One Way Price"]
        input_schema.set_ampl_data(ampl_dat, ampl, {"load_amounts": "LOAD_AMTS"})

        ampl.solve()

        if ampl.getValue("solve_result") != "infeasible":
            # store the results if and only if the model is feasible
            av = ampl.getVariable("Num_Visits").getValues()
            if av.toDict():
                df = ampl.getVariable("Num_Visits").getValues().toPandas().reset_index()
                df.rename(columns = {df.columns[0]: "Load Amount", df.columns[1]: "Number Of Visits"}, inplace=True)
                df["Number One Way Trips"] = number_trips
                df["Amount Leftover"] = amount_leftover
                load_amount_details = load_amount_details.append(df[df["Number Of Visits"] > 0])

    load_amount_details.sort_values(by=["Number One Way Trips", "Amount Leftover", "Load Amount"], inplace=True)
    load_amount_summary = DataFrame(columns=['Number One Way Trips', 'Amount Leftover', 'Number Of Visits'])
    if len(load_amount_details):
        cols = ["Number One Way Trips", "Amount Leftover"]
        load_amount_summary = load_amount_details.set_index(cols, drop=False).groupby(level=cols).aggregate(
                                                  {"Number Of Visits":sum}).reset_index().sort_values(by=cols)
    sln = solution_schema.PanDat(load_amount_details=load_amount_details,
                                 load_amount_summary=load_amount_summary)

    return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
