#
# Demonstrates reading/writing datetime (here, specifically pandas.Timestamp) data
# to and from .csv files.
#
# Command line interface works like this
#    python simple_datetime_solver.py -i sample_data -o solution_directory
#
# This is a very simply app that datetime functionality that might be useful for a routing application.
# A parameter defines the start of the model, and each order has a "Deliver By" time requirement. The solution
# (which is just diagnostic information) is the time elapsed (in days) between the start time of the model and the
# "Delvery By time for each order

from ticdat import PanDatFactory, standard_main
# ------------------------ define the input schema --------------------------------
input_schema = PanDatFactory(parameters=[["Name"],["Value"]],
                             orders=[["Name"], ["Deliver By"]])
input_schema.set_data_type("orders", "Deliver By", datetime=True)
input_schema.add_parameter("Start Of Model", "Jan 1 2019", datetime=True)
# ---------------------------------------------------------------------------------

# ------------------------ define the output schema -------------------------------
solution_schema = PanDatFactory(time_to_deliver=[["Name"], ["Maximum Time To Deliver"]])
# ---------------------------------------------------------------------------------

# ------------------------ create a solve function --------------------------------
def solve(dat):
    assert input_schema.good_pan_dat_object(dat)
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_data_type_failures(dat) # "Delvery By" will be a datetime.datetime
    assert not input_schema.find_data_row_failures(dat) # "Start Of Model", if present, will be a datetime.datetime

    start_of_model = input_schema.create_full_parameters_dict(dat)["Start Of Model"]

    df_soln = dat.orders.copy(deep=True)
    df_soln["Maximum Time To Deliver"] = (df_soln["Deliver By"] - start_of_model)\
                                         .apply(lambda x: x.total_seconds()/(60*60*24))

    return solution_schema.PanDat(time_to_deliver=df_soln.drop("Deliver By", axis=1))
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/SQLite files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
