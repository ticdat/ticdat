from ticdat import TicDatFactory, standard_main

# ------------------------ define the input schema --------------------------------
input_schema = TicDatFactory(
    # needs to be filled in
    # use camelcase and underscore for table names but upper case/lower case and spaces for field names
)
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 3 primary key fields and 3 data fields.
solution_schema = TicDatFactory(
    # needs to be filled in
    # use camelcase and underscore for table names but upper case/lower case and spaces for field names
)
# ---------------------------------------------------------------------------------


# ------------------------ create a solve function --------------------------------
def solve(dat):
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    # return a solution_schema.TicDat object on success or None on failure

# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
