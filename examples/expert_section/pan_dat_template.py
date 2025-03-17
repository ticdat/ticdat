
from ticdat import PanDatFactory, standard_main

# ------------------------ define the input schema --------------------------------
input_schema = PanDatFactory(
    # needs to be filled in
    # use camelcase and underscore for table names but upper case/lower case and spaces for field names
)

# ---------------------------------------------------------------------------------

# ------------------------ define the output schema -------------------------------
solution_schema = PanDatFactory(
    # needs to be filled in
    # use camelcase and underscore for table names but upper case/lower case and spaces for field names
)
# ---------------------------------------------------------------------------------

# ------------------------ solving section-----------------------------------------
def solve(dat):

    assert input_schema.good_pan_dat_object(dat)
    # PanDat objects might contain duplicate rows, whereas TicDat objects cannot. This is because DataFrames
    # can contain duplicate rows, but dictionaries cannot.
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)
    # return a solution_schema.PanDat object on success or None on failure
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------