from metrorail import input_schema
dat = input_schema.json.create_tic_dat("metrorail_sample_data.json")
input_schema.xls.write_file(dat, "Metro Rail Data.xlsx", allow_overwrite=True)