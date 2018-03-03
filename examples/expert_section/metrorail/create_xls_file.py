from metrorail import input_schema
dat = input_schema.json.create_tic_dat_from_sql("metrorail_sample_data.json")
input_schema.xlsx.write_file(dat, "Metro Rail Data.xlsx", allow_overwrite=True)