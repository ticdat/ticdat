from workforce import input_schema
dat = input_schema.json.create_pan_dat("workforce_sample_data.json")
input_schema.xls.write_file(dat, "Workforce_Sample_Data.xlsx", case_space_sheet_names=True)