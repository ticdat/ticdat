from fantop import input_schema
dat = input_schema.sql.create_tic_dat_from_sql("fantop_sample_data.sql")
input_schema.xls.write_file(dat, "Fantop_Sample_Data.xlsx", allow_overwrite=True,
                            case_space_sheet_names = True)