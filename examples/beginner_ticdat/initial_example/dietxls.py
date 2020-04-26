# Read/write from Excel file. Analgous to this file. https://bit.ly/2S4Xvyo
from diet import solve, input_schema, solution_schema

dat = input_schema.xls.create_tic_dat("diet.xls") # Just look for the diet.xls file in the current directory

sln = solve(dat)

if sln: # if the solve succeeds, write back to the current directory
    solution_schema.xls.write_file(sln, "solution.xls", allow_overwrite=True)