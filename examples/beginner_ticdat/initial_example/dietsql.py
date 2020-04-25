# Read/write from SQLite database file. Analgous to this file. https://bit.ly/3aB6Zrp
from diet import solve, input_schema, solution_schema
from diet import solve, input_schema, solution_schema

dat = input_schema.sql.create_tic_dat("diet.db") # Just look for the diet.db file in the current directory

sln = solve(dat)

if sln: # if the solve succeeds, write back to the current directory
    solution_schema.sql.write_db_data(sln, "solution.db", allow_overwrite=True)