import os
from netflowmodel import dataFactory, solve, solutionFactory

assert not dataFactory.csv.get_duplicates("csv_data")
dat = dataFactory.csv.create_tic_dat("csv_data", freeze_it=True)
# the foreign key and data type data checks are part of solve

solution = solve(dat)

assert solution, "unexpected infeasibility"

solutionFactory.csv.write_directory(solution, os.path.join("csv_data", "solution"),
                                    allow_overwrite=True)