#!/usr/bin/python
#
# Copyright 2016, Opalytics, Inc.
#
# Small driver script for the netflowmodel.py script.
#
# This file solves the model with the data provided by csv files in the "csv_data" directory.
# It prints the summary information to the screen, and writes the solution into a
# a "solution" subdirectory of "csv_data", overwriting any pre-existing solution.
#
# Creating a similar driver file for the other ticdat formats (xls, SQLite, mdb, static data) would be
# trivial. See the diet example for more details.

import os
from netflowmodel import dataFactory, solve, solutionFactory

assert not dataFactory.csv.get_duplicates("csv_data")
dat = dataFactory.csv.create_tic_dat("csv_data", freeze_it=True)
# the foreign key and data type data checks are part of solve

solution = solve(dat)

assert solution, "unexpected infeasibility"

solutionFactory.csv.write_directory(solution, os.path.join("csv_data", "solution"),
                                    allow_overwrite=True)