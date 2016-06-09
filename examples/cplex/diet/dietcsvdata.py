#!/usr/bin/python

# Copyright 2015, 2016 Opalytics, Inc.

# Separate the model (dietmodel.py) from the data file (dietcsvdata.py), so
# that the model can be solved with different data files.
#
# This file solves the model with the data provided by csv files in the "diet" directory.
# It prints the summary information to the screen, and writes the solution into a
# a "solution" subdirectory of "diet", overwriting any pre-existing solution.
#

from dietmodel import solve, dataFactory, solutionFactory
import os

# read the data from the diet directory into a TicDat object dat
dat = dataFactory.csv.create_tic_dat("diet", freeze_it=True)

solution =  solve(dat)

if solution :
    print('\nCost: %g' % solution.parameters[0]["totalCost"])
    # will write to a solution subdirectory, overwriting any pre-existing solution
    solutionFactory.csv.write_directory(solution, os.path.join("diet", "solution"),
                                    allow_overwrite=True)
else :
    print('\nNo solution')

