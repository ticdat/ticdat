#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.

# Separate the model (cogmodel.py) from the data file (cogxlsdata.py), so
# that the model can be solved with different data files.
#
# This file solves the model with the data provided by the Excel file "cog.xls".
# It prints the summary information to the screen, and writes the solution into a
# a "solution.xls" file, overwriting any pre-existing "solution.xls" file.
#

from cogmodel import solve, dataFactory, solutionFactory
import os

# read the data from cog.xls into the FrozenTicDat object dat
dat = dataFactory.xls.create_frozen_tic_dat("cog.xls")

solution = solve(dat)

if solution :
    print('\nUpper Bound: %g' % solution.parameters["Upper Bound"]["value"])
    print('Lower Bound: %g' % solution.parameters["Lower Bound"]["value"])
    # will write to a solution.xls file, overwriting any pre-existing file
    solutionFactory.xls.write_file(solution, "solution.xls", allow_overwrite=True)
else :
    print('\nNo solution')

