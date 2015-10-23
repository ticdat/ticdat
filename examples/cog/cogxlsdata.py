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
from ticdat import LogFactory

if os.path.exists("cog.stop"):
    print "Removing the cog.stop file so that solve can proceed."
    print "Add cog.stop whenever you want to stop the optimization"
    os.remove("cog.stop")

# read the data from cog.xls into a TicDat object dat
dat = dataFactory.xls.create_tic_dat("cog.xls", freeze_it=True)

def percentError(lb, ub):
    assert lb<=ub
    return "%.2f"%(100.0 * (ub-lb) / ub) + "%"

def progressFunction(progDict):
    if set(progDict.keys()) == {"Upper Bound", "Lower Bound"}:
        print "Percent Error".ljust(30) + percentError(progDict["Lower Bound"], progDict["Upper Bound"])
    else :
        for k,v in progDict.items():
            print k.ljust(30) + str(v)
    # return False (to stop optimization) if the cog.stop file exists
    return not os.path.exists("cog.stop")

# solve the model using a generic LogFactory object
solution = solve(dat, LogFactory(), progressFunction)

if solution :
    print('\n\nUpper Bound   : %g' % solution.parameters["Upper Bound"]["value"])
    print('Lower Bound   : %g' % solution.parameters["Lower Bound"]["value"])
    print('Percent Error : %s' % percentError(solution.parameters["Lower Bound"]["value"],
                                              solution.parameters["Upper Bound"]["value"]))
    # will write to a solution.xls file, overwriting any pre-existing file
    solutionFactory.xls.write_file(solution, "solution.xls", allow_overwrite=True)
else :
    print('\nNo solution')

