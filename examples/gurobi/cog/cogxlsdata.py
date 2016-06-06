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
from ticdat import LogFile, Progress

def percent_error(lb, ub):
    assert lb<=ub
    return "%.2f"%(100.0 * (ub-lb) / ub) + "%"

if os.path.exists("cog.stop"):
    print "Removing the cog.stop file so that solve can proceed."
    print "Add cog.stop whenever you want to stop the optimization"
    os.remove("cog.stop")

# read the data from cog.xls into a TicDat object dat
dat = dataFactory.xls.create_tic_dat("cog.xls", freeze_it=True)

class CogStopProgress(Progress):
    def mip_progress(self, theme, lower_bound, upper_bound):
        super(CogStopProgress, self).mip_progress(theme, lower_bound, upper_bound)
        print "%s:%s:%s"%(theme.ljust(30), "Percent Error".ljust(20),
                          percent_error(lower_bound, upper_bound))
        # return False (to stop optimization) if the cog.stop file exists
        return not os.path.exists("cog.stop")

# solve the model using a generic LogFactory object
with LogFile("output.txt") as out :
    with LogFile("error.txt") as err :
        solution = solve(dat, out, err, CogStopProgress())

if solution :
    print('\n\nUpper Bound   : %g' % solution.parameters["Upper Bound"]["value"])
    print('Lower Bound   : %g' % solution.parameters["Lower Bound"]["value"])
    print('Percent Error : %s' % percent_error(solution.parameters["Lower Bound"]["value"],
                                               solution.parameters["Upper Bound"]["value"]))
    # will write to a solution.xls file, overwriting any pre-existing file
    solutionFactory.xls.write_file(solution, "solution.xls", allow_overwrite=True)
else :
    print('\nNo solution')

