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
# It can select an argument to select between using the pandas version of the netflow model
# or the version that is based on the gurobi example file.
#
# Creating a similar driver file for the other ticdat formats (xls, SQLite, mdb, static data) would be
# trivial. See the diet example for more details.

import os
import sys
import getopt
from netflowticdatfactories import dataFactory, solutionFactory
import netflowmodel as nf
import netflowpandasmodel as nfpd


def main(argv):
    normal_solve = pandas_solve = None
    try:
      opts, args = getopt.getopt(argv,"hnp",["normal", "pandas"])
    except getopt.GetoptError:
      print 'netflowcsvdata.py -np'
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print 'netflowcsvdata.py -np'
         sys.exit()
      elif opt in ("-n", "--normal"):
         normal_solve = nf.solve
      elif opt in ("-p", "--pandas"):
         pandas_solve = nfpd.solve

    if normal_solve and pandas_solve:
        print "cannot specify both -n and -p"
        sys.exit(2)
    if not (normal_solve or pandas_solve):
        normal_solve = nf.solve

    assert not dataFactory.csv.get_duplicates("csv_data")
    dat = dataFactory.csv.create_tic_dat("csv_data", freeze_it=True)
    # the foreign key and data type data checks are part of solve

    solution = (normal_solve or pandas_solve)(dat)

    assert solution, "unexpected infeasibility"

    solutionFactory.csv.write_directory(solution, os.path.join("csv_data", "solution"),
                                        allow_overwrite=True)


if __name__ == "__main__":
   main(sys.argv[1:])