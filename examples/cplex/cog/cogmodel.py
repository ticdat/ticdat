#!/usr/bin/python

# Copyright 2015, Opalytics, Inc
#
# Solve the Center of Gravity problem from _A Deep Dive into Strategic Network Design Programming_
# http://amzn.to/1Lbd6By
#
# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.

# this version of the file uses CPLEX

import time
import datetime
from docplex.mp.model import Model
from ticdat import TicDatFactory, Progress, LogFile, Slicer

# ------------------------ define the input schema --------------------------------
# There are three input tables, with 4 primary key fields  and 4 data fields.
dataFactory = TicDatFactory (
     sites      = [['name'],['demand', 'center_status']],
     distance   = [['source', 'destination'],['distance']],
     parameters = [["key"], ["value"]])

# add foreign key constraints
dataFactory.add_foreign_key("distance", "sites", ['source', 'name'])
dataFactory.add_foreign_key("distance", "sites", ['destination', 'name'])

# center_status is a flag field which can take one of two string values.
dataFactory.set_data_type("sites", "center_status", number_allowed=False,
                          strings_allowed=["Can Be Center", "Pure Demand Point"])
# The default type of non infinite, non negative works for distance
dataFactory.set_data_type("distance", "distance")
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 2 primary key fields and 3
# data fields amongst them.
solutionFactory = TicDatFactory(
    openings    = [['site'],[]],
    assignments = [['site', 'assigned_to'],[]],
    parameters  = [["key"], ["value"]])
# ---------------------------------------------------------------------------------

# ------------------------ create a solve function --------------------------------
def time_stamp() :
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

# the following are cplex credentials for cloud solving. Edit as needed.
_cplex_url = "GET YOUR OWN FROM CPLEX"
_cplex_key = "GET YOUR OWN FROM CPLEX"

def solve(dat, out, err, progress):
    assert isinstance(progress, Progress)
    assert isinstance(out, LogFile) and isinstance(err, LogFile)
    assert dataFactory.good_tic_dat_object(dat)
    assert not dataFactory.find_foreign_key_failures(dat)
    assert not dataFactory.find_data_type_failures(dat)
    out.write("COG output log\n%s\n\n"%time_stamp())
    err.write("COG error log\n%s\n\n"%time_stamp())

    def get_distance(x,y):
        if (x,y) in dat.distance:
            return dat.distance[x,y]["distance"]
        if (y,x) in dat.distance:
            return dat.distance[y,x]["distance"]
        return float("inf")

    def can_assign(x, y):
        return dat.sites[y]["center_status"] == "Can Be Center" \
               and get_distance(x,y)<float("inf")


    unassignables = [n for n in dat.sites if not
                     any(can_assign(n,y) for y in dat.sites) and
                     dat.sites[n]["demand"] > 0]
    if unassignables:
        # Infeasibility detected. Generate an error table and return None
        err.write("The following sites have demand, but can't be " +
                  "assigned to anything.\n")
        err.log_table("Un-assignable Demand Points",
                      [["Site"]] + [[_] for _ in unassignables])
        return

    useless = [n for n in dat.sites if not any(can_assign(y,n) for y in dat.sites) and
                                             dat.sites[n]["demand"] == 0]
    if useless:
        # Log in the error table as a warning, but can still try optimization.
        err.write("The following sites have no demand, and can't serve as the " +
                  "center point for any assignments.\n")
        err.log_table("Useless Sites", [["Site"]] + [[_] for _ in useless])

    progress.numerical_progress("Feasibility Analysis" , 100)

    m = Model("cog")

    assign_vars = {(n, assigned_to) : m.binary_var(name = "%s_%s"%(n,assigned_to))
                    for n in dat.sites for assigned_to in dat.sites
                    if can_assign(n, assigned_to)}
    open_vars = {n : m.binary_var(name = "open_%s"%n)
                     for n in dat.sites
                     if dat.sites[n]["center_status"] == "Can Be Center"}
    if not open_vars:
        err.write("Nothing can be a center!\n") # Infeasibility detected.
        return

    progress.numerical_progress("Core Model Creation", 50)

    assign_slicer = Slicer(assign_vars)

    for n, r in dat.sites.items():
        if r["demand"] > 0:
            m.add_constraint(m.sum(assign_vars[n, assign_to]
                                    for _, assign_to in assign_slicer.slice(n, "*"))
                             == 1,
                            ctname = "must_assign_%s"%n)

    crippledfordemo = "formulation" in dat.parameters and \
                      dat.parameters["formulation"]["value"] == "weak"
    for assigned_to, r in dat.sites.items():
        if r["center_status"] == "Can Be Center":
            _assign_vars = [assign_vars[n, assigned_to]
                            for n,_ in assign_slicer.slice("*", assigned_to)]
            if crippledfordemo:
                m.add_constraint(m.sum(_assign_vars) <=
                            len(_assign_vars) * open_vars[assigned_to],
                            ctname="weak_force_open%s"%assigned_to)
            else:
                for var in _assign_vars :
                    m.add_constraint(var <= open_vars[assigned_to],
                                ctname = "strong_force_open_%s"%assigned_to)

    number_of_centroids = dat.parameters["Number of Centroids"]["value"] \
                          if "Number of Centroids" in dat.parameters else 1
    if number_of_centroids <= 0:
        err.write("Need to specify a positive number of centroids\n") # Infeasibility detected.
        return

    m.add_constraint(m.sum(v for v in open_vars.values()) == number_of_centroids,
                ctname= "numCentroids")

    # !TMP!
    # if "mipGap" in dat.parameters:
    #     m.Params.MIPGap = dat.parameters["mipGap"]["value"]
    progress.numerical_progress("Core Model Creation", 100)

    m.minimize(m.sum(var * get_distance(n,assigned_to) * dat.sites[n]["demand"]
                     for (n, assigned_to),var in assign_vars.items()))

    progress._add_listener(m) # !TMP!

    if m.solve(url=_cplex_url, key=_cplex_key):

        progress.numerical_progress("Core Optimization", 100)
        cplex_soln = m.solution
        sln = solutionFactory.TicDat()
        sln.parameters["Lower Bound"] = cplex_soln.get_objective_value() # !TMP!
        sln.parameters["Upper Bound"] = cplex_soln.get_objective_value() # !TMP!
        out.write('Upper Bound: %g\n' % sln.parameters["Upper Bound"]["value"])
        out.write('Lower Bound: %g\n' % sln.parameters["Lower Bound"]["value"])

        def almostone(x) :
            return abs(x-1) < 0.0001

        for (n, assigned_to), var in assign_vars.items() :
            if almostone(cplex_soln.get_value(var)) :
                sln.assignments[n,assigned_to] = {}
        for n,var in open_vars.items() :
            if almostone(cplex_soln.get_value(var)) :
                sln.openings[n]={}
        out.write('Number Centroids: %s\n' % len(sln.openings))
        progress.numerical_progress("Full Cog Solve",  100)
        return sln
# ---------------------------------------------------------------------------------





