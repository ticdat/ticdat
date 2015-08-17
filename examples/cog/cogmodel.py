#!/usr/bin/python

# Copyright 2015, Opalytics, Inc
#
# Solve the Center of Gravity problem from _A Deep Dive into Strategic Network Design Programming_
# http://amzn.to/1Lbd6By
#

import time
import datetime
import gurobipy as gu
from ticdat import TicDatFactory, freeze_me

# define the input schema. There are three input tables, with 4 primary key fields
# and 4 data fields amongst them.
dataFactory = TicDatFactory (
     sites      = [['name'],['demand', 'center_status']],
     distance   = [['source', 'destination'],['distance']],
     parameters = [["key_"], ["value"]])

# define the solution schema. There are three solution tables, with 2 primary key fields
# and 3 data fields amongst them.
solutionFactory = TicDatFactory(
    openings    = [['site'],[]],
    assignments = [['site', 'assigned_to'],[]],
    parameters  = [["key_"], ["value"]])

def timeStamp() :
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def solve(dat, logFactory, progressFunction = lambda progDict : True):
    assert dataFactory.good_tic_dat_object(dat)
    with logFactory.LogFile("output.txt") as out :
        out.write("COG output log\n%s\n\n"%timeStamp())
        with logFactory.LogFile("error.txt") as err:
            err.write("COG error log\n%s\n\n"%timeStamp())

            def getDistance(x,y):
                if (x,y) in dat.distance:
                    return dat.distance[x,y]["distance"]
                if (y,x) in dat.distance:
                    return dat.distance[y,x]["distance"]
                return float("inf")

            def canAssign(x, y):
                return dat.sites[y]["center_status"] == "canBeCenter" and getDistance(x,y)<float("inf")


            unassignables = [n for n in dat.sites if not any(canAssign(n,y) for y in dat.sites) and
                                                     dat.sites[n]["demand"] > 0]
            if unassignables:
                err.write("The following sites have demand, but can't be assigned to anything.\n")
                err.long_sequence(unassignables)
                return

            useless = [n for n in dat.sites if not any(canAssign(y,n) for y in dat.sites) and
                                                     dat.sites[n]["demand"] == 0]
            if useless:
                err.write(
"The following sites have no demand, and serve as the center point for any assignments.\n")
                err.long_sequence(useless)

            progressFunction({"Feasibility Analysis" : 100})

            m = gu.Model("cog")

            assignVars = { (n, assignedTo) : m.addVar(vtype = gu.GRB.BINARY,
                                                name = "%s_%s"%(n,assignedTo),
                                                obj = getDistance(n,assignedTo) * dat.sites[n]["demand"])
                    for n in dat.sites for assignedTo in dat.sites if canAssign(n,assignedTo)}
            openVars = { n : m.addVar(vtype = gu.GRB.BINARY, name = "open_%s"%n)
                            for n in dat.sites if dat.sites[n]["center_status"] == "canBeCenter"}
            if not openVars:
                err.write("Nothing can be a center!\n")
                return

            m.update()

            progressFunction({"Core Model Creation" : 50})

            inboundAssignVars = {assignedTo : set() for assignedTo in openVars}
            outboundAssignVars = {n : 0 for n in dat.sites}
            for (n,assignedTo),assignVar in assignVars.items() :
                inboundAssignVars[assignedTo].add(assignVar)
                outboundAssignVars[n] += assignVar

            for n, varSum in outboundAssignVars.items():
                if dat.sites[n]["demand"] > 0:
                    m.addConstr(varSum == 1, name = "must_assign_%s"%n)

            crippledForDemo = "formulation" in dat.parameters and \
                              dat.parameters["formulation"]["value"] == "weak"
            if crippledForDemo:
                for assignedTo, _assignVars in inboundAssignVars.items() :
                    m.addConstr(gu.quicksum(_assignVars) <= len(_assignVars) * openVars[assignedTo],
                                name="weak_force_open%s"%assignedTo)
            else:
                for assignedTo, _assignVars in inboundAssignVars.items() :
                    for var in _assignVars :
                        m.addConstr(var <= openVars[assignedTo],
                                    name = "strong_force_open_%s"%assignedTo)

            numberOfCentroids = dat.parameters["numberOfCentroids"]["value"] \
                                if "numberOfCentroids" in dat.parameters else 1
            m.addConstr(gu.quicksum(v for v in openVars.values()) == numberOfCentroids,
                        name= "numCentroids")

            if "mipGap" in dat.parameters:
                m.Params.MIPGap = dat.parameters["mipGap"]["value"]
            m.update()
            if "lpFileName" in dat.parameters:
                m.write(dat.parameters["lpFileName"]["value"])

            progressFunction({"Core Model Creation" : 100})

            with logFactory.GurobiCallBackAndLog(dat.parameters["guLogFileName"]["value"]
                    if "guLogFileName" in dat.parameters else None,
                    lambda lb,ub : progressFunction({"Lower Bound":lb, "Upper Bound":ub})) as guLog:
                m.optimize(guLog.create_gurobi_callback())

            progressFunction({"Core Optimization" : 100})

            if not hasattr(m, "status"):
                print "missing status - likely premature termination"
                return
            for failStr,grbKey in (("inf_or_unbd", gu.GRB.INF_OR_UNBD),
                                   ("infeasible", gu.GRB.INFEASIBLE),
                                   ("unbounded", gu.GRB.UNBOUNDED)):
                 if m.status == grbKey:
                    print "Optimization failed due to model status of %s"%failStr
                    return

            if m.status == gu.GRB.INTERRUPTED:
                err.write("Solve process interrupted by user feedback\n")
                if not all(hasattr(var, "x") for var in openVars.values()):
                    err.write("No solution was found\n")
                    return
            elif m.status != gu.GRB.OPTIMAL:
                err.write("unexpected status %s\n"%m.status)
                return

            sln = solutionFactory.TicDat()
            sln.parameters["Lower Bound"] = getattr(m, "objBound", m.objVal)
            sln.parameters["Upper Bound"] = m.objVal
            out.write('Upper Bound: %g\n' % sln.parameters["Upper Bound"]["value"])
            out.write('Lower Bound: %g\n' % sln.parameters["Lower Bound"]["value"])

            def almostOne(x) :
                return abs(x-1) < 0.0001

            for (n, assignedTo), var in assignVars.items() :
                if almostOne(var.x) :
                    sln.assignments[n,assignedTo] = {}
            for n,var in openVars.items() :
                if almostOne(var.x) :
                    sln.openings[n]={}
            out.write('Number Centroids: %s\n' % len(sln.openings))
            progressFunction({"Full Cog Solve" : 100})
            return freeze_me(sln)





