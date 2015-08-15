#!/usr/bin/python

# Copyright 2015, Opalytics, Inc
#
# Solve the Center of Gravity problem from _A Deep Dive into Strategic Network Design Programming_
# http://amzn.to/1Lbd6By
#

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

def solve(dat):
    assert dataFactory.good_tic_dat_object(dat)

    m = gu.Model("cog")

    def getDistance(x,y):
        if (x,y) in dat.distance:
            return dat.distance[x,y]["distance"]
        if (y,x) in dat.distance:
            return dat.distance[y,x]["distance"]
        return float("inf")

    assignVars = { (n, assignedTo) : m.addVar(vtype = gu.GRB.BINARY, name = "%s_%s"%(n,assignedTo),
                                              obj = distance * dat.sites[n]["demand"])
            for n in dat.sites for assignedTo in dat.sites
            for distance in (getDistance(n, assignedTo),)
            if dat.sites[assignedTo]["center_status"] == "canBeCenter" and distance < float("inf")}
    openVars = { n : m.addVar(vtype = gu.GRB.BINARY, name = "open_%s"%n)
                            for n in dat.sites if dat.sites[n]["center_status"] == "canBeCenter"}
    if not openVars:
        print "Nothing can be a center!"
        return

    m.update()

    inboundAssignVars = {assignedTo : set() for assignedTo in openVars}
    outboundAssignVars = {n : 0 for n in dat.sites}
    for (n,assignedTo),assignVar in assignVars.items() :
        inboundAssignVars[assignedTo].add(assignVar)
        outboundAssignVars[n] += assignVar

    for n, varSum in outboundAssignVars.items():
        m.addConstr(varSum == 1, name = "must_assign_%s"%n)

    crippledForDemo = "formulation" in dat.parameters and dat.parameters["formulation"]["value"] == "weak"
    if crippledForDemo:
        for assignedTo, _assignVars in inboundAssignVars.items() :
            m.addConstr(gu.quicksum(_assignVars) <= len(_assignVars) * openVars[assignedTo],
                        name="weak_force_open%s"%assignedTo)
    else:
        for assignedTo, _assignVars in inboundAssignVars.items() :
            for var in _assignVars :
                m.addConstr(var <= openVars[assignedTo], name = "strong_force_open_%s"%assignedTo)

    numberOfCentroids = dat.parameters["numberOfCentroids"]["value"] if "numberOfCentroids" in dat.parameters else 1
    m.addConstr(gu.quicksum(v for v in openVars.values()) == numberOfCentroids, name= "numCentroids")

    if "mipGap" in dat.parameters:
        m.Params.MIPGap = dat.parameters["mipGat"]["value"]

    m.update()
    m.optimize()

    if not hasattr(m, "status"):
        print "missing status - likely premature termination"
        return
    for failStr,grbKey in (("inf_or_unbd", gu.GRB.INF_OR_UNBD), ("infeasible", gu.GRB.INFEASIBLE),
                              ("unbounded", gu.GRB.UNBOUNDED)):
         if m.status == grbKey:
            print "Optimization failed due to model status of %s"%failStr
            return

    if m.status != gu.GRB.OPTIMAL:
        print "unexpected status %s"%m.status
        return

    sln = solutionFactory.TicDat()
    sln.parameters["Lower Bound"] = getattr(m, "objBound", m.objVal)
    sln.parameters["Upper Bound"] = m.objVal

    def almostOne(x) :
        return abs(x-1) < 0.0001

    for (n, assignedTo), var in assignVars.items() :
        if almostOne(var.x) :
            sln.assignments[n,assignedTo] = {}
    for n,var in openVars.items() :
        if almostOne(var.x) :
            sln.openings[n]={}

    return sln





