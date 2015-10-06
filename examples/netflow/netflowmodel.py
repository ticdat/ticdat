#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.
#
# edited with permission from Gurobi Optimization, Inc.

# Solve a multi-commodity flow problem.  Two products ('Pencils' and 'Pens')
# are produced in 2 cities ('Detroit' and 'Denver') and must be sent to
# warehouses in 3 cities ('Boston', 'New York', and 'Seattle') to
# satisfy demand ('inflow[h,i]').
#
# Flows on the transportation network must respect arc capacity constraints
# ('capacity[i,j]'). The objective is to minimize the sum of the arc
# transportation costs ('cost[i,j]').

from gurobipy import *
from ticdat import TicDatFactory

# define the input schema.
dataFactory = TicDatFactory (
     commodities = [["name"],[]],
     nodes  = [["name"],[]],
     arcs = [["source", "destination"],["capacity"]],
     cost = [["commodity", "source", "destination"], ["cost"]],
     inflow = [["commodity", "node"],["quantity"]]
)

# add foreign key constraints
dataFactory.add_foreign_key("arcs", "nodes", ['source', 'name'])
dataFactory.add_foreign_key("arcs", "nodes", ['destination', 'name'])
dataFactory.add_foreign_key("cost", "nodes", ['source', 'name'])
dataFactory.add_foreign_key("cost", "nodes", ['destination', 'name'])
dataFactory.add_foreign_key("cost", "commodities", ['commodity', 'name'])
dataFactory.add_foreign_key("inflow", "commodities", ['commodity', 'name'])
dataFactory.add_foreign_key("inflow", "nodes", ['node', 'name'])

# the default default types (non infinite, non negative) are fine almost everywhere
dataFactory.set_data_type("arcs", "capacity")
dataFactory.set_data_type("cost", "cost")
# except quantity which allows negatives
dataFactory.set_data_type("inflow", "quantity", min=-float("inf"), inclusive_min=False)

solutionFactory = TicDatFactory(
        flow = [["commodity", "source", "destination"], ["quantity"]])

def solve(dat):
    assert dataFactory.good_tic_dat_object(dat)

    # Create optimization model
    m = Model('netflow')

    # Create variables
    flow = {}
    for h, i, j in dat.cost:
        if (i,j) in dat.arcs:
            flow[h,i,j] = m.addVar(ub=dat.arcs[i,j]["capacity"], obj=dat.cost[h,i,j]["cost"],
                                   name='flow_%s_%s_%s' % (h, i, j))
    m.update()

    # Arc capacity constraints
    for i,j in dat.arcs:
        m.addConstr(quicksum(flow[h,i,j] for h in dat.commodities if (h,i,j) in flow) <= dat.arcs[i,j]["capacity"],
                    'cap_%s_%s' % (i, j))


    # due to using older gurobi version, including a dummy forced to zero var
    zero = m.addVar(lb=0, ub=0, name = "forcedToZero")
    m.update()

    flowselect = tuplelist(flow)

    # Flow conservation constraints - use only relevant pairs for extreme sparsity
    for h_,j_ in set(k for k,v in dat.inflow.items() if abs(v["quantity"]) > 0).union(
            {(h,i) for h,i,j in flow}, {(h,j) for h,i,j in flow}) :
        m.addConstr(
          (quicksum(flow[h,i,j] for h,i,j in flowselect.select(h_,'*',j_)) or zero) +
              dat.inflow.get((h_,j_), {"quantity":0})["quantity"] ==
          (quicksum(flow[h,i,j] for h,i,j in flowselect.select(h_, j_, '*')) or zero),
                   'node_%s_%s' % (h_, j_))

    # Compute optimal solution
    m.optimize()

    if m.status == GRB.status.OPTIMAL:
        rtn = solutionFactory.TicDat()
        solution = m.getAttr('x', flow)
        for h, i, j in flow:
            if solution[h,i,j] > 0:
                rtn.flow[h,i,j] = solution[h,i,j]
        return rtn

