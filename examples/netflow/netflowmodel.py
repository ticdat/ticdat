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


solutionFactory = TicDatFactory()

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

    arcs = tuplelist(dat.arcs)
    # Flow conservation constraints
    for h in dat.commodities:
        for j in dat.nodes:
            m.addConstr(
              quicksum(flow[h,i,j] for i,j in arcs.select('*',j) if (h,i,j) in flow) +
                  dat.inflow[h,j]["quantity"] ==
              quicksum(flow[h,j,k] for j,k in arcs.select(j,'*') if (h,j,k) in flow),
                       'node_%s_%s' % (h, j))

    # Compute optimal solution
    m.optimize()