#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.
#
# edited with permission from Gurobi Optimization, Inc.

# Solve a multi-commodity flow problem.

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

# the whole schema has only three data fields to type
dataFactory.set_data_type("arcs", "capacity")
dataFactory.set_data_type("cost", "cost")
# except quantity which allows negatives
dataFactory.set_data_type("inflow", "quantity", min=-float("inf"), inclusive_min=False)

solutionFactory = TicDatFactory(
        flow = [["commodity", "source", "destination"], ["quantity"]])



def create_model(dat):
    '''
    :param dat: a good ticdat for the dataFactory
    :return: a gurobi model and dictionary of gurboi flow variables
    '''
    assert dataFactory.good_tic_dat_object(dat)
    assert not dataFactory.find_foreign_key_failures(dat)
    assert not dataFactory.find_data_type_failures(dat)

    # Create optimization model
    m = Model('netflow')

    # Create variables
    flow = {}
    for h, i, j in dat.cost:
        if (i,j) in dat.arcs:
            flow[h,i,j] = m.addVar(ub=dat.arcs[i,j]["capacity"], obj=dat.cost[h,i,j]["cost"],
                                   name='flow_%s_%s_%s' % (h, i, j))
    m.update()

    flowselect = tuplelist(flow)

    # Arc capacity constraints
    for i_,j_ in dat.arcs:
        m.addConstr(quicksum(flow[h,i,j] for h,i,j in flowselect.select('*',i_, j_)) <= dat.arcs[i_,j_]["capacity"],
                    'cap_%s_%s' % (i_, j_))


    # for readability purposes (and also backwards compatibility with gurobipy) using a dummy variable
    # thats always zero
    zero = m.addVar(lb=0, ub=0, name = "forcedToZero")
    m.update()

    # Flow conservation constraints. Constraints are generated only for relevant pairs.
    # So we generate a conservation of flow constraint if there is negative or positive inflow
    # quantity, or at least one inbound flow variable, or at least one outbound flow variable.
    for h_,j_ in set(k for k,v in dat.inflow.items() if abs(v["quantity"]) > 0).union(
            {(h,i) for h,i,j in flow}, {(h,j) for h,i,j in flow}) :
        m.addConstr(
          (quicksum(flow[h,i,j] for h,i,j in flowselect.select(h_,'*',j_)) or zero) +
              dat.inflow.get((h_,j_), {"quantity":0})["quantity"] ==
          (quicksum(flow[h,i,j] for h,i,j in flowselect.select(h_, j_, '*')) or zero),
                   'node_%s_%s' % (h_, j_))
    return m, flow

def solve(dat):
    m, flow = create_model(dat)

    # Compute optimal solution
    m.optimize()

    if m.status == GRB.status.OPTIMAL:
        rtn = solutionFactory.TicDat()
        for (h, i, j),var in flow.items():
            if var.x > 0:
                # ticdat recognizes flow as a one-data-field table, thus making write through easy
                rtn.flow[h,i,j] = var.x
        return rtn

