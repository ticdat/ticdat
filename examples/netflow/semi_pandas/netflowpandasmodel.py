#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.
#
# edited with permission from Gurobi Optimization, Inc.

# Solve a multi-commodity flow problem. pandas version

from gurobipy import *
from ticdat import TicDatFactory, Sloc
import pandas as pd

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

    dat = dataFactory.copy_to_pandas(dat, drop_pk_columns=False)

    # Create optimization model
    m = Model('netflow')

    flow = Sloc.add_sloc(dat.cost.join(dat.arcs, on = ["source", "destination"], how = "inner", rsuffix="_arcs").
              apply(lambda r : m.addVar(ub=r.capacity, obj=r.cost,
                                        name='flow_%s_%s_%s' % (r.commodity, r.source, r.destination)),
                    axis=1, reduce=True))
    flow.name = "flow"

    m.update()

    dat.arcs.join(flow.groupby(level=["source", "destination"]).sum()).apply(
        lambda r : m.addConstr(r.flow <= r.capacity, 'cap_%s_%s' % (r.source, r.destination)), axis =1)

    # for readability purposes (and also backwards compatibility with gurobipy) using a dummy variable
    # thats always zero
    zero = m.addVar(lb=0, ub=0, name = "forcedToZero")

    m.update()

    # there is a more pandonic way to do this group of constraints, but lets
    # demonstrate .sloc for those who think it might be more intuitive
    # at some point we can make yet another netflow example that is more fully pandonic
    # and compare all three netflows
    for h_,j_ in sorted(set(dat.inflow[abs(dat.inflow.quantity) > 0].index).union(
        flow.groupby(level=['commodity','source']).groups.keys(),
        flow.groupby(level=['commodity','destination']).groups.keys())):
            m.addConstr((quicksum(flow.sloc[h_,:,j_]) or zero) + dat.inflow.quantity.loc[h_,j_] ==
                        (quicksum(flow.sloc[h_,j_,:]) or zero), 'node_%s_%s' % (h_, j_))

    m.update()
    return m, flow

def solve(dat):
    m, flow = create_model(dat)

    # Compute optimal solution
    m.optimize()

    if m.status == GRB.status.OPTIMAL:
        t = flow.apply(lambda r : r.x)
        # TicDat is smart enough to handle a Series for a single data field table
        return solutionFactory.freeze_me(solutionFactory.TicDat(flow = t[t > 0]))

