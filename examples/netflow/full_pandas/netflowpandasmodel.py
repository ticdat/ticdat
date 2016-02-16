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

    flow = dat.cost.join(dat.arcs, on = ["source", "destination"], how = "inner", rsuffix="_arcs").\
              apply(lambda r : m.addVar(ub=r.capacity, obj=r.cost,
                                        name='flow_%s_%s_%s' % (r.commodity, r.source, r.destination)),
                    axis=1, reduce=True)

    m.update()

    dat.arcs.join(flow.groupby(level=["source", "destination"]).aggregate({"flow": quicksum})).apply(
        lambda r : m.addConstr(r.flow <= r.capacity, 'cap_%s_%s' % (r.source, r.destination)), axis =1)


    m.update()
    def flow_subtotal(node_fld, sum_field_name):
    # I had trouble figuring out how to rename just one field in the multiindex,
    # so I move the multi-index in and out of the data columns just for renaming
         return flow.groupby(level=['commodity',node_fld]).aggregate({sum_field_name : quicksum}).\
            reset_index().rename(columns={node_fld:"node"}).set_index(["commodity", "node"])

    flow_subtotal("destination", "flow_in").join(dat.inflow[abs(dat.inflow.quantity) > 0].quantity, how="outer").\
        join(flow_subtotal("source", "flow_out"), how = "outer").fillna(0).\
        apply(lambda r : m.addConstr(r.flow_in + r.quantity  - r.flow_out == 0, 'cap_%s_%s' % r.name), axis =1)

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


