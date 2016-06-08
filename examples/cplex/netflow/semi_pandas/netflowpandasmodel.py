#!/usr/bin/python

# Copyright 2015, 2016 Opalytics, Inc.
#
# edited with permission from Gurobi Optimization, Inc.

# Solve a multi-commodity flow problem as python package.
# This version of the file uses pandas for data tables
# but also iterates over table indicies explicitly and uses
# Sloc to perform pandas slicing.

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.


# this version of the file uses CPLEX
from docplex.mp.model import Model
from ticdat import TicDatFactory, Sloc
import pandas as pd

# ------------------------ define the input schema --------------------------------
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
dataFactory.set_data_type("inflow", "quantity", min=-float("inf"),
                          inclusive_min=False)
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
solutionFactory = TicDatFactory(
        flow = [["commodity", "source", "destination"], ["quantity"]])
# ---------------------------------------------------------------------------------

# ------------------------ solving section-----------------------------------------
_cplex_url = "GET YOUR OWN FROM CPLEX"
_cplex_key = "GET YOUR OWN FROM CPLEX"
_cplex_key = "api_e22f42ce-fddd-4e98-a48a-e23ea8a79d0a"
_cplex_url = "https://api-oaas.docloud.ibmcloud.com/job_manager/rest/v1/"
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the dataFactory
    :return: a good ticdat for the solutionFactory, or None
    """
    m, flow = create_model(dat)

    # Compute optimal solution
    if m.solve(url=_cplex_url, key=_cplex_key):
        cplex_soln = m.solution
        t = flow.apply(lambda var : cplex_soln.get_value(var))
        # TicDat is smart enough to handle a Series for a single data field table
        return solutionFactory.freeze_me(solutionFactory.TicDat(flow = t[t > 0]))

def create_model(dat):
    '''
    utility function helpful for troubleshooting
    :param dat: a good ticdat for the dataFactory
    :return: a docplex model and dictionary of docplex flow variables
    '''
    assert dataFactory.good_tic_dat_object(dat)
    assert not dataFactory.find_foreign_key_failures(dat)
    assert not dataFactory.find_data_type_failures(dat)

    dat = dataFactory.copy_to_pandas(dat, drop_pk_columns=False)

    # Create optimization model
    m = Model('netflow')

    flow = Sloc.add_sloc(dat.cost.join(dat.arcs, on = ["source", "destination"],
                                       how = "inner", rsuffix="_arcs").
              apply(lambda r : m.addVar(ub=r.capacity, obj=r.cost,
                            name= 'flow_%s_%s_%s' % (r.commodity, r.source, r.destination)),
                    axis=1, reduce=True))
    flow.name = "flow"

    m.update()

    dat.arcs.join(flow.groupby(level=["source", "destination"]).sum()).apply(
        lambda r : m.addConstr(r.flow <= r.capacity,
                               'cap_%s_%s'%(r.source, r.destination)), axis =1)

    # for readability purposes using a dummy variable thats always zero
    zero = m.addVar(lb=0, ub=0, name = "forcedToZero")

    m.update()

    # there is a more pandonic way to do this group of constraints, but lets
    # demonstrate .sloc for those who think it might be more intuitive
    for h_,j_ in sorted(set(dat.inflow[abs(dat.inflow.quantity) > 0].index).union(
        flow.groupby(level=['commodity','source']).groups.keys(),
        flow.groupby(level=['commodity','destination']).groups.keys())):
            m.addConstr((quicksum(flow.sloc[h_,:,j_]) or zero) +
                        dat.inflow.quantity.loc[h_,j_] ==
                        (quicksum(flow.sloc[h_,j_,:]) or zero),
                        'node_%s_%s' % (h_, j_))

    m.update()
    return m, flow
# ---------------------------------------------------------------------------------
