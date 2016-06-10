#!/usr/bin/python

# Copyright 2015, 2016 Opalytics, Inc.
#

# Solve a multi-commodity flow problem as python package.
# This version of the file doesn't use pandas at all, but instead uses ticdat
# dict-of-dicts to represent data tables and ticdat.Slicer for slicing.

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.

# this version of the file uses CPLEX
from docplex.mp.model import Model
from ticdat import TicDatFactory, Slicer

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
# the following are cplex credentials for cloud solving. Edit as needed.
_cplex_url = "GET YOUR OWN FROM CPLEX"
_cplex_key = "GET YOUR OWN FROM CPLEX"
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the dataFactory
    :return: a good ticdat for the solutionFactory, or None
    """
    m, flow = create_model(dat)

    # Compute optimal solution
    if m.solve(url=_cplex_url, key=_cplex_key):
        rtn = solutionFactory.TicDat()
        cplex_soln = m.solution
        for (h, i, j),var in flow.items():
            if cplex_soln.get_value(var) > 0:
# ticdat recognizes flow as a one-data-field table, thus making write through easy
                rtn.flow[h,i,j] = cplex_soln.get_value(var)
        return rtn

def create_model(dat):
    '''
    :param dat: a good ticdat for the dataFactory
    :return: a docplex model and dictionary of docplex flow variables
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
            flow[h,i,j] = m.continuous_var(name='flow_%s_%s_%s' % (h, i, j))

    # docplex doesn't provide a native slicer so we use ticdat.Slicer
    flowslice = Slicer(flow)

    # Arc capacity constraints
    for i_,j_ in dat.arcs:
        # docplex doesn't currently support float("inf") as RHS
        if dat.arcs[i_,j_]["capacity"] < float("inf"):
            m.add_constraint(m.sum(flow[h,i,j] for h,i,j in flowslice.slice('*',i_, j_))
                         <= dat.arcs[i_,j_]["capacity"],
                         ctname = 'cap_%s_%s' % (i_, j_))


    # for readability purposes using a dummy variable thats always zero
    zero = m.continuous_var(lb=0, ub=0, name = "forcedToZero")

    # Flow conservation constraints. Constraints are generated only for relevant pairs.
    # So we generate a conservation of flow constraint if there is negative or positive inflow
    # quantity, or at least one inbound flow variable, or at least one outbound flow variable.
    for h_,j_ in set(k for k,v in dat.inflow.items() if abs(v["quantity"]) > 0).union(
            {(h,i) for h,i,j in flow}, {(h,j) for h,i,j in flow}) :
        m.add_constraint(
          (m.sum(flow[h,i,j] for h,i,j in flowslice.slice(h_,'*',j_)) or zero) +
              dat.inflow.get((h_,j_), {"quantity":0})["quantity"] ==
          (m.sum(flow[h,i,j] for h,i,j in flowslice.slice(h_, j_, '*')) or zero),
                   ctname = 'node_%s_%s' % (h_, j_))

    # setting the objective function
    m.minimize(m.sum(flow * dat.cost[h, i, j]["cost"] for (h, i, j),flow in flow.items()))

    return m, flow
# ---------------------------------------------------------------------------------
