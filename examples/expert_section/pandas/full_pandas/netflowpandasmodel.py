#!/usr/bin/python

# Copyright 2015, Opalytics, Inc.
#
# edited with permission from Gurobi Optimization, Inc.

# Solve a multi-commodity flow problem as python package.
# This version of the file uses pandas both for table and for complex data
# manipulation. There is no slicing or iterating over indexes.

# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python netflowpandasmodel.py -i csv_data -o solution_csv_data
# will read from a model stored in .csv files in the csv_data directory
# and write the solution to .csv files in the solution_csv_data directory

from gurobipy import *
from ticdat import TicDatFactory, standard_main

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

# the whole schema has only three data fields to type - two are default type
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
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the dataFactory
    :return: a good ticdat for the solutionFactory, or None
    """
    m, flow = create_model(dat)

    # Compute optimal solution
    m.optimize()

    if m.status == GRB.status.OPTIMAL:
        t = flow.apply(lambda r : r.x)
        # TicDat is smart enough to handle a Series for a single data field table
        return solutionFactory.freeze_me(solutionFactory.TicDat(flow = t[t > 0]))

def create_model(dat):
    '''
    :param dat: a good ticdat for the dataFactory
    :return: a gurobi model and a Series of gurboi flow variables
    '''
    assert dataFactory.good_tic_dat_object(dat)
    assert not dataFactory.find_foreign_key_failures(dat)
    assert not dataFactory.find_data_type_failures(dat)

    dat = dataFactory.copy_to_pandas(dat, drop_pk_columns=False)

    # Create optimization model
    m = Model('netflow')

    flow = dat.cost.join(dat.arcs, on = ["source", "destination"],
                         how = "inner", rsuffix="_arcs")\
        .apply(lambda r : m.addVar(ub=r.capacity, obj=r.cost,
                                   name='flow_%s_%s_%s'%
                                        (r.commodity, r.source, r.destination)),
               axis=1, reduce=True)
    m.update()

    # combining aggregate with gurobipy.quicksum is more efficient than using sum
    flow.groupby(level=["source", "destination"])\
        .aggregate({"flow": quicksum})\
        .join(dat.arcs)\
        .apply(lambda r : m.addConstr(r.flow <= r.capacity,
                                      'cap_%s_%s' %(r.source, r.destination)),
               axis =1)

    m.update()
    def flow_subtotal(node_fld, sum_field_name):
        rtn = flow.groupby(level=['commodity',node_fld])\
                  .aggregate({sum_field_name : quicksum})
        rtn.index.names = [u'commodity', u'node']
        return rtn

    # We need a proxy for zero because of the toehold problem, and
    # we use quicksum([]) instead of a dummy variable because of the fillna problem.
    # (see notebooks in this directory and parent directory)
    zero_proxy = quicksum([])
    flow_subtotal("destination", "flow_in")\
        .join(dat.inflow[abs(dat.inflow.quantity) > 0].quantity, how="outer")\
        .join(flow_subtotal("source", "flow_out"), how = "outer")\
        .fillna(zero_proxy)\
        .apply(lambda r : m.addConstr(r.flow_in + r.quantity  - r.flow_out == 0,
                                      'cons_flow_%s_%s' % r.name),
               axis =1)

    m.update()

    return m, flow
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/mdb files
if __name__ == "__main__":
    standard_main(dataFactory, solutionFactory, solve)
# ---------------------------------------------------------------------------------
