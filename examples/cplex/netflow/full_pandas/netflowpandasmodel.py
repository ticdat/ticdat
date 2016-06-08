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

# this version of the file uses CPLEX
from docplex.mp.model import Model
from ticdat import TicDatFactory

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
_cplex_key = "api_e22f42ce-fddd-4e98-a48a-e23ea8a79d0a"
_cplex_url = "https://api-oaas.docloud.ibmcloud.com/job_manager/rest/v1/"
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
        cplex_soln = m.solution
        t = flow.apply(lambda var : cplex_soln.get_value(var))
        # TicDat is smart enough to handle a Series for a single data field table
        return solutionFactory.freeze_me(solutionFactory.TicDat(flow = t[t > 0]))

def create_model(dat):
    '''
    :param dat: a good ticdat for the dataFactory
    :return: a docplex model and a Series of docplex flow variables
    '''
    assert dataFactory.good_tic_dat_object(dat)
    assert not dataFactory.find_foreign_key_failures(dat)
    assert not dataFactory.find_data_type_failures(dat)

    dat = dataFactory.copy_to_pandas(dat, drop_pk_columns=False)

    # Create optimization model
    m = Model('netflow')

    flow = dat.cost.join(dat.arcs, on = ["source", "destination"],
                                       how = "inner", rsuffix="_arcs").\
              apply(lambda r : m.continuous_var(name= 'flow_%s_%s_%s'%
                                (r.commodity, r.source, r.destination)),
                    axis=1, reduce=True)

    # combining aggregate with m.sum is more efficient than using sum
    flow.groupby(level=["source", "destination"])\
        .aggregate({"flow": m.sum})\
        .join(dat.arcs)\
        .apply(lambda r : m.addConstr(r.flow <= r.capacity,
                                      'cap_%s_%s' %(r.source, r.destination)),
               axis =1)

    def flow_subtotal(node_fld, sum_field_name):
        rtn = flow.groupby(level=['commodity',node_fld])\
                  .aggregate({sum_field_name : m.sum})
        rtn.index.names = [u'commodity', u'node']
        return rtn

    # quicksum([]) instead of the number 0 insures proper constraints are created
    zero_proxy = quicksum([])
    flow_subtotal("destination", "flow_in")\
        .join(dat.inflow[abs(dat.inflow.quantity) > 0].quantity, how="outer")\
        .join(flow_subtotal("source", "flow_out"), how = "outer")\
        .fillna(zero_proxy)\
        .apply(lambda r : m.addConstr(r.flow_in + r.quantity  - r.flow_out == 0,
                                      'cons_flow_%s_%s' % r.name),
               axis =1)

    m.minimize(dat.cost.join(flow).
               apply(lambda r : r.flow * r.cost, axis = 1, reduce = True).
               sum())


    return m, flow
# ---------------------------------------------------------------------------------
