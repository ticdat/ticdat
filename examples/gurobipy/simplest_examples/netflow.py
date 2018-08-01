# Simplest netflow example using gurobipy and ticdat

import gurobipy as gu
from ticdat import TicDatFactory, standard_main, Slicer, gurobi_env

input_schema = TicDatFactory (
     commodities = [["Name"],[]],
     nodes  = [["Name"],[]],
     arcs = [["Source", "Destination"],["Capacity"]],
     cost = [["Commodity", "Source", "Destination"], ["Cost"]],
     inflow = [["Commodity", "Node"],["Quantity"]]
)

solution_schema = TicDatFactory(
        flow = [["Commodity", "Source", "Destination"], ["Quantity"]],
        parameters = [["Parameter"],["Value"]])

def solve(dat):
    assert input_schema.good_tic_dat_object(dat)

    mdl = gu.Model("netflow", env=gurobi_env())

    flow = {(h, i, j): mdl.addVar(name='flow_%s_%s_%s' % (h, i, j)) for h, i, j in dat.cost}

    flowslice = Slicer(flow)

    # Arc Capacity constraints
    for i,j in dat.arcs:
        mdl.addConstr(gu.quicksum(flow[h_i_j] for h_i_j in flowslice.slice('*',i, j))
                      <= dat.arcs[i,j]["Capacity"],
                      name='cap_%s_%s' % (i, j))

    # Flow conservation constraints. Constraints are generated only for relevant pairs.
    # So we generate a conservation of flow constraint if there is negative or positive inflow
    # quantity, or at least one inbound flow variable, or at least one outbound flow variable.
    for h in dat.commodities:
        for j in dat.nodes:
            mdl.addConstr(
                gu.quicksum(flow[h_i_j] for h_i_j in flowslice.slice(h,'*',j)) +
                dat.inflow.get((h,j), {"Quantity":0})["Quantity"] ==
                gu.quicksum(flow[h_j_i] for h_j_i in flowslice.slice(h, j, '*')),
                name='node_%s_%s' % (h, j))

    mdl.setObjective(gu.quicksum(flow * dat.cost[h, i, j]["Cost"]
                                 for (h, i, j), flow in flow.items()),
                     sense=gu.GRB.MINIMIZE)
    mdl.optimize()

    if mdl.status == gu.GRB.OPTIMAL:
        rtn = solution_schema.TicDat()
        for (h, i, j), var in flow.items():
            if var.x > 0:
                rtn.flow[h, i, j] = var.x
        rtn.parameters["Total Cost"] = sum(dat.cost[h, i, j]["Cost"] * r["Quantity"]
                                           for (h, i, j), r in rtn.flow.items())
        return rtn

# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
