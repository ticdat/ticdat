#
# Solve a multi-commodity flow problem.
#
# See netflow.py for more information. This file is similar, except it uses PanDatFactory
# instead of TicDatFactory. As a result, the solve function takes a PanDat object, which is simply
# a collection of pre-validated pandas.DataFrame objects consistent with the input_schema.
#
# If your default methodology is to convert input data into pandas.DataFrame objects, then PanDatFactory
# can dramatically help you pre-define the pre and post conditions of your solve subroutine.
#
try:
    import gurobipy as gp
except:
    gp = None
from ticdat import PanDatFactory, standard_main, Slicer

# ------------------------ define the input schema --------------------------------
input_schema = PanDatFactory (
    commodities=[["Name"], ["Volume"]],
    nodes  = [["Name"],[]],
     arcs = [["Source", "Destination"],["Capacity"]],
     cost = [["Commodity", "Source", "Destination"], ["Cost"]],
     inflow = [["Commodity", "Node"],["Quantity"]]
)

# Define the foreign key relationships
input_schema.add_foreign_key("arcs", "nodes", ['Source', 'Name'])
input_schema.add_foreign_key("arcs", "nodes", ['Destination', 'Name'])
input_schema.add_foreign_key("cost", "arcs", [['Source', 'Source'], ['Destination', 'Destination']])
input_schema.add_foreign_key("cost", "nodes", ['Destination', 'Name'])
input_schema.add_foreign_key("cost", "commodities", ['Commodity', 'Name'])
input_schema.add_foreign_key("inflow", "commodities", ['Commodity', 'Name'])
input_schema.add_foreign_key("inflow", "nodes", ['Node', 'Name'])

# Define the data types
input_schema.set_data_type("arcs", "Capacity", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("cost", "Cost", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("inflow", "Quantity", min=-float("inf"), max=float("inf"),
                           inclusive_min=False, inclusive_max=False)
input_schema.set_data_type("commodities", "Volume", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=False)

# The default-default of zero makes sense everywhere except for Capacity
input_schema.set_default_value("arcs", "Capacity", float("inf"))
# ---------------------------------------------------------------------------------

# ------------------------ define the output schema -------------------------------
solution_schema = PanDatFactory(
        flow = [["Commodity", "Source", "Destination"], ["Quantity"]],
        parameters = [["Parameter"],["Value"]])
# ---------------------------------------------------------------------------------

# ------------------------ solving section-----------------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good PanDat object for the input_schema
    :return: a good PanDat object for the solution_schema, or None
    """
    assert input_schema.good_pan_dat_object(dat)
    # PanDat objects might contain duplicate rows, whereas TicDat objects cannot. This is because DataFrames
    # can contain duplicate rows, but dictionaries cannot.
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)


    # Create optimization model
    mdl = gp.Model('netflow')

    # itertuples is the most performant way to iterate over the rows of a DataFrame
    flow = {(h, i, j): mdl.addVar(name=f'flow_{h}_{i}_{j}', obj=cost)
            for h, i, j, cost in dat.cost.itertuples(index=False)}

    flowslice = Slicer(flow)
    volume = {k: volume for k, volume in dat.commodities.itertuples(index=False)}

    # Arc Capacity constraints
    for i, j, capacity in dat.arcs.itertuples(index=False):
        mdl.addConstr(gp.quicksum(flow[_h, _i, _j] * volume[_h]
                                  for _h, _i, _j in flowslice.slice('*', i, j))
                      <= capacity, name=f'cap_{i}_{j}')

    inflow = {(h, j): qty for h, j, qty in dat.inflow.itertuples(index=False) if abs(qty) > 0}
    # Flow conservation constraints. Constraints are generated only for relevant pairs.
    # So we generate a conservation of flow constraint if there is negative or positive inflow
    # quantity, or at least one inbound flow variable, or at least one outbound flow variable.
    for h, j in set(inflow).union({(h, i) for h, i, j in flow}, {(h, j) for h, i, j in flow}):
        mdl.addConstr(
            gp.quicksum(flow[h_i_j] for h_i_j in flowslice.slice(h, '*', j)) +
            inflow.get((h, j), 0) ==
            gp.quicksum(flow[h_j_i] for h_j_i in flowslice.slice(h, j, '*')),
            name=f'node_{h}_{j}')

    # Compute optimal solution
    mdl.optimize()

    if mdl.status == gp.GRB.status.OPTIMAL:
        # PanDatFactory also makes it easy to create DataFrame objects from rows of data
        rtn = solution_schema.PanDat(flow=[[h, i, j, var.x] for (h, i, j), var in flow.items() if var.x > 0],
                                     parameters=[["Total Cost",  mdl.objVal]])
        return rtn
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write xls/csv/db/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
