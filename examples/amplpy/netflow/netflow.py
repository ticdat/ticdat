#
# Solve a multi-commodity flow problem as python package.
#
# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python netflow.py -i netflow_sample_data.sql -o netflow_solution.sql
# will read from the model stored in netflow_sample_data.sql
# and write the solution to netflow_solution.sql

from ticdat import PanDatFactory, standard_main
from amplpy import AMPL
from itertools import product

# ------------------------ define the input schema --------------------------------
input_schema = PanDatFactory (
     commodities = [["Name"],[]],
     nodes  = [["Name"],[]],
     arcs = [["Source", "Destination"],["Capacity"]],
     cost = [["Commodity", "Source", "Destination"], ["Cost"]],
     inflow = [["Commodity", "Node"],["Quantity"]]
)

# Define the foreign key relationships
input_schema.add_foreign_key("arcs", "nodes", ['Source', 'Name'])
input_schema.add_foreign_key("arcs", "nodes", ['Destination', 'Name'])
input_schema.add_foreign_key("cost", "nodes", ['Source', 'Name'])
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
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    assert input_schema.good_pan_dat_object(dat)
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)

    # copy the data over to amplpy.DataFrame objects, renaming the data fields as needed
    dat = input_schema.copy_to_ampl(dat, field_renamings={("arcs", "Capacity"): "capacity",
            ("cost", "Cost"): "cost", ("inflow", "Quantity"): "inflow"})

    # for instructional purposes, the following code anticipates extreme sparsity and doesn't generate
    # conservation of flow records unless they are really needed
    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    set NODES;
    set ARCS within {i in NODES, j in NODES: i <> j};
    set COMMODITIES;
    param capacity {ARCS} >= 0;
    set SHIPMENT_OPTIONS within {COMMODITIES,ARCS};
    param cost {SHIPMENT_OPTIONS} > 0;
    set INFLOW_INDEX within {COMMODITIES,NODES};
    param inflow {INFLOW_INDEX};
    var Flow {SHIPMENT_OPTIONS} >= 0;
    minimize TotalCost:
       sum {(h,i,j) in SHIPMENT_OPTIONS} cost[h,i,j] * Flow[h,i,j];
    subject to Capacity {(i,j) in ARCS}:
       sum {(h,i,j) in SHIPMENT_OPTIONS} Flow[h,i,j] <= capacity[i,j];
    subject to Conservation {h in COMMODITIES, j in NODES:
         card {(h,i,j) in SHIPMENT_OPTIONS} > 0 or
         card {(h,j,i) in SHIPMENT_OPTIONS} > 0 or
         (h,j) in INFLOW_INDEX}:
       sum {(h,i,j) in SHIPMENT_OPTIONS} Flow[h,i,j] +
       (if (h,j) in INFLOW_INDEX then inflow[h,j]) =
       sum {(h,j,i) in SHIPMENT_OPTIONS} Flow[h,j,i];
    """)

    input_schema.set_ampl_data(dat, ampl, {"nodes": "NODES", "arcs": "ARCS",
                                           "commodities": "COMMODITIES", "cost":"SHIPMENT_OPTIONS",
                                            "inflow":"INFLOW_INDEX"})
    ampl.solve()

    if ampl.getValue("solve_result") != "infeasible":
        sln = solution_schema.copy_from_ampl_variables(
            {('flow' ,'Quantity'):ampl.getVariable("Flow")})
        sln.parameters.loc[0] = ['Total Cost', ampl.getObjective('TotalCost').value()]
        return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------