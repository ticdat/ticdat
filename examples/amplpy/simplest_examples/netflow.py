# Simplest multi-commodity flow example using amplpy and ticdat

from ticdat import PanDatFactory, standard_main
from amplpy import AMPL

input_schema = PanDatFactory (
    commodities=[["Name"], ["Volume"]],
    nodes=[["Name"], []],
    arcs=[["Source", "Destination"] ,["Capacity"]],
    cost=[["Commodity", "Source", "Destination"], ["Cost"]],
    inflow=[["Commodity", "Node"], ["Quantity"]]
)

solution_schema = PanDatFactory(
    flow=[["Commodity", "Source", "Destination"], ["Quantity"]],
    parameters=[["Parameter"],["Value"]])

def solve(dat):
    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    set NODES;
    set ARCS within {i in NODES, j in NODES: i <> j};
    set COMMODITIES;
    param volume {COMMODITIES} > 0, < Infinity;
    param capacity {ARCS} >= 0;
    param cost {COMMODITIES,ARCS} >= 0, < Infinity;
    param inflow {COMMODITIES,NODES} > -Infinity, < Infinity;
    var Flow {COMMODITIES,ARCS} >= 0;
    minimize TotalCost:
       sum {h in COMMODITIES, (i,j) in ARCS} cost[h,i,j] * Flow[h,i,j];
    subject to Capacity {(i,j) in ARCS}:
       sum {h in COMMODITIES} Flow[h,i,j] * volume[h] <= capacity[i,j];
    subject to Conservation {h in COMMODITIES, j in NODES}:
       sum {(i,j) in ARCS} Flow[h,i,j] + inflow[h,j] = sum {(j,i) in ARCS} Flow[h,j,i];
    """)

    # copy the tables to amplpy.DataFrame objects, renaming the data fields as needed
    dat = input_schema.copy_to_ampl(dat, field_renamings={("commodities", "Volume"): "volume",
            ("arcs", "Capacity"): "capacity", ("cost", "Cost"): "cost", ("inflow", "Quantity"): "inflow"})

    # load the amplpy.DataFrame objects into the AMPL model, explicitly identifying how to populate the AMPL sets
    input_schema.set_ampl_data(dat, ampl, {"nodes": "NODES", "arcs": "ARCS",
                                           "commodities": "COMMODITIES"})
    ampl.solve()
    if ampl.getValue("solve_result") != "infeasible":
        # solution tables are populated by mapping solution (table, field) to AMPL variable
        sln = solution_schema.copy_from_ampl_variables(
            {('flow' ,'Quantity'):ampl.getVariable("Flow")})
        # append the solution KPI results to the solution parameters table
        sln.parameters.loc[0] = ['Total Cost', ampl.getObjective('TotalCost').value()]
        return sln

# when run from the command line, will read/write xls/csv/json/db files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)