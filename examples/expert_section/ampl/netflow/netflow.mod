include ticdat_netflow.mod;

var flow {j in arcs} >= 0;

minimize total_flow: sum { j in arcs}