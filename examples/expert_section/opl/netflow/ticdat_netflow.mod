{string} nodes = ...;
{string} commodities = ...;

tuple cost_type
{
	key string commodity;
	key string source;
	key string destination;
	float cost;
};

{cost_type} cost=...;

tuple arcs_type
{
	key string source;
	key string destination;
	float capacity;
};

{arcs_type} arcs=...;

tuple inflow_type
{
	key string commodity;
	key string node;
	float quantity;
};

{inflow_type} inflow=...;

