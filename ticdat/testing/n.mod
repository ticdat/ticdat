tuple commodities_type
{
	key string name;
};

{commodities_type} COMMODITIES=...;

tuple inflow_type
{
	key string commodity;
	key string node;
	string quantity;
};

{inflow_type} INFLOW=...;

tuple nodes_type
{
	key string name;
};

{nodes_type} NODES=...;

tuple cost_type
{
	key string commodity;
	key string source;
	key string destination;
	string cost;
};

{cost_type} COST=...;

tuple arcs_type
{
	key string source;
	key string destination;
	string capacity;
};

{arcs_type} ARCS=...;

