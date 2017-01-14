tuple nutrition_quantities_type
{
	key string food;
	key string category;
	float qty;
};

{nutrition_quantities_type} nutrition_quantities=...;

tuple foods_type
{
	key string name;
	float cost;
};

{foods_type} foods=...;

tuple categories_type
{
	key string name;
	float min_nutrition;
	float max_nutrition;
};

{categories_type} categories=...;

