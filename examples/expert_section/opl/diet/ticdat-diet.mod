tuple nutritionQuantities_type
{
	key string food;
	key string category;
	string qty;
};

{nutritionQuantities_type} NUTRITIONQUANTITIES=...;

tuple foods_type
{
	key string name;
	string cost;
};

{foods_type} FOODS=...;

tuple categories_type
{
	key string name;
	string minNutrition;
	string maxNutrition;
};

{categories_type} CATEGORIES=...;

