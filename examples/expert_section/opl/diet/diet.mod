tuple Foods_type
{
	key string name;
	float cost;
};

{Foods_type} FOODS=...;

tuple Categories_type
{
	key string name;
	float min_nutrition;
	float max_nutrition;
};

{Categories_type} CATEGORIES=...;

tuple Nutrition_quantities_type
{
	key string food;
	key string category;
	float qty;
};

{Nutrition_quantities_type} NUTRITION_QUANTITIES=...;

// Josh, please fill the rest of this in. Please please please pretty please.