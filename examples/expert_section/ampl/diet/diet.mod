include ticdat_diet.mod;

var buy {j in foods} >= 0;

minimize total_cost:  sum {j in foods} foods_cost[j] * Buy[j];

subject to Diet {i in categories}:
   categories_min_nutrition[i] <= sum {j in foods} nutrition_quantities_quantity[j,i] * Buy[j] <= categories_max_nutrition[i];
