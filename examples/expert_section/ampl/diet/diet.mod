include ticdat_diet.mod;

var buy {j in foods} >= 0;

minimize total_cost:  sum {j in foods} foods_cost[j] * buy[j];

subject to Diet {i in categories}:
   categories_min_nutrition[i] <= sum {j in foods} nutrition_quantities_quantity[j,i] * buy[j] <= categories_max_nutrition[i];
