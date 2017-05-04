include ticdat_diet.mod;

var Buy {j in foods} >= 0;

minimize Total_Cost:  sum {j in foods} foods_cost[j] * Buy[j];

subject to Diet {i in categories}:
   categories_min_nutrition[i] <= sum {j in foods} nutritionQuantities_qty[j,i] * Buy[j] <= categories_max_nutrition[i];
