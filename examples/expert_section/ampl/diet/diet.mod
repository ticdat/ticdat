var Buy {j in foods} >= 0;

minimize Total_Cost:  sum {j in foods} cost[j] * Buy[j];

subject to Diet {i in categories}:
   min_nutrition[i] <= sum {j in foods} quantity[j,i] * Buy[j] <= max_nutrition[i];