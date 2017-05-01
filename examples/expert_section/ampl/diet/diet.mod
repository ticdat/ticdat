set categories;
set foods;
set nutrition_quantities within (foods cross categories);

param cost {foods};

param min_nutrition {categories};
param max_nutrition {i in categories};

param quantity {nutrition_quantities};

var Buy {j in foods} >= 0;

minimize Total_Cost:  sum {j in foods} cost[j] * Buy[j];

subject to Diet {i in categories}:
   min_nutrition[i] <= sum {j in foods} quantity[j,i] * Buy[j] <= max_nutrition[i];