CREATE TABLE inpNutritionQuantities (
  food not null,
  category not null,
  qty real_not_negative_not_inf default 0 ,
  ticid default 0 not null,
  FOREIGN KEY(food) REFERENCES inpFoods(name) on delete cascade,
  FOREIGN KEY(category) REFERENCES inpCategories(name) on delete cascade,
  PRIMARY KEY(food, category)
);