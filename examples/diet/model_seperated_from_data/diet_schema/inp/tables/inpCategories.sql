CREATE TABLE inpCategories
-- true, false strings are turned into bool objects
(name NOT NULL CHECK((lower(name) != 'true') and (lower(name) != 'false')),
 minNutrition real_not_negative_not_inf_not_bigger_than_max default 0 , --  not larger than maxNutrition
 maxNutrition real_not_negative_not_smaller_than_min default 'inf' , -- not smaller than minNutrition, inf allowed
 ticid default 0 not null,
PRIMARY KEY (name));