-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW goodNutritionQuantities AS
Select p.food, p.category, p.qty, p.ticid from
inpNutritionQuantities p
left outer join badNutritionQuantities b on p.food = b.food and p.category = b.category
join goodFoods fkt1 on fkt1.name = p.food
join goodCategories fkt0 on fkt0.name = p.category
where b.food is null
         