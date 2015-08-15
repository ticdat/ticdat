-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW goodCategories AS
Select p.name, p.minNutrition, p.maxNutrition, p.ticid from
inpCategories p
left outer join badCategories b on p.name = b.name

where b.name is null
         