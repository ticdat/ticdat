-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57
CREATE VIEW badCategories AS
Select name, 'Bad maxNutrition value' badDetail, tic_fields, ticid from badCategoriesMaxNutrition
union
Select name, 'Bad minNutrition value' badDetail, tic_fields, ticid from badCategoriesMinNutrition