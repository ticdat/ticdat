-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57
CREATE VIEW badCategoriesMaxNutrition AS
Select name, maxNutrition, 'Non-negative number (or inf) required for maxNutrition' columnRequirement,
'maxNutrition' tic_fields, ticid
from inpCategories where maxNutrition is null or
                    not (((typeof(maxNutrition) = 'real' or typeof(maxNutrition) = 'integer') and maxNutrition >= 0)
                          or lower(maxNutrition) = 'inf')
                          or maxNutrition is null