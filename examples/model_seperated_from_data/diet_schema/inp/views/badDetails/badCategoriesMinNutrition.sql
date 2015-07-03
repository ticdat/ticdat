-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57
CREATE VIEW badCategoriesMinNutrition AS
Select name, minNutrition, 'Non-negative number, not inf required for minNutrition' columnRequirement,
'minNutrition' tic_fields, ticid
from inpCategories where minNutrition is null or
                    not ((typeof(minNutrition) = 'real' or typeof(minNutrition) = 'integer') and minNutrition >= 0)