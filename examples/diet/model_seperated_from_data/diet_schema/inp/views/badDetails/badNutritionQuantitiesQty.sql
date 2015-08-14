-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57
CREATE VIEW badNutritionQuantitiesQty AS
Select food, category, qty, 'Non-negative number, not inf required for qty' columnRequirement,
'qty' tic_fields, ticid
from inpNutritionQuantities where qty is null or
                    not ((typeof(qty) = 'real' or typeof(qty) = 'integer') and qty >= 0)