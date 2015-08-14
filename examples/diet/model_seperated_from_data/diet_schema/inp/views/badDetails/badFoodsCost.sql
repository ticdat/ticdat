-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57
CREATE VIEW badFoodsCost AS
Select name, cost, 'Non-negative number, not inf required for cost' columnRequirement,
'cost' tic_fields, ticid
from inpFoods where cost is null or
                    not ((typeof(cost) = 'real' or typeof(cost) = 'integer') and cost >= 0)