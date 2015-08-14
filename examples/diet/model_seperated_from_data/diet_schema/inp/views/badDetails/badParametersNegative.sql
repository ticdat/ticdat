-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW badParametersNegative AS
Select key_, value, 'Value cannot be negative for this key' columnRequirement,
'key_ value' tic_fields, ticid
from inpParameters where key_ in (Select key_ from _goodParameterKeys where rejectsNegative)
and ((lower(value) = '-inf') or
     ((typeof(value) = 'real' or typeof(value) = 'integer') and value < 0 ))
            