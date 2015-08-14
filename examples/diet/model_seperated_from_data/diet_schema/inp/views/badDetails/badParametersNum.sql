-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW badParametersNum AS
Select key_, value, 'Value needs to be a number for this key' columnRequirement,
'key_ value' tic_fields, ticid
from inpParameters where key_ in (Select key_ from _goodParameterKeys where needsNumber)
and not (typeof(value) = 'real' or typeof(value) = 'integer' or lower(value) = 'inf' or lower(value) = '-inf')
        