-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW badParametersKeys AS
Select key_, 'Unrecognized key' columnRequirement,
'key_' tic_fields, ticid
from inpParameters where key_ not in (Select key_ from _goodParameterKeys)
            