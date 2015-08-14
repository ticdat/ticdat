-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW badParametersValues AS
Select p.key_, p.value, 'Unrecognized value for key' columnRequirement,
'key_ value' tic_fields, ticid
from inpParameters p
left outer join _goodParameterKeyValues g on p.key_ = g.key_ and p.value = g.value
where p.key_  in (Select key_ from _goodParameterKeyValues)
and g.key_ is null