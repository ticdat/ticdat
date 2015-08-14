-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW goodParameters AS
Select p.key_, p.value, p.ticid
from inpParameters p
left outer join badParameters b on p.key_ = b.key_
where b.key_ is null
                