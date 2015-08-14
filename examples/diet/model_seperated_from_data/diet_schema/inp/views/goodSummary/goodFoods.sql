-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW goodFoods AS
Select p.name, p.cost, p.ticid from
inpFoods p
left outer join badFoods b on p.name = b.name

where b.name is null
         