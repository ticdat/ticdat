CREATE TABLE inpFoods
-- true, false strings are turned into bool objects
(name NOT NULL CHECK((lower(name) != 'true') and (lower(name) != 'false')),
 cost real_not_negative_not_inf default 0 ,
 ticid default 0 not null,
PRIMARY KEY (name));