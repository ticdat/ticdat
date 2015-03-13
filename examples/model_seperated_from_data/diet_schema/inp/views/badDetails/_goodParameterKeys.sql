-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE TABLE _goodParameterKeys
(key_ NOT NULL,
 needsNumber default 0 not null,
 rejectsNegative default 0 not null,
 requiresPositive default 0 not null,
PRIMARY KEY (key_));
update _goodParameterKeys set requiresPositive =1;
update _goodParameterKeys set rejectsNegative =1;
update _goodParameterKeys set needsNumber =1;
Insert into _goodParameterKeys(key_) values ('lpFileName');
Insert into _goodParameterKeys(key_) values ('dbFileName');