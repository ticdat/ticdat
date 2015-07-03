CREATE TABLE slnBuyFood
(food  NOT NULL,
 qty default 0,
 FOREIGN KEY(food) REFERENCES inpFoods(name) on delete cascade,
 PRIMARY KEY (food));