CREATE TABLE slnConsumeNutrition
(category  NOT NULL,
 qty default 0,
 FOREIGN KEY(category) REFERENCES inpCategories(name) on delete cascade,
 PRIMARY KEY (category));