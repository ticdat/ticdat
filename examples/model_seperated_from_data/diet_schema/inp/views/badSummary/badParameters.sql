-- auto generated view created by SQLDbDeveloperHelper on 2015-03-13 13:26:57

CREATE VIEW badParameters AS
Select key_, 'Bad key' badDetail, tic_fields, ticid from badParametersKeys
union
Select key_, 'Bad value' badDetail, tic_fields, ticid from badParametersValues
union
Select key_, 'Bad numeric value' badDetail, tic_fields, ticid from badParametersNum
union
Select key_, 'Bad negative value' badDetail, tic_fields, ticid from badParametersNegative
union
Select key_, 'Bad non positive value' badDetail, tic_fields, ticid from badParametersNotPositive
               