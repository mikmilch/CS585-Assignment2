
-- Loading FaceInPage
faceInPage = LOAD '/Project1/Final/faceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Filter by only American Users
clean1 = FILTER faceInPage BY Nationality == 'American';

-- For each user generate their id, name and hobby
clean2 = FOREACH clean1 GENERATE ID, Name, Hobby;

--Output
dump clean2;
STORE clean2 INTO '/Project2/Output/TaskA/Final' USING PigStorage(',');



