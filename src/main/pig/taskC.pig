test = LOAD '/Project1/Testing/faceInPageTest.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

clean = FOREACH test GENERATE Nationality;
clean1 = GROUP clean BY Nationality;

--clean2 = FOREACH clean1 GENERATE group, COUNT(clean1);
testing = FOREACH clean1 GENERATE group, COUNT(clean.Nationality);

dump testing;

STORE testing INTO '/Project2/Pig/TaskB/Test/taskB.csv' USING PigStorage(',');

