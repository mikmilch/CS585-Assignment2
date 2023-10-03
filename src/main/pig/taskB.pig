test = LOAD '/Project1/Testing/faceInPageTest.csv' USING PigStorage(',') AS (id: int, name: chararray, nationality: chararray, countryCod: int, hobby: chararray);

clean = FOREACH test GENERATE nationality;
clean1 = GROUP clean BY nationality;

--clean2 = FOREACH clean1 GENERATE group, COUNT(clean1);
testing = FOREACH clean1 GENERATE group, COUNT(clean.nationality);

dump testing;

STORE testing INTO '/Project2/Pig/TaskB/Test/taskB.csv' USING PigStorage(',');

