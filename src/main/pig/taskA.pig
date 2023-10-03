--LOAD '/Project1/Final/faceInPage.csv' USING PigStorage('\t') AS (id: int, name: chararray, nationality: chararray, countryCod: int, hobby: chararray);

--test = LOAD '/Project1/Testing/faceInPageTest.csv' USING PigStorage(',') AS (id: int, name: chararray, nationality: chararray, countryCod: int, hobby: chararray);
test = LOAD '/Project1/Final/faceInPage.csv' USING PigStorage(',') AS (id: int, name: chararray, nationality: chararray, countryCod: int, hobby: chararray);


clean1 = FILTER test BY nationality == 'American';

clean2 = FOREACH clean1 GENERATE id, name, nationality;

dump clean2;
--STORE clean2 INTO '/Project2/Pig/TaskA/taskA.csv' USING PigStorage(',');

