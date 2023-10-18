--Report for each country, how many of its citizens have a FaceInPage

--Load FaceInPage
faceInPage = LOAD '/Project2/Data/Final/faceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

--Group by Nationality
clean = FOREACH faceInPage GENERATE Nationality;
clean1 = GROUP clean BY Nationality;

--Count users of each Nationality
final = FOREACH clean1 GENERATE group, COUNT(clean.Nationality);

--Output
dump final;
STORE final INTO '/Project2/Output/TaskC/Final' USING PigStorage(',');

