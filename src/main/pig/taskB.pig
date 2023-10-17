/* Get the ID of the 10 FaceIn pages with the most accesses based on the AccessLog. Return Id, Name and Nationality from FaceInPage. */

--Load Access Logs
--accessLogs = LOAD 'shared_folder/accessLogsTest.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);
accessLogs = LOAD '/Project1/Testing/accessLogsTest.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

--Load FaceInPage
--faceInPage = LOAD 'shared_folder/faceInPageTest.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
faceInPage = LOAD '/Project1/Testing/faceInPageTest.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

-- Group and Count by Page accessed
group_pages = GROUP accessLogs BY WhatPage;
count_pages = FOREACH group_pages GENERATE group, COUNT(accessLogs) AS Count;

-- Get only the 10 most accessed pages
top_pages = ORDER count_pages BY Count DESC;
top_10_pages = LIMIT top_pages 10;

-- For each user generate their Id, name, and nationality
usersValues = FOREACH faceInPage GENERATE ID, Name, Nationality;

-- Join to get only the top 10
joined = JOIN top_10_pages BY group, usersValues BY ID;
product = FOREACH joined GENERATE $2, $3, $4;

--Output
--STORE product INTO './output' USING PigStorage(',');

STORE product INTO '/Project2/Output/TaskB/Test' USING PigStorage(',');
