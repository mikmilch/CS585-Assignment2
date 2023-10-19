/*Identify "outdated" FaceInPages. Return IDs and Names of persons that have not accessed FaceIn for 90 days (i.e., no entries in the AccessLog in the last 90 days).*/

--Load AccessLogs
accessLogs = LOAD '/Project2/Data/Final/accessLogs.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

--Load FaceInPage
faceInPage = LOAD '/Project2/Data/Final/faceInPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);


--Group by user accessing a page
group_pages = GROUP accessLogs BY ByWho;

--Get users who have not accessed a FaceInPage in 129600 minutes
min_pages = FOREACH group_pages GENERATE group, MIN(accessLogs.AccessTime) AS Min;
filtered_pages = FILTER min_pages BY Min > 129600;

-- For each user generate their Id, name
usersValues = FOREACH faceInPage GENERATE ID, Name;

-- Join to get only those who have not accessed a FaceInPage in 129600 minutes (90 Days)
joined = JOIN filtered_pages BY group, usersValues BY ID;

--For those users generate their Id and Name
product = FOREACH joined GENERATE $2, $3;

--Output
STORE product INTO '/Project2/Output/TaskG/Final' USING PigStorage(',');