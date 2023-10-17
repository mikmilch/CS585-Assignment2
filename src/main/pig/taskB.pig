/* Get the ID of the 10 FaceIn pages with the most accesses based on the AccessLog. Return Id, Name and Nationality from FaceInPage. */
pages = LOAD 'shared_folder/accessLogsTest.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);
group_pages = GROUP pages BY WhatPage;
count_pages = FOREACH group_pages GENERATE group, COUNT(pages) AS Count;
top_pages = ORDER count_pages BY Count DESC;
top_10_pages = LIMIT top_pages 10;

users = LOAD 'shared_folder/faceInPageTest.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
usersValues = FOREACH users GENERATE ID, Name, Nationality;

joined = JOIN top_10_pages BY group, usersValues BY ID;
product = FOREACH joined GENERATE $2, $3, $4;

STORE product INTO './output' USING PigStorage(',');