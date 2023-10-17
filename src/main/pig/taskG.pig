/*
Identify "outdated" FaceInPages. Return IDs and Names of persons that have not accessed FaceIn for 90 days (i.e., no entries in the AccessLog in the last 90 days).
 */
pages = LOAD 'shared_folder/accessLogsTest.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);
group_pages = GROUP pages BY ByWho;
min_pages = FOREACH group_pages GENERATE group, MIN(pages.AccessTime) AS Min;
filtered_pages = FILTER min_pages BY Min > 129600;

users = LOAD 'shared_folder/faceInPageTest.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
usersValues = FOREACH users GENERATE ID, Name;

joined = JOIN filtered_pages BY group, usersValues BY ID;
product = FOREACH joined GENERATE $2, $3;

STORE product INTO './output' USING PigStorage(',');