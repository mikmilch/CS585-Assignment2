/*
Determine which people have favorites. That is, for each FaceInPage owner, determine
how many total accesses to FaceInPage they have made (as reported in the AccessLog)
and how many distinct FaceInPages they have accessed in total.
*/

accessLogs = LOAD 'shared_folder/accessLogsTest.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);
FaceInPage = LOAD 'shared_folder/faceInPageTest.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

accessLogsGrouped = GROUP accessLogs BY ByWho;
accessLogsCount = FOREACH accessLogsGrouped GENERATE group, COUNT(accessLogs);

accessLogsDistinct = FOREACH accessLogsGrouped {
    accessLogsDistinct = DISTINCT accessLogs.WhatPage;
    GENERATE group, COUNT(accessLogsDistinct);
}

accessLogsJoined = JOIN accessLogsCount BY group, accessLogsDistinct BY group;

nameJoin = JOIN accessLogsJoined BY $0, FaceInPage BY ID;

-- 0 [index], 1 [name], 3 [total accesses], 5 [total distinct accesses]
nameJoin = FOREACH nameJoin GENERATE $0, $1, $3, $5;

STORE nameJoin INTO './output' USING PigStorage(',');