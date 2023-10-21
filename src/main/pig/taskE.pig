--Determine which people have favorites. That is, for each FaceInPage owner, determine
--how many total accesses to FaceInPage they have made (as reported in the AccessLog)
--and how many distinct FaceInPages they have accessed in total.

--Load AccessLogs
accessLogs = LOAD '/Project2/Data/Final/accessLogs.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);
--Load FaceInPages
FaceInPage = LOAD '/Project2/Data/Final/faceInPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

--Group by Person Accessing a Page
accessLogsGrouped = GROUP accessLogs BY ByWho;

-- Count Number of Total Access Made By a User
accessLogsCount = FOREACH accessLogsGrouped GENERATE group, COUNT(accessLogs);

--Count Number of Distinct Access Made By a User
accessLogsDistinct = FOREACH accessLogsGrouped {
    accessLogsDistinct = DISTINCT accessLogs.WhatPage;
    GENERATE group, COUNT(accessLogsDistinct);
}

--Join
accessLogsJoined = JOIN accessLogsCount BY group, accessLogsDistinct BY group;

--Join with FaceInPage
nameJoin = JOIN accessLogsJoined BY $0, FaceInPage BY ID;

-- 0 [index], 5 [name], 1 [total accesses], 3 [total distinct accesses]
final = FOREACH nameJoin GENERATE $0, $5, $1, $3;

STORE final INTO '/Project2/Output/TaskE/Final' USING PigStorage(',');