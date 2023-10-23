
--Identify people that have a relationship with someone (Associates); yet never accessed
--their respective friend’s FaceInPage. Report IDs and names.

--Load Associates
associates = LOAD '/Project2/Data/Final/associates.csv' USING PigStorage(',') AS (FriendRel:int, PersonA_ID:int, PersonB_ID:int, DateOfFriendship:int, Description:chararray);

--Load AccessLogs
accessLogs = LOAD '/Project2/Data/Final/accessLogs.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

--Load FaceInPages
FaceInPage = LOAD '/Project2/Data/Final/faceInPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

--Map Relationships Between Users
friends = FOREACH associates GENERATE PersonA_ID, PersonB_ID;
friends2 = FOREACH associates GENERATE PersonB_ID, PersonA_ID;
friends = UNION friends, friends2;

-- Join AccessLogs with Associates
accessLogs = FOREACH accessLogs GENERATE ByWho, WhatPage;
accessLogs = JOIN accessLogs BY ByWho, friends BY $0;


--Find Friends that Have already Access a Page
goodFriends = FILTER accessLogs BY WhatPage == $3;
goodFriends = FOREACH goodFriends GENERATE $2, $3;
goodFriends = DISTINCT goodFriends;


--
--Find Friend sthat have a relationship but nevered accessed their page
badFriends = JOIN friends BY $0 LEFT OUTER, goodFriends BY $0;
badFriends = FILTER badFriends BY $2 IS NULL;
badFriends = FOREACH badFriends GENERATE $0, $1;
badFriends = ORDER badFriends BY $0;

--
--
-- FaceInPage
person = FOREACH FaceInPage GENERATE ID, Name;


-- Join with FaceInPage to Get Name
badFriendsNames = JOIN badFriends BY $0, FaceInPage BY ID;
badFriendsNames = FOREACH badFriendsNames GENERATE $0, $1, $3;
badFriendsNames = JOIN badFriendsNames BY $1, person BY $0;
badFriendsNames = ORDER badFriendsNames BY $0;
--
-- Generate id and Name of both users
badFriendsNames = FOREACH badFriendsNames GENERATE $0, $2, $3, $4;
--
----Output
--
STORE badFriendsNames INTO '/Project2/Output/TaskF/Final' USING PigStorage(',');

