
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

accessLogs = FOREACH accessLogs GENERATE ByWho, WhatPage;
accessLogs = JOIN accessLogs BY ByWho, friends BY PersonA_ID;

--Find Friends that Have already Access a Page
goodFriends = FILTER accessLogs BY WhatPage == PersonB_ID;
goodFriends = FOREACH goodFriends GENERATE PersonA_ID, PersonB_ID;
goodFriends = DISTINCT goodFriends;

--Find Friend sthat have a relationship but nevered accessed their page

badFriends = JOIN friends BY PersonB_ID LEFT OUTER, goodFriends BY PersonB_ID;
badFriends = FILTER badFriends BY goodFriends::friends::PersonB_ID IS NULL;
badFriends = FOREACH badFriends GENERATE $0, $1;
badFriends = ORDER badFriends BY $0;

badFriends = FOREACH badFriends GENERATE $0, $1;

-- FaceInPage
person = FOREACH FaceInPage GENERATE ID, Name;
dump person;

-- Join with FaceInPage to Get Name
badFriendsNames = JOIN badFriends BY $0, FaceInPage BY ID;
badFriendsNames = FOREACH badFriendsNames GENERATE $0, $1, $3;
badFriendsNames = JOIN badFriendsNames BY $1, person BY $0;
badFriendsNames = ORDER badFriendsNames BY $0;

-- Generate id and Name of both users
badFriendsNames = FOREACH badFriendsNames GENERATE $0, $2, $3, $4;

--Output
dump badFriendsNames;

STORE badFriendsNames INTO '/Project2/Output/TaskF/Final' USING PigStorage(',');