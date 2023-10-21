/*
Identify people that have a relationship with someone (Associates); yet never accessed
their respective friend’s FaceInPage. Report IDs and names.
*/

associates = LOAD 'shared_folder/associatesTest.csv' USING PigStorage(',') AS (FriendRelationship:int, PersonAID:int, PersonBID:int, DateOfFriendship:int, Description:chararray);
accessLogs = LOAD 'shared_folder/accessLogsTest.csv' USING PigStorage(',') AS (ID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);
FaceInPage = LOAD 'shared_folder/faceInPageTest.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
 
friends = FOREACH associates GENERATE PersonAID, PersonBID;

-- could optimize by finding the most recent accessLog for each person
accessLogs = FOREACH accessLogs GENERATE ByWho, WhatPage;
accessLogs = JOIN accessLogs BY ByWho, friends BY PersonAID;

goodFriends = FILTER accessLogs BY WhatPage == PersonBID;
goodFriends = FOREACH goodFriends GENERATE PersonAID, PersonBID;
goodFriends = DISTINCT goodFriends;

badFriends = JOIN friends BY PersonAID LEFT OUTER, goodFriends BY PersonAID;
badFriends = FILTER badFriends BY goodFriends::friends::PersonAID IS NULL;
badFriends = FOREACH nameJoin GENERATE $0, $1;
badFriends = ORDER badFriends BY $0;

badFriendsNames = JOIN badFriends BY $0, FaceInPage BY ID;
badFriendsNames = FOREACH badFriendsNames GENERATE $2, $3;

STORE badFriendsNames INTO './output' USING PigStorage(',');