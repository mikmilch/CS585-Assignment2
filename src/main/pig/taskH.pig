--Report all owners of a FaceInPage who are more popular than an average user, namely,
--those who have more relationships than the average number of relationships across all
--owners FaceInPages.

--Load Associates
associates = LOAD '/Project2/Data/Final/associates.csv' USING PigStorage(',') AS (FriendRel: int, PersonA_ID: int, PersonB_ID: int, DateofFriendship: int, Descr: chararray);

-- Map PersonA_ID and PersonB_ID and the other way around
clean1 = FOREACH associates GENERATE PersonA_ID, PersonB_ID;
clean2 = FOREACH associates GENERATE PersonB_ID, PersonA_ID;

-- Grouping by PersonA_ID and PersonB_ID
clean3 = GROUP clean1 BY PersonA_ID;
clean4 = GROUP clean2 BY PersonB_ID;

-- Count the occurance of each user relationship
count = FOREACH clean3 GENERATE group, COUNT(clean1.PersonA_ID);
count1 = FOREACH clean4 GENERATE group, COUNT(clean2.PersonB_ID);

-- Join
joined = JOIN count BY group, count1 BY group;

-- Count of all Relations of a user
countRelations = FOREACH joined GENERATE $0, ($1 + $3);

-- Group all together in order to get the average relationships of all users
group1 = FOREACH countRelations GENERATE 1, $0, $1;
joinedRelation = GROUP group1 BY $0;

-- Averagr
average = FOREACH joinedRelation GENERATE group, group1.$0, AVG(group1.$2);

--Filter By those more popular than the avaerage
popular = Filter countRelations BY $1 > average.$2;

--Final
final = FOREACH popular GENERATE $0;

--Output
dump final;
STORE final INTO '/Project2/Output/TaskH/Final' USING PigStorage(',');