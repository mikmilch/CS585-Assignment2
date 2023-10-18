--For each FaceInPage, compute the “happiness factor” of its owner. That is, for each
--FaceInPage, report the owner’s name, and the number of relationships they have. For
--page owners that aren't listed in Associates, return a score of zero. Please note that we
--maintain a symmetric relationship, take that into account in your calculations

--Load Associates
associates = LOAD '/Project2/Data/Final/associates.csv' USING PigStorage(',') AS (FriendRel: int, PersonA_ID: int, PersonB_ID: int, DateofFriendship: int, Descr: chararray);
--Load FaceInPage
faceInPage = LOAD '/Project2/Data/Final/faceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

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

-- Count total realtionships of each user
countRelations = FOREACH joined GENERATE $0, ($1 + $3);

--Join with FaceInPage
joined2 = JOIN countRelations BY $0, faceInPage BY ID;

-- For each user generate their name and number of relationships
final = FOREACH joined2 GENERATE $3, $1;

--Output
dump final;
STORE final INTO '/Project2/Output/TaskD/Final' USING PigStorage(',');