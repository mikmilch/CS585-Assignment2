associates = LOAD '/Project1/Testing/associatesTest.csv' USING PigStorage(',') AS (FriendRel: int, PersonA_ID: int, PersonB_ID: int, DateofFriendship: int, Descr: chararray);
faceInPage = LOAD '/Project1/Testing/faceInPageTest.csv' USING PigStorage(',') AS (id: int, name: chararray, nationality: chararray, countryCod: int, hobby: chararray);

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
test = JOIN count BY group, count1 BY group;

-- Get Final
testing = FOREACH test GENERATE $0, ($1 + $3);

testing1 = JOIN testing BY $0, faceInPage BY id;

final = FOREACH testing1 GENERATE $3, $1;


dump final;
--dump a;
STORE final INTO '/Project2/Pig/TaskB/Test/taskD.csv' USING PigStorage(',');