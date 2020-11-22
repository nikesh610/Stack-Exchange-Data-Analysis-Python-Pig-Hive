--Creating a hive table to store the data from processed dataset 
CREATE TABLE hiveTable (Id INT, Score INT, Body String, OwnerUserId INT, Title String, Tags String) ROW format delimited FIELDS TERMINATED BY ',' ;

--Loading the exported data from Pig  into the table
LOAD DATA INPATH '/finpig/processedData' INTO TABLE hiveTable;

--  3(i) ->  To fetch  top 10 posts by score
SELECT Id, Title, Score FROM hiveTable ORDER BY Score DESC LIMIT 10;

--  3(ii) -> The top 10 users by post score
SELECT OwnerUserId, SUM(Score) AS TotalScore FROM hiveTable GROUP BY OwnerUserId ORDER BY TotalScore DEC LIMIT 10;

-- 3(iii) -> The number of distinct users, who used the word "hadoop" in one of their posts
SELECT COUNT (DISTINCT OwnerUserId) FROM hiveTable WHERE (LOWER(title) LIKE '%hadoop%' OR LOWER(body) LIKE '%hadoop%' OR LOWER(tags) like '%hadoop%');
