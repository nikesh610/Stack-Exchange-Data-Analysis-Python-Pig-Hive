# Stack Exchange Data Analysis using Python,Pig,Hive(Hivemall Library for TF-IDF Calculation)
#### by `Nikesh Pothabattula,DCU Student No: 20211581`

This is a project to perform analysis on Stack Exchange data using **GCP (Google Cloud Platform) Dataproc ** for computing the below tasks using (Python,Pig Latin, Hive, Hive-Mall Library for TF-IDF Calculation) . 

1. Get data from Stack Exchange
2. Load them with PIG
3. Query them with Hive
4. Calculate TF-IDF with Hive

### 1. Extracting data from Stack Exchange
We need to extract the top **2,00,000** data form [**Stack Exchange Data Explorer**]((http://data.stackexchange.com/stackoverflow/query/new)) based on top view count for Posts

Below are the queries to extract top **2,00,000** rows of Data based on view count
```sql
--Sorce File Location - Code/Stack-Exchange Data Extraction/StackDataExtraction.sql

--Dataset 1 
select top 50000 * from Posts where ViewCount >=112523

--Dataset 2 
select top 50000 * from Posts where ViewCount <112523 AND ViewCount >=66243

--Dataset 3 
select top 50000 * from Posts where ViewCount <66243AND ViewCount >= 47290

--Dataset 4 
select top 50000 * from Posts where ViewCount < 47290 AND ViewCount >=36780


```


#### Pre-Processing the data using `Python` language:

 To avoid the complexity and randomness in the extracted data (`csv format`) before loading the dataset using Pig we need to remove the **html tags,commas,tabs,line breaks** to transform the data with ease in Pig.
 
````python

#Source File Location - /Code/Python/preProcess.py

import pandas as pd
import re

def preProcData(fileloc,filename):

#Input the CSV file from the location 
    unProcDS = pd.read_csv(fileloc+filename)
   
    print('Started processing of '+filename+" ...")

    #Removing HTML tags in the Body and Title Column
    unProcDS["Body"] = unProcDS.Body.apply(lambda b: re.sub('<.*?>','', b))
    unProcDS["Title"] = unProcDS.Title.apply(lambda t: re.sub('<.*?>','', t))

    #Removing Commas in Body,Text and Tags fields
    unProcDS["Body"] = unProcDS.Body.apply(lambda x: re.sub(',*','', x))
    unProcDS["Title"] = unProcDS.Title.apply(lambda y: re.sub(',*','', y))
    unProcDS["Tags"] = unProcDS.Title.apply(lambda y: re.sub(',*','', y))

    #Removing  Spaces,Line Breaks,Tab Space in Body and Title Column
    unProcDS["Body"] = unProcDS.Body.apply(lambda x: re.sub('\\n*\\t*\\r*\\s+',' ', x))
    unProcDS["Title"] = unProcDS.Title.apply(lambda y: re.sub('\\n*\\t*\\r*\\s+',' ', y))

    #Writing the processed data to CSV files at the Output folder
    unProcDS.to_csv(fileloc[0:len(fileloc)-12]+"PreProcessed/"+"preProc"+filename)
    print("Sucessfully Processed "+filename+" to Location "+fileloc[0:len(fileloc)-12]+"PreProcessed/"+"preProc"+filename+"\n")
    

def main(): 
    # Location of Extracted Data sets from Stack Exchange
    orgFileloc = "/Users/NikeshPothabattula/Documents/RawDatasets/"
    
    # CDTS is the prefix for extracted datasets  
    orgFilename = "CTDS"

    # As i have extracted 4 CSV files containing 50,000 rows each , I ran the for loop from 1 to 5  to automate 
    # the extration and cleaning of data set by passing the values of dataset location and filename to preProcData function . 
    for i in range(1,5):
     preProcData(orgFileloc,orgFilename+str(i)+".csv")

    
if __name__=="__main__": 
    main() 
````
The Pre-Processed files generated from the execution of above python program are exported to **HDFS** 
````
hdfs dfs -put /Users/NikeshPothabattula/Documents/PreProcessed/ /nikesh_datasets/
````
### 2. Loading the Datasets with PIG
After moving the files from local to HDFS. Using `piggybank.jar` which contains `CSVExcelStorage()` function is used to load `csv` file into PIG.
````pig

--Source File Location - Code/Pig/script.pig

register '/usr/lib/pig/piggybank.jar';
define csvStore org.apache.pig.piggybank.storage.CSVExcelStorage();



-- Loading the Data from PreProcessed Datasets into PIG variables to combine and transform the data 

preProc_Data_1 = load '/nikesh_datasets/PreProcessed/preProcCTDS1.csv' using csvStore(';', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Sno:int,Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

preProc_Data_2 = load '/nikesh_datasets/PreProcessed/preProcCTDS2.csv' using csvStore(';', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Sno:int,Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

preProc_Data_3 = load '/nikesh_datasets/PreProcessed/preProcCTDS3.csv' using csvStore(';', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Sno:int,Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

preProc_Data_4 = load '/nikesh_datasets//PreProcessed/preProcCTDS4.csv' using csvStore(';', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Sno:int,Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

preProc_Comb_Data  = UNION  preProc_Data_1,preProc_Data_2,preProc_Data_3,preProc_Data_4 ;

--Filtering the rows that contain null values in OwnerUserId and Score 
preProc_Data_Filter = filter preProc_Comb_Data by (OwnerUserId is not null) and (Score is not null);

--Selecting the usable colums for hive table 
finData  = foreach preProc_Data_Filter generate  Id as Id, Score as Score, Body as Body, OwnerUserId as OwnerUserId, Title as Title,Tags as Tags;

--Storing the Data to HDFS
store finData into '/finpig/processedData' using csvStore(',');

````
Execute the script as given below to load and transform the data 
````
pig -x mapreduce script.pig
````


After the execution of above pig script it exports the transformed dataset into HDFS location for further execution of hive queries to perform analysis on transformed data 

### 3. Hive Queries to solve below problems 

1. The top 10 posts by score
2. The top 10 users by post score
3. The number of distinct users, who used the word “Hadoop” in one of their posts

We have to enter into the hive query execution environment in order to execute any query using the below command.
````
sudo hive
````
Load the data from HDFS where the Pig output is written and perform the queries as given below to obtain the solutions for the above problems.
````sql
Source File Location - Code/Hive/hive_script.sql

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

````

### 4 Calculating TF-IDF with Hive using Hivemall Library 

TF-IDF (term frequency–inverse document frequency) is a numerical statistic that is intended to reflect how important a word is to a document in a collection (Source: [Wikipedia tf-idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)). We used `hivemall`  in order to compute TF-IDF per-user. 

References  - 
1. [Hivemall Installation](https://github.com/myui/hivemall/wiki/Installation)
2. [TF-IDF Term Weighting Hivemall User Manual](https://hivemall.incubator.apache.org/userguide/ft_engineering/tfidf.html)

````

--Source File Location - /Code/Hive-TF-IDF/TFIDF.sql

-- 4 -> Using Hive calculating the per-user TF-IDF (To submit the top 10 terms for each of the top 10 users from Question 3(ii) )

add jar /home/nikesh.pothabattula2/hivemall-core-0.4.2-rc.2-with-dependencies.jar;
source /home/nikesh.pothabattula2/define-all.hive;

create temporary macro max2(x INT, y INT) if(x>y,x,y);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT) tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);


create table distOwnerIDs as SELECT OwnerUserId, SUM(Score) AS TotalScore FROM hiveTable GROUP BY OwnerUserId ORDER BY TotalScore DESC LIMIT 10;

create table mainUSRData as Select HT.OwnerUserID,title from hiveTable  HT JOIN distOwnerIDs DO on  HT.OwnerUserID = DO.OwnerUserID

create or replace view mainUSRView as select ownerUserId, eachword from mainUSRData LATERAL VIEW explode(tokenize(title, True)) t as eachword where not is_stopword(eachword);

create or replace view tempView as select ownerUserid, eachword, freq from (select ownerUserId, tf(eachword) as word2freq from mainUSRView group by ownerUserId) t LATERAL VIEW explode(word2freq) t2 as eachword, freq;

create or replace view tfFinalView as select * from (select ownerUserId, eachword, freq rank()  over (partition by ownerUserId order by freq desc) as rn from tempView as t) as t where t.rn<=10 ;

select * from tfFinalView;  
````



> All screenshots of above all code executions are included in the `Screenshots` folder in the project.