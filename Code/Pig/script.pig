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


