-- 4 -> Using Hive calculating the per-user TF-IDF (To submit the top 10 terms for each of the top 10 users from Question 3(ii) )

add jar /home/nikesh.pothabattula2/hivemall-core-0.4.2-rc.2-with-dependencies.jar;
source /home/nikesh.pothabattula2/define-all.hive;


create temporary macro max2(x INT, y INT) if(x>y,x,y);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT) tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);


create table distOwnerIDs as SELECT OwnerUserId, SUM(Score) AS TotalScore FROM hiveTable GROUP BY OwnerUserId ORDER BY TotalScore DESC LIMIT 10;

create table mainUSRData as Select HT.OwnerUserID,title from hiveTable  HT JOIN distOwnerIDs DO on  HT.OwnerUserID = DO.OwnerUserID

create or replace view mainUSRView as select ownerUserId, eachword from mainUSRData LATERAL VIEW explode(tokenize(title, True)) t as eachword where not is_stopword(eachword);

create or replace view tempView as select ownerUserid, eachword, freq from (select ownerUserId, tf(eachword) as word2freq from mainUSRView group by ownerUserId) t LATERAL VIEW explode(word2freq) t2 as eachword, freq;

create or replace view tfFinalView as select * from (select ownerUserId, eachword, freq,rank()  over (partition by ownerUserId order by freq desc) as rn from tempView as t) as t where t.rn<=10 ;

select * from tfFinalView;  