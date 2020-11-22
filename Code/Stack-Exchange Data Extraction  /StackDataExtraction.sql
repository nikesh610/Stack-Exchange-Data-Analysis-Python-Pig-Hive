
--Website  - https://data.stackexchange.com/stackoverflow/query/new

--Queries to extract top 20000 rows based on most ViewCount

--Dataset 1 
select top 50000 * from Posts where ViewCount >=112523

--Dataset 2 
select top 50000 * from Posts where ViewCount <112523 AND ViewCount >=66243

--Dataset 3 
select top 50000 * from Posts where ViewCount <66243AND ViewCount >= 47290

--Dataset 4 
select top 50000 * from Posts where ViewCount < 47290 AND ViewCount >=36780


