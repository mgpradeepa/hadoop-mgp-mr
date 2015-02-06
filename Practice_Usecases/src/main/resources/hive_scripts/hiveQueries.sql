SELECT c.custid, c.score, c.location                
 FROM rank_customer c                                
ORDER BY c.custid ASC, c.location ASC, c.score DESC;



123	5	INDIA
123	1	INDIA
123	0	USA
124	1	INDIA
125	5	USA
126	9	INDIA
126	5	USA
126	0	USA




hive> EXPLAIN SELECT c.custid, c.score, c.location                
    > FROM rank_customer c                                
    > ORDER BY c.custid ASC, c.location ASC, c.score DESC;
    
    
 SELECT * FROM (
select * from customer_score where type like '%e%' 
UNION ALL Select a.*  from (Select * from customer_score where type like '%o%') a LEFT OUTER JOIN 
(Select * from customer_score where type like '%e%') b ON (a.custid = b.custid) WHERE b.custid is NULL
) c;
 
 
 
 SELECT * FROM (
 SELECT * FROM customer_score WHERE type LIKE '%e'% 
 UNION ALL  
 SELECT a.* FROM (SELECT * FROM customer_score WHERE type LIKE '%o%') a LEFT OUTER JOIN
 (SELECT * FROM customer_Score  WHERE type LIKE '%e%' ) b
 ON (a.custid = b.custid) WHERE b.custid IS NULL)c
 
 
  