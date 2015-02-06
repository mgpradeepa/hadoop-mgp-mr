// Create table: 
create table avgNumber(num Int) ; 


// Load data file to hdfs for HIVE
load data local inpath '/home/ubuntu/Public/MGP/DataFOlder/AvgNumberData' overwrite into table avgNumber;

// Query to find  the average of numbers
hive> select avg(num) from avgNumber;
