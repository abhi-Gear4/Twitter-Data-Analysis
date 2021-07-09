drop table followers;
drop table follow_count;

create table followers (
person int,
follower int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table followers;

create table follow_count (
follower int,
count int)
row format delimited fields terminated by ',' stored as textfile;

INSERT INTO follow_count
SELECT followers.follower,COUNT(followers.person) 
FROM followers
GROUP BY followers.follower;

SELECT follow_count.count,COUNT(*)
FROM follow_count
GROUP BY follow_count.count ;

