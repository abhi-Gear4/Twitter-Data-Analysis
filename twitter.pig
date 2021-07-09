A = LOAD '$G' USING PigStorage(',') AS (userid:int, followerid:int);
B = group A by followerid;
C = foreach B generate group,COUNT(A.userid);
D = group C by $1;
E = foreach D generate group,COUNT(C.group);
STORE E into '$O' using PigStorage(',');





