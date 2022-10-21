-- @suite
-- @setup
drop table if exists t1;
create table t1 ( id int, c char(10),vc varchar(20));
insert into t1 values (1,'Daffy','Aducklife');
insert into t1 values (1,'Daffy','Aducklife');
insert into t1 values (2,'Bugs','Arabbitlife');
insert into t1 values (3,'Cowboy','Lifeontherange');
insert into t1 values (4,'Anonymous','Wannabuythisbook?');
insert into t1 values (5,'BestSeller','OneHeckuvabook');
insert into t1 values (5,'BestSeller','OneHeckuvabook');
insert into t1 values (6,'EveryoneBu','Thisverybook');
insert into t1 values (7,'SanFran','Itisasanfranlifetyle');
insert into t1 values (8,'BerkAuthor','Cool.Berkly.the.book');
insert into t1 values (9,null,null);
-- insert into t1 values (10,'北京市','中关村');
insert into t1 values (10,'','');

-- @case
-- @desc:test for SUBSTRING
-- @label:bvt
select SUBSTRING(c,1),SUBSTR(vc,1) from t1;
select SUBSTRING(c,2),SUBSTR(vc,2) from t1;
select SUBSTRING(c,5),SUBSTR(vc,9) from t1 where id = 1 ;
select substring(c,11),substr(vc,13) from t1 where id = 6 ;

select SUBSTRING(c,-1),SUBSTR(vc,-1) from t1;
select SUBSTRING(c,-2),SUBSTR(vc,-2) from t1;
select SUBSTRING(c,-5),SUBSTR(vc,-9) from t1 where id = 1 ;

select substring(c,-11),substr(vc,-13) from t1 where id = 6 ;

select SUBSTRING(c,1,1),SUBSTR(vc,1,1) from t1 where id = 1 ;
select SUBSTRING(c,2,2),SUBSTR(vc,3,3) from t1 where id = 1 ;
select SUBSTRING(c,2,5),SUBSTR(vc,3,9) from t1 where id = 1 ;
select substring(c,2,9),substr(vc,3,17) from t1 where id = 4 ;
select substring(c,2,10),substr(vc,3,18) from t1 where id = 4 ;
select substring(c,5,4),substr(vc,5,4) from t1 where id = 4 ;
select substring(c,10,10),substr(vc,18,18) from t1 where id = 4 ;
select substring(c,2,-1),substr(vc,3,-1) from t1 where id = 4 ;

select SUBSTRING(c,-1,1),SUBSTR(vc,1,1) from t1 where id = 1 ;
select SUBSTRING(c,-2,2),SUBSTR(vc,3,3) from t1 where id = 1 ;
select SUBSTRING(c,-2,5),SUBSTR(vc,3,9) from t1 where id = 1 ;
select substring(c,-2,9),substr(vc,3,17) from t1 where id = 4 ;
select substring(c,-2,10),substr(vc,3,18) from t1 where id = 4 ;
select substring(c,1,-1),substr(vc,3,-2) from t1 where id = 4 ;

select substring(c,1,a),substr(vc,3,1) from t1 where id = 4 ;
select substring(c,1,2),substr(vc,a,1) from t1 where id = 4 ;

-- @case
-- @desc:test for SUBSTRING with max,min
-- @label:bvt
select max(substr(c,2)) from t1;
select min(substr(c,2)) from t1;

-- @case
-- @desc:test for SUBSTRING with max,min
-- @label:bvt
select * from t1 where substr(c,2) = 'affy';
select * from t1 where substr(c,2) <> 'affy';
select * from t1 where substr(c,2) > 'affy';
select * from t1 where substr(c,2) > substring('fdasfsad',2);

-- @case
-- @desc:test for SUBSTRING with distinct
-- @label:bvt
select distinct(substr(c,2)) from t1 order by 1;
select distinct(substr(vc,3)) from t1 order by 1;


-- @case
-- @desc:test for endswith
-- @label:bvt
select endswith(c,'a'),endswith(vc,'a') from t1;
select endswith(c,'y'),endswith(vc,'e') from t1;
select * from t1 where endswith(c,'y') = 1;
select * from t1 where endswith(c,'y') = 1 and endswith(vc,'ge') = 1;

-- @case
-- @desc:test for startswith
-- @label:bvt
select startswith(c,'B'),startswith(vc,'A') from t1;
select startswith(c,'y'),startswith(vc,'e') from t1;
select * from t1 where startswith(c,'B') = 1;
select * from t1 where startswith(c,'B') = 1 and startswith(vc,'A') = 1;

-- @case
-- @desc:test for lpad
-- @label:bvt
select lpad(c,0,'*') from t1;
select lpad(c,1,'*') from t1;
select lpad(c,5,'*') from t1;
select lpad(c,10,'*') from t1;

select rpad(c,'1','*') from t1;


-- @case
-- @desc:test for rpad
-- @label:bvt
select rpad(c,0,'*') from t1;
select rpad(c,1,'*') from t1;
select rpad(c,5,'*') from t1;
select rpad(c,10,'*') from t1;

-- @bvt:issue#3165
select rpad(c,'1','*') from t1;
-- @bvt:issue


-- @suite
-- @setup
drop table if exists t1;
create table t1 ( id int, c char(20),vc varchar(50));
insert into t1 values (1,'Daffy  ','  Aducklife');
insert into t1 values (1,'  Daffy  ','Aducklife ');
insert into t1 values (2,' Bugs',' Arabbitlife ');
insert into t1 values (3,'    Cowboy',' Lifeontherange');
insert into t1 values (4,' Anonymous  ',' Wannabuythisbook?  ');
insert into t1 values (5,'  BestSeller',' OneHeckuvabook ');
insert into t1 values (5,'  BestSeller','OneHeckuvabook ');
insert into t1 values (6,' EveryoneBu',' Thisverybook ');
insert into t1 values (7,' SanFran',' Itisasanfranlifetyle ');
insert into t1 values (8,' BerkAuthor','  Cool.Berkly.the.book ');
insert into t1 values (9,null,null);
-- insert into t1 values (10,' 北京市 ',' 中关村 ');
insert into t1 values (10,'','');

-- @case
-- @desc:test for ltrim,rtrim
-- @label:bvt
-- @separator:table
select ltrim(c),ltrim(vc) from t1;
-- @separator:table
select rtrim(c),rtrim(vc) from t1;
-- @separator:table
select ltrim(rtrim(c)),rtrim(ltrim(vc)) from t1;
-- @separator:table
select * from t1 where ltrim(c) = 'BestSeller';
-- @separator:table
select * from t1 where ltrim(c) = 'BestSeller' and rtrim(vc) = 'OneHeckuvabook';


-- @case
-- @desc:test for space
-- @label:bvt
drop table if exists t1;
create table t1 ( d int);
insert into t1 values(0),(-1),(2),(10);
-- @separator:table
select space(d) from t1 where d <> -1;
-- @separator:table
select space(d) from t1;
drop table t1;
