#SELECT, 嵌套，中文
SELECT FIND_IN_SET('b','a,b,c,d');
select bin(concat_ws(" ",find_in_set('b','a,b,c,d')));
select find_in_set("b","a,b,c"),find_in_set("c","a,b,c"),find_in_set("dd","a,bbb,dd"),find_in_set("bbb","a,bbb,dd");
select find_in_set("d","a,b,c"),find_in_set("dd","a,bbb,d"),find_in_set("bb","a,bbb,dd");
select find_in_set("","a,b,c"),find_in_set("","a,b,c,"),find_in_set("",",a,b,c");
select find_in_set("abc","abc"),find_in_set("ab","abc"),find_in_set("abcd","abc");
select find_in_set('1','3,1,');

select find_in_set('你好', "你好,我好,大家好");



#NULL
select find_in_set(Null, null);
select find_in_set('1', null);
select find_in_set(null,"a,b,c,d");


#EXTREME VALUES
select find_in_set('a,', 'a,b,c,d');
select find_in_set("*#(()@*31()@*)#)_", "qwkrjqjiofj,*#(()@*31()@*)#)_,f023jf09j2");
select find_in_set('a', 'abcd');


#WHERE
create table t1 (a varchar(255));
insert into t1 values('1'),('-1'),('0'),("abc");
select * from t1 where find_in_set('-1', a);
drop table t1;




#ON, 比较操作
CREATE TABLE t1 (
access_id int NOT NULL default 0,
name varchar(20) default NULL,
`rank` int NOT NULL default 0,
PRIMARY KEY idx (access_id)
);
CREATE TABLE t2 (
faq_group_id int NOT NULL default 0,
faq_id int NOT NULL default 0,
access_id int default NULL,
PRIMARY KEY idx1 (faq_id)
);
INSERT INTO t1 VALUES (1,'Everyone',2),(2,'Help',3),(3,'Technical Support',1),(4,'Chat User',4);
INSERT INTO t2 VALUES (261,265,1),(490,494,1);
SELECT t2.faq_id FROM t1 INNER JOIN t2 ON (t1.access_id = t2.access_id) LEFT JOIN t2 t ON (t.faq_group_id = t2.faq_group_id AND find_in_set(t.access_id, '1,4') < find_in_set(t2.access_id, '1,4')) WHERE t2.access_id IN (1,4) AND t.access_id IS NULL AND t2.faq_id in (265);
drop table t1;
drop table t2;



#INSERT,distinct
CREATE TABLE t1(a char(255), b int);
INSERT INTO t1 select 'a,b,c,d', FIND_IN_SET('b','a,b,c,d');
INSERT INTO t1 select "a,bbb,dd", find_in_set("dd","a,bbb,dd");
INSERT INTO t1 select "a,b,c", find_in_set("c","a,b,c");
SELECT distinct find_in_set('a', a) FROM t1;
drop table t1;

#HAVING, 比较运算
CREATE TABLE t1 (a varchar(10));
INSERT INTO t1 VALUES ('abc'), ('xyz');
SELECT a, CONCAT_WS(",",a,' ',a) AS c FROM t1
HAVING find_in_set('a', c) =0;
DROP TABLE t1;



#算式操作
select find_in_set("","a,b,c")*find_in_set("","a,b,c,")-find_in_set("bb","a,bbb,dd");

#WITH RECURSIVE AS 0.5暂不支持
#create table nodes(id int);
#create table arcs(from_id int, to_id int);
#insert into nodes values(1),(2),(3),(4),(5),(6),(7),(8);
#insert into arcs values(1,3), (3,6), (1,4), (4,6), (6,2), (2,1);
#with recursive cte as
#(
#select id, cast(id as char(200)) as path, 0 as is_cycle
#from nodes where id=1
#union all
#select to_id, concat(cte.path, ",", to_id), find_in_set(to_id, path)
#from arcs, cte
#where from_id=cte.id and is_cycle=0
#)
#select * from cte;
#drop table nodes, arcs;


