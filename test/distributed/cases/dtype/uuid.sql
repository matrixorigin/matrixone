-- test DDL with uuid type
create table t1(a int, b uuid);
desc t1;
show create table t1;
drop table t1;
-- test uuid type as primary key
create table t2(
a uuid primary key,
b int,
c varchar(20),
d date
) COMMENT='test uuid parimary key';
show create table t2;
-- test insert into statement with uuid type
INSERT INTO t2 VALUES ("6d1b1f73-2dbf-11ed-940f-000c29847904",12,'SMITH','1980-12-17');
INSERT INTO t2 VALUES ("ad9f809f-2dbd-11ed-940f-000c29847904",34,'ALLEN','1981-02-20');
INSERT INTO t2 VALUES ("1b50c137-2dba-11ed-940f-000c29847904",15,'WARD','1981-02-22');
INSERT INTO t2 VALUES ("149e3f0f-2de4-11ed-940f-000c29847904",27,'JONES','1981-04-02');
select * from t2 where a = '6d1b1f732dbf11ed940f000c29847904';
select * from t2 where a != '6d1b1f732dbf11ed940f000c29847904';
select * from t2 where a > '6d1b1f732dbf11ed940f000c29847904';
select * from t2 where a >= '6d1b1f732dbf11ed940f000c29847904';
select * from t2 where a <= '6d1b1f732dbf11ed940f000c29847904';
select * from t2 where a < '6d1b1f732dbf11ed940f000c29847904';
select * from t2 order by a;
drop table t2;
-- uuid type in DQL
create table t3(a int, b uuid);
insert into t3 values(10, "f6355110-2d0c-11ed-940f-000c29847904");
insert into t3 values(20, "1ef96142-2d0d-11ed-940f-000c29847904");
insert into t3 values(30, "117a0bd5-2d0d-11ed-940f-000c29847904");
insert into t3 values(40, "18b21c70-2d0d-11ed-940f-000c29847904");
insert into t3 values(50, "1b50c129-2dba-11ed-940f-000c29847904");
insert into t3 values(60, "ad9f83eb-2dbd-11ed-940f-000c29847904");
insert into t3 values(70, "6d1b1fdb-2dbf-11ed-940f-000c29847904");
insert into t3 values(80, "6d1b1fdb-2dbf-11ed-940f-000c29847904");
insert into t3 values(90, "1b50c129-2dba-11ed-940f-000c29847904");

-- uuid type field as filter condition
select * from t3;
select a,b from t3 where b =  "18b21c70-2d0d-11ed-940f-000c29847904";
select a,b from t3 where b != "18b21c70-2d0d-11ed-940f-000c29847904";
select a,b from t3 where b >  "f6355110-2d0c-11ed-940f-000c29847904";
select a,b from t3 where b >= "f6355110-2d0c-11ed-940f-000c29847904";
select a,b from t3 where b <  "117a0bd5-2d0d-11ed-940f-000c29847904";
select a,b from t3 where b <= "117a0bd5-2d0d-11ed-940f-000c29847904";

-- The uuid type field is used as the parameter of the aggregate function
select min(b) from t3;
select max(b) from t3;

-- uuid type field as grouping condition
select count(*) from t3 group by b;
select count(b) from t3 group by b;
select sum(a) from t3 group by b;
select sum(a) from t3 group by b having by sum(a) > 20;
-- uuid  type field as sorting criteria when order by
select * from t3 order by b;
select * from t3 order by b desc;
-- test delete statement with uuid type
delete from t3 where b = 'ad9f83eb-2dbd-11ed-940f-000c29847904';
select * from t3;
-- test update statement with uuid type
update t3 set b = 'e5c8314e-2ea7-11ed-8ec0-000c29847904' where a = 50;
select * from t3 where a = 50;
drop table t3;