
-- test coalesce function

drop table if exists t1;
create table t1(
a int,
b varchar(25)
);

insert into  t1 values (0, 'a');
insert into  t1 values (1, NULL);
insert into  t1 values (NULL, NULL);
insert into  t1 values (null, 'b');

select * from t1;
select coalesce(a, 1) from t1;
-- echo error
select coalesce(b, 1) from t1;

select coalesce(b, '1') from t1;

drop table t1;


drop table if exists t2;
create table t2(
a float,
b datetime
);

insert into t2 values (12.345, '2022-02-20 10:10:10.999999');
insert into t2 values (3.45646, NULL);
insert into t2 values(NULL, '2023-04-03 22:10:29.999999');
insert into t2 values (NULL, NULL);

select * from t2;
select coalesce(a, 1.0) from t2;
select coalesce(a, 1) from t2;

select coalesce(b, 2022-01-01) from t2;
select coalesce(b, 2022) from t2;
select coalesce(b, 2) from t2;


select coalesce(b, '2022-10-01') from t2;
select coalesce(b, '2022-10-01 10:10:10.999999') from t2;

-- echo error
select coalesce(b, '2022') from t2;

-- echo error
select coalesce(b, '2022/10/01') from t2;
drop table t2;


drop table if exists t3;
create table t3(
a bool,
b text
);

insert into t3 values (0, 'a');
insert into t3 values (1, 'b');
insert into t3 values (NULL, 'c');
insert into t3 values (TRUE, NULL);
insert into t3 values (NULL, NULL);

select * from t3;

select coalesce(a, 1) from t3;
select coalesce(a, 0) from t3;
-- echo error
select coalesce(a, 200) from t3;

select coalesce(b, '1') from t3;
select coalesce(b, 'bull') from t3;

drop table t3;


-- test create view
drop table if exists t4;
create table t4 (f1 date, f2 datetime, f3 varchar(20));

drop view if exists view_t1;
create view view_t1 as select coalesce(f1,f1) as f4 from t4;
desc view_t1;
drop view view_t1;

drop view if exists view_t2;
create view view_t2 as select coalesce(f1,f2) as f4 from t4;
desc view_t2;
drop view view_t2;


drop view if exists view_t3;
create view view_t3 as select coalesce(f2,f2) as f4 from t4;
desc view_t3;
drop view view_t3;

drop view if exists view_t4;
create view view_t4 as select coalesce(f1,f3) as f4 from t4;
desc view_t4;
drop view view_t4;

drop view if exists view_t5;
create view view_t5 as select coalesce(f2,f3) as f4 from t4;
desc view_t5;
drop view view_t5;

drop table t4;



drop table if exists t5;
CREATE TABLE t5 (b datetime);

INSERT INTO t5 VALUES ('2010-01-01 00:00:00'), ('2010-01-01 00:00:00');
SELECT * FROM t5 WHERE b <= coalesce(NULL, now());

DROP TABLE t5;
