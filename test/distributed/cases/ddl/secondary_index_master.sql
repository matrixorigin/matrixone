-- 0.  insert, update, delete
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
select * from t1;
create index idx1 using master on t1(a,b);
insert into t1 values("Changing","Expanse", "4");
update t1 set a = "Altering" where c = "4";
delete from t1 where c = "2";

-- 1. failure on create index on non strings.
create table t2(a varchar(30), b bigint, c varchar(30) primary key);
insert into t2 values("Congress",1, "1");
insert into t2 values("Juniper",2, "2");
insert into t2 values("Nightingale",3, "3");
create index idx2 using master on t2(a,b);

-- 2.1.a Insert Normal (from Test Document)
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
create index idx1 using master on t1(a,b);
insert into t1 values("Alberta","Blvd", "4");


-- 2.1.b Insert Duplicates
insert into t1 values("Nightingale","Lane", "5");

-- 2.1.c Insert Nulls
insert into t1 values(NULL,"Lane", "6");

-- 2.1.d Insert Into Select *
drop table if exists t2;
create table t2(a varchar(30), b varchar(30), c varchar(30));
insert into t2 values("arjun", "sk", "7");
insert into t2 values("albin", "john", "8");
insert into t1 select * from t2;

-- 2.2.a Update a record to duplicate
update t1 set a="albin" ,b="john" where c="7";

-- 2.2.b Update a record to NULL
update t1 set a=NULL ,b="john" where c="7";

-- 2.2.c Delete a record
delete from t1 where c="7";

-- 2.2.d truncate
truncate table t1;

-- 2.2.e drop
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";
drop table t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";

-- 2.3.a Create Index on a single column
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
create index idx1 using master on t1(a);
insert into t1 values("Abi","Ma", "4");

-- 2.3.b Create Index on multiple columns (>3)
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
create index idx1 using master on t1(a,b,c);
insert into t1 values("Abel","John", "4");
insert into t1 values("Amy","Brian", "5");
-- TODO: Fix this

-- 2.3.c Create Index before table population
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");

-- 2.3.e Create Index using `create table syntax`
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key, index idx1 using master (a,b));
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");

-- 2.3.f Create Index using `alter table syntax`
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 add index idx1 using master(a,b);

-- 2.4.a No PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30));
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");

-- 2.4.c Composite PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30), primary key(a,b));
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");

-- 2.5.b Drop column
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 drop column b;

-- 2.5.c Rename column
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 rename column a to a1;

-- 2.5.d Change column type
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 modify column a int;

-- 2.5.e Add PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 drop primary key;
alter table t1 add primary key (a,b);

-- 2.5.f Drop PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "4");
insert into t1 values("Juniper","Way", "5");
insert into t1 values("Nightingale","Lane", "6");
alter table t1 drop primary key;

-- 2.6.a Non Varchar column
drop table if exists t1;
create table t1(a varchar(30), b bigint, c varchar(30) primary key);
create index idx1 using master on t1(a,b);



-- 2.7.a Select with No PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30));
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
--explain select * from t1 where a="Congress" and b="Lane";
--+---------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                  |
--+---------------------------------------------------------------------------------------------+
--| Project                                                                                     |
--|   ->  Join                                                                                  |
--|         Join Type: INNER                                                                    |
--|         Join Cond: (t1.__mo_fake_pk_col = #[1,0])                                           | <-- Good
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b = 'Lane'), (t1.a = 'Congress')                             |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df358-07e9-7ba7-98ad-b0925c730588 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FCongress ')                       |
--|               ->  Table Scan on a.__mo_index_secondary_018df358-07e9-7ba7-98ad-b0925c730588 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                           |
--+---------------------------------------------------------------------------------------------+
select * from t1 where a="Congress" and b="Lane";

-- 2.7.b Select with Single PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
--mysql> explain select * from t1 where a="Nightingale" and b="Lane";
--+---------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                  |
--+---------------------------------------------------------------------------------------------+
--| Project                                                                                     |
--|   ->  Join                                                                                  |
--|         Join Type: INNER   hashOnPK                                                         |
--|         Join Cond: (#[0,0] = t1.c)                                                          |<-- Good
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df3d0-af04-76e6-9720-4dee6d627ecc |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FNightingale ')                    |
--|               ->  Table Scan on a.__mo_index_secondary_018df3d0-af04-76e6-9720-4dee6d627ecc |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                           |
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b = 'Lane'), (t1.a = 'Nightingale')                          |
--+---------------------------------------------------------------------------------------------+
select * from t1 where a="Nightingale" and b="Lane";


-- 2.7.c Select with 2 or more PK
drop table if exists t1;
create table t1(a varchar(30), b0 varchar(30), b1 varchar(30), c varchar(30), d varchar(30), primary key( c, d));
create index idx1 using master on t1(a,b0);
insert into t1 values("Congress","Lane", "ALane","1","0");
insert into t1 values("Juniper","Way","AWay", "2","0");
insert into t1 values("Nightingale","Lane","ALane", "3","0");
--mysql> explain select * from t1 where a="Nightingale" and b0="Lane";
--+---------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                  |
--+---------------------------------------------------------------------------------------------+
--| Project                                                                                     |
--|   ->  Join                                                                                  |
--|         Join Type: INNER                                                                    |
--|         Join Cond: (#[0,0] = t1.__mo_cpkey_col)                                             | <-- Good
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df3cf-e454-7b47-84d3-ac0241fde645 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FNightingale ')                    |
--|               ->  Table Scan on a.__mo_index_secondary_018df3cf-e454-7b47-84d3-ac0241fde645 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fb0 FLane ')                          |
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b0 = 'Lane'), (t1.a = 'Nightingale')                         |
--+---------------------------------------------------------------------------------------------+
select * from t1 where a="Nightingale" and b0="Lane";

-- 2.8.a Select with one Filter
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
--mysql> explain  select * from t1 where b="Lane";
--+---------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                            |
--+---------------------------------------------------------------------------------------+
--| Project                                                                               |
--|   ->  Join                                                                            |
--|         Join Type: INNER   hashOnPK                                                   |
--|         Join Cond: (#[0,0] = t1.c)                                                    |
--|         ->  Table Scan on a.__mo_index_secondary_018df3d4-9f51-7036-a244-e41c4a6d7b12 |
--|               Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                           |
--|         ->  Table Scan on a.t1                                                        |
--|               Filter Cond: (t1.b = 'Lane')                                            |
--+---------------------------------------------------------------------------------------+
select * from t1 where b="Lane";

-- 2.8.b Select with 2 Filters
--mysql> explain select * from t1 where a="Juniper" and b="Way";
--+---------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                  |
--+---------------------------------------------------------------------------------------------+
--| Project                                                                                     |
--|   ->  Join                                                                                  |
--|         Join Type: INNER   hashOnPK                                                         |
--|         Join Cond: (#[0,0] = t1.c)                                                          |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df3d4-9f51-7036-a244-e41c4a6d7b12 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FJuniper ')                        |
--|               ->  Table Scan on a.__mo_index_secondary_018df3d4-9f51-7036-a244-e41c4a6d7b12 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fb FWay ')                            |
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b = 'Way'), (t1.a = 'Juniper')                               |
--+---------------------------------------------------------------------------------------------+
select * from t1 where a="Juniper" and b="Way";

-- 2.8.c Select with 3 or more Filters
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30));
create index idx1 using master on t1(a,b,c);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
--mysql> explain select * from t1 where a="Congress" and b="Lane" and c="1";
--+---------------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                        |
--+---------------------------------------------------------------------------------------------------+
--| Project                                                                                           |
--|   ->  Join                                                                                        |
--|         Join Type: INNER   hashOnPK                                                               |
--|         Join Cond: (#[0,0] = t1.__mo_fake_pk_col)                                                 |
--|         ->  Join                                                                                  |
--|               Join Type: INNER                                                                    |
--|               Join Cond: (#[0,0] = #[1,0])                                                        |
--|               ->  Table Scan on a.__mo_index_secondary_018df3d6-3662-7f6d-b1e6-68e7413c5c7d       |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FCongress ')                             |
--|               ->  Join                                                                            |
--|                     Join Type: INNER                                                              |
--|                     Join Cond: (#[0,0] = #[1,0])                                                  |
--|                     ->  Table Scan on a.__mo_index_secondary_018df3d6-3662-7f6d-b1e6-68e7413c5c7d |
--|                           Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                           |
--|                     ->  Table Scan on a.__mo_index_secondary_018df3d6-3662-7f6d-b1e6-68e7413c5c7d |
--|                           Filter Cond: prefix_eq(#[0,0], 'Fc F1 ')                              |
--|         ->  Table Scan on a.t1                                                                    |
--|               Filter Cond: (t1.c = '1'), (t1.b = 'Lane'), (t1.a = 'Congress')                     |
--+---------------------------------------------------------------------------------------------------+
select * from t1 where a="Congress" and b="Lane" and c="1";


-- 2.8.d Select with = and between
--mysql> explain select * from t1 where a="Nightingale" and c between "2" and "3";
--+---------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                  |
--+---------------------------------------------------------------------------------------------+
--| Project                                                                                     |
--|   ->  Join                                                                                  |
--|         Join Type: INNER   hashOnPK                                                         |
--|         Join Cond: (#[0,0] = t1.__mo_fake_pk_col)                                           |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df3d6-3662-7f6d-b1e6-68e7413c5c7d |
--|                     Filter Cond: prefix_between(#[0,0], 'Fc F2 ', 'Fc F3 ')             |
--|               ->  Table Scan on a.__mo_index_secondary_018df3d6-3662-7f6d-b1e6-68e7413c5c7d |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FNightingale ')                    |
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.a = 'Nightingale'), t1.c BETWEEN '2' AND '3'                 |
--+---------------------------------------------------------------------------------------------+
select * from t1 where a="Nightingale" and c between "2" and "3";