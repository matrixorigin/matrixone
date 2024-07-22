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
select * from t1 where a = "Congress" and b="Lane";

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
select * from t1 where a = "Alberta" and b="Blvd";


-- 2.1.b Insert Duplicates
insert into t1 values("Nightingale","Lane", "5");
select * from t1 where a = "Nightingale" and b="Lane";

-- 2.1.c Insert Nulls
insert into t1 values(NULL,"Lane", "6");
select * from t1 where b="Lane";

-- 2.1.d Insert Into Select *
drop table if exists t2;
create table t2(a varchar(30), b varchar(30), c varchar(30));
insert into t2 values("arjun", "sk", "7");
insert into t2 values("albin", "john", "8");
insert into t1 select * from t2;
select * from t1 where b="Lane";

-- 2.2.a Update a record to duplicate
update t1 set a="albin" ,b="john" where c="7";
select * from t1 where a="albin";

-- 2.2.b Update a record to NULL
update t1 set a=NULL ,b="john" where c="7";
select * from t1 where b="john";

-- 2.2.c Delete a record
delete from t1 where c="7";
select * from t1 where a="john";

-- 2.2.d truncate
truncate table t1;
select * from t1;

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
select * from t1 where a = "Abi";

-- 2.3.b Create Index on multiple columns (>3)
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
create index idx1 using master on t1(a,b,c);
insert into t1 values("Abel","John", "4");
insert into t1 values("Amy","Brian", "5");
select * from t1 where a = "Congress" and b="Lane" and c="1";
-- TODO: Fix this

-- 2.3.c Create Index before table population
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
select * from t1 where a = "Congress" and b="Lane";

-- 2.3.e Create Index using `create table syntax`
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key, index idx1 using master (a,b));
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
select * from t1 where a = "Congress" and b="Lane";

-- 2.3.f Create Index using `alter table syntax`
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 add index idx1 using master(a,b);
insert into t1 values("Congress","Lane", "4");
select * from t1 where a = "Congress" and b="Lane";

-- 2.4.a No PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30));
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
select * from t1 where a="Congress" and b="Lane";

-- 2.4.c Composite PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30), primary key(a,b));
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
select * from t1 where a="Congress" and b="Lane";

-- 2.5.b Drop column
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a,b);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 drop column b;
insert into t1 values("Congress", "4");
select * from t1 where a="Congress";

-- 2.5.c Rename column
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 rename column a to a1;
insert into t1 values("Congress","Lane", "4");
select * from t1 where a1="Congress";

-- 2.5.d Change column type
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 modify column c int;
insert into t1 values("Congress","Lane", 4);
select * from t1 where a="Congress";

-- 2.5.e Add PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
alter table t1 drop primary key;
alter table t1 add primary key (a,b);
insert into t1 values("Congress","Lane2", "4");
select * from t1 where a="Congress";

-- 2.5.f Drop PK
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30) primary key);
create index idx1 using master on t1(a);
insert into t1 values("Congress","Lane", "4");
insert into t1 values("Juniper","Way", "5");
insert into t1 values("Nightingale","Lane", "6");
alter table t1 drop primary key;
insert into t1 values("Congress","Lane", "7");
select * from t1 where a="Congress";

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
--|         Join Type: INDEX                                                                    |
--|         Join Cond: (t1.__mo_fake_pk_col = #[1,0])                                           | <-- Good
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b = 'Lane'), (t1.a = 'Congress')                             |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df437-c576-7c78-8d68-eb29bf7cd598 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FCongress ')                       |
--|               ->  Table Scan on a.__mo_index_secondary_018df437-c576-7c78-8d68-eb29bf7cd598 |
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
--|         Join Type: INDEX                                                                    |
--|         Join Cond: (t1.c = #[1,0])                                                          |<-- Good
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b = 'Lane'), (t1.a = 'Nightingale')                          |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df438-9530-7b1d-b252-b10d794ae2a4 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FNightingale ')                    |
--|               ->  Table Scan on a.__mo_index_secondary_018df438-9530-7b1d-b252-b10d794ae2a4 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                           |
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
--|         Join Type: INDEX                                                                    |
--|         Join Cond: (t1.__mo_cpkey_col = #[1,0])                                             |<-- Good
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b0 = 'Lane'), (t1.a = 'Nightingale')                         |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df43c-db6a-7afe-bb23-bdf898223435 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FNightingale ')                    |
--|               ->  Table Scan on a.__mo_index_secondary_018df43c-db6a-7afe-bb23-bdf898223435 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fb0 FLane ')                          |
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
--|         Join Type: INDEX                                                              |
--|         Join Cond: (t1.c = #[1,0])                                                    |<-- Good
--|         ->  Table Scan on a.t1                                                        |
--|               Filter Cond: (t1.b = 'Lane')                                            |
--|         ->  Table Scan on a.__mo_index_secondary_018df43d-47dd-75bd-a6c4-9c25c7a51c23 |
--|               Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                           |
--+---------------------------------------------------------------------------------------+
select * from t1 where b="Lane";

-- 2.8.b Select with 2 Filters
--mysql> explain select * from t1 where a="Juniper" and b="Way";
--+---------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                  |
--+---------------------------------------------------------------------------------------------+
--| Project                                                                                     |
--|   ->  Join                                                                                  |
--|         Join Type: INDEX                                                                    |
--|         Join Cond: (t1.c = #[1,0])                                                          |
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.b = 'Way'), (t1.a = 'Juniper')                               |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |<-- Good
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df43d-47dd-75bd-a6c4-9c25c7a51c23 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FJuniper ')                        |
--|               ->  Table Scan on a.__mo_index_secondary_018df43d-47dd-75bd-a6c4-9c25c7a51c23 |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fb FWay ')                            |
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
--|         Join Type: INDEX                                                                          |
--|         Join Cond: (t1.__mo_fake_pk_col = #[1,0])                                                 |
--|         ->  Table Scan on a.t1                                                                    |
--|               Filter Cond: (t1.c = '1'), (t1.b = 'Lane'), (t1.a = 'Congress')                     |
--|         ->  Join                                                                                  |
--|               Join Type: INNER                                                                    |<-- Good
--|               Join Cond: (#[0,0] = #[1,0])                                                        |
--|               ->  Table Scan on a.__mo_index_secondary_018df43e-105b-70d8-a9c1-88c03b26d8ee       |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FCongress ')                             |
--|               ->  Join                                                                            |
--|                     Join Type: INNER                                                              |<-- Good
--|                     Join Cond: (#[0,0] = #[1,0])                                                  |
--|                     ->  Table Scan on a.__mo_index_secondary_018df43e-105b-70d8-a9c1-88c03b26d8ee |
--|                           Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                           |
--|                     ->  Table Scan on a.__mo_index_secondary_018df43e-105b-70d8-a9c1-88c03b26d8ee |
--|                           Filter Cond: prefix_eq(#[0,0], 'Fc F1 ')                              |
--+---------------------------------------------------------------------------------------------------+
select * from t1 where a="Congress" and b="Lane" and c="1";


-- 2.8.d Select with = and between
--mysql> explain select * from t1 where a="Nightingale" and c between "2" and "3";
--+---------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                  |
--+---------------------------------------------------------------------------------------------+
--| Project                                                                                     |
--|   ->  Join                                                                                  |
--|         Join Type: INDEX                                                                    |
--|         Join Cond: (t1.__mo_fake_pk_col = #[1,0])                                           |
--|         ->  Table Scan on a.t1                                                              |
--|               Filter Cond: (t1.a = 'Nightingale'), t1.c BETWEEN '2' AND '3'                 |
--|         ->  Join                                                                            |
--|               Join Type: INNER                                                              |<-- Good
--|               Join Cond: (#[0,0] = #[1,0])                                                  |
--|               ->  Table Scan on a.__mo_index_secondary_018df43e-105b-70d8-a9c1-88c03b26d8ee |
--|                     Filter Cond: prefix_between(#[0,0], 'Fc F2 ', 'Fc F3 ')                 |<-- Good
--|               ->  Table Scan on a.__mo_index_secondary_018df43e-105b-70d8-a9c1-88c03b26d8ee |
--|                     Filter Cond: prefix_eq(#[0,0], 'Fa FNightingale ')                      |<-- Good
--+---------------------------------------------------------------------------------------------+
select * from t1 where a="Nightingale" and c between "2" and "3";

-- 2.8.e Select with = and in
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30));
create index idx1 using master on t1(a,b,c);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
--mysql> explain analyze select * from t1 where a in ("Congress","Nightingale") and b="Lane" and c in("1","2","3");
--+-----------------------------------------------------------------------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                                                                                |
--+-----------------------------------------------------------------------------------------------------------------------------------------------------------+
--| Project                                                                                                                                                   |
--|   Analyze: timeConsumed=0ms waitTime=0ms inputRows=2 outputRows=2 InputSize=144bytes OutputSize=144bytes MemorySize=144bytes                              |
--|   ->  Join                                                                                                                                                |
--|         Analyze: timeConsumed=0ms waitTime=7ms inputRows=4 outputRows=2 InputSize=176bytes OutputSize=144bytes MemorySize=16bytes                         |
--|         Join Type: INDEX                                                                                                                                  |
--|         Join Cond: (t1.__mo_fake_pk_col = #[1,0])                                                                                                         |
--|         Runtime Filter Build: #[-1,0]                                                                                                                     |
--|         ->  Table Scan on a.t1                                                                                                                            |
--|               Analyze: timeConsumed=0ms waitTime=0ms inputRows=3 outputRows=2 InputSize=240bytes OutputSize=160bytes MemorySize=409bytes                  |
--|               Filter Cond: (t1.b = 'Lane'), t1.c in ([1 2 3]), t1.a in ([Congress Nightingale])                                                           |
--|               Block Filter Cond: t1.__mo_fake_pk_col in ([1 3])                                                                                           |
--|               Runtime Filter Probe: t1.__mo_fake_pk_col                                                                                                   |
--|         ->  Join                                                          [GOOD]                                                                                 |
--|               Analyze: timeConsumed=0ms waitTime=0ms inputRows=2 outputRows=2 InputSize=16bytes OutputSize=16bytes MemorySize=32898bytes                  |
--|               Join Type: INNER                                                                                                                            |
--|               Join Cond: (#[0,0] = #[1,0])                                                                                                                |
--|               ->  Project                                                                                                                                 |
--|                     Analyze: timeConsumed=0ms waitTime=0ms inputRows=2 outputRows=2 InputSize=16bytes OutputSize=16bytes MemorySize=16bytes               |
--|                     ->  Table Scan on a.__mo_index_secondary_018e1cf0-f06c-7d3a-9000-bb0adee77acc                                                         |
--|                           Analyze: timeConsumed=0ms waitTime=0ms inputRows=9 outputRows=2 InputSize=288bytes OutputSize=16bytes MemorySize=313bytes       |
--|                           Filter Cond: prefix_in(#[0,0], [Fa FCongress  Fa FNightingale ])                                                            |
--|               ->  Join                                                                                                                                    |
--|                     Analyze: timeConsumed=0ms waitTime=0ms inputRows=2 outputRows=2 InputSize=16bytes OutputSize=16bytes MemorySize=32898bytes            |
--|                     Join Type: INNER                                                                                                                      |
--|                     Join Cond: (#[0,0] = #[1,0])                                                                                                          |
--|                     ->  Project                                            [GOOD]                                                                               |
--|                           Analyze: timeConsumed=0ms waitTime=0ms inputRows=3 outputRows=3 InputSize=24bytes OutputSize=24bytes MemorySize=24bytes         |
--|                           ->  Table Scan on a.__mo_index_secondary_018e1cf0-f06c-7d3a-9000-bb0adee77acc                                                   |
--|                                 Analyze: timeConsumed=0ms waitTime=0ms inputRows=9 outputRows=3 InputSize=288bytes OutputSize=24bytes MemorySize=321bytes |
--|                                 Filter Cond: prefix_in(#[0,0], [Fc F1  Fc F2  Fc F3 ]) [GOOD]                                                              |
--|                     ->  Project                                                                                                                           |
--|                           Analyze: timeConsumed=0ms waitTime=0ms inputRows=2 outputRows=2 InputSize=16bytes OutputSize=16bytes MemorySize=16bytes         |
--|                           ->  Table Scan on a.__mo_index_secondary_018e1cf0-f06c-7d3a-9000-bb0adee77acc                                                   |
--|                                 Analyze: timeConsumed=0ms waitTime=0ms inputRows=9 outputRows=2 InputSize=288bytes OutputSize=16bytes MemorySize=313bytes |
--|                                 Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                                                                             |
--+-----------------------------------------------------------------------------------------------------------------------------------------------------------+
--35 rows in set (0.01 sec)
select * from t1 where a in ("Congress","Nightingale") and b="Lane" and c in("1","2","3");

-- 2.8.f SELECT with LIMIT
drop table if exists t1;
create table t1(a varchar(30), b varchar(30), c varchar(30));
create index idx1 using master on t1(a,b,c);
insert into t1 values("Congress","Lane", "1");
insert into t1 values("Juniper","Way", "2");
insert into t1 values("Nightingale","Lane", "3");
select * from t1 where a between "Congress" and "Nightingale" and b="Lane" and c between "1" and "3";
select * from t1 where a between "Congress" and "Nightingale" and b="Lane" and c between "1" and "3" limit 1;
--mysql> explain analyze select * from t1 where a between "Congress" and "Nightingale" and b="Lane" and c between "1" and "3" limit 1;
--+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
--| QUERY PLAN                                                                                                                                                                                                    |
--+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
--| Project                                                                                                                                                                                                       |
--|   Analyze: timeConsumed=0ms waitTime=1ms inputRows=1 outputRows=1 InputSize=72bytes OutputSize=72bytes MemorySize=72bytes                                                                                     |
--|   ->  Join                                                                                                                                                                                                    |
--|         Analyze: timeConsumed=0ms waitTime=2ms inputRows=1 outputRows=1 InputSize=8bytes OutputSize=72bytes MemorySize=8bytes                                                                                 |
--|         Join Type: INDEX                                                                                                                                                                                      |
--|         Join Cond: (t1.__mo_fake_pk_col = #[1,0])                                                                                                                                                             |
--|         Runtime Filter Build: #[-1,0]                                                                                                                                                                         |
--|         ->  Table Scan on a.t1                                                                                                                                                                                |
--|               Analyze: timeConsumed=0ms waitTime=0ms inputRows=1 outputRows=1 InputSize=80bytes OutputSize=80bytes MemorySize=164bytes                                                                        |
--|               Filter Cond: (t1.b = 'Lane'), t1.c BETWEEN '1' AND '3', t1.a BETWEEN 'Congress' AND 'Nightingale'                                                                                               |
--|               Block Filter Cond: t1.__mo_fake_pk_col in (1)                                                                                                                                                   |
--|               Runtime Filter Probe: t1.__mo_fake_pk_col                                                                                                                                                       |
--|         ->  Join                                                                                                                                                                                              |
--|               Analyze: timeConsumed=0ms probe_time=[total=0ms,min=0ms,max=0ms,dop=10] build_time=[0ms] waitTime=8ms inputRows=2 outputRows=1 InputSize=16bytes OutputSize=8bytes MemorySize=180859bytes       |
--|               Join Type: INNER                                                                                                                                                                                |
--|               Join Cond: (#[0,0] = #[1,0])                                                                                                                                                                    |
--|               ->  Project                                                    [GOOD]                                                                                                                                      |
--|                     Analyze: timeConsumed=0ms waitTime=0ms inputRows=1 outputRows=1 InputSize=8bytes OutputSize=8bytes MemorySize=8bytes                                                                      |
--|                     ->  Table Scan on a.__mo_index_secondary_018e1ced-b355-7509-a021-b417bd3bd535                                                                                                             |
--|                           Analyze: timeConsumed=0ms waitTime=0ms inputRows=9 outputRows=1 InputSize=288bytes OutputSize=8bytes MemorySize=321bytes                                                            |
--|                           Filter Cond: prefix_between(#[0,0], 'Fa FCongress ', 'Fa FNightingale ')                                                                                                        |
--|                           Limit: 1                                                                                                                                                                            |
--|               ->  Join                                                                                                                                                                                        |
--|                     Analyze: timeConsumed=0ms probe_time=[total=0ms,min=0ms,max=0ms,dop=10] build_time=[0ms] waitTime=4ms inputRows=2 outputRows=1 InputSize=16bytes OutputSize=8bytes MemorySize=180859bytes |
--|                     Join Type: INNER                                                                                                                                                                          |
--|                     Join Cond: (#[0,0] = #[1,0])                                                                                                                                                              |
--|                     ->  Project                                                  [GOOD]                                                                                                                             |
--|                           Analyze: timeConsumed=0ms waitTime=0ms inputRows=1 outputRows=1 InputSize=8bytes OutputSize=8bytes MemorySize=8bytes                                                                |
--|                           ->  Table Scan on a.__mo_index_secondary_018e1ced-b355-7509-a021-b417bd3bd535                                                                                                       |
--|                                 Analyze: timeConsumed=0ms waitTime=0ms inputRows=9 outputRows=1 InputSize=288bytes OutputSize=8bytes MemorySize=321bytes                                                      |
--|                                 Filter Cond: prefix_between(#[0,0], 'Fc F1 ', 'Fc F3 ')                                                                                                                   |
--|                                 Limit: 1       [GOOD]                                                                                                                                                                    |
--|                     ->  Project                                                [GOOD]                                                                                                                                    |
--|                           Analyze: timeConsumed=0ms waitTime=0ms inputRows=1 outputRows=1 InputSize=8bytes OutputSize=8bytes MemorySize=8bytes                                                                |
--|                           ->  Table Scan on a.__mo_index_secondary_018e1ced-b355-7509-a021-b417bd3bd535                                                                                                       |
--|                                 Analyze: timeConsumed=0ms waitTime=0ms inputRows=9 outputRows=1 InputSize=288bytes OutputSize=8bytes MemorySize=313bytes                                                      |
--|                                 Filter Cond: prefix_eq(#[0,0], 'Fb FLane ')                                                                                                                                 |
--|                                 Limit: 1       [GOOD]                                                                                                                                                                    |
--+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
--38 rows in set (0.01 sec)

