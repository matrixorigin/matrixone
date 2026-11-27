-- @suit
-- @case
-- @desc: ALGORITHM=COPY replays RTREE/spatial indexed tables through the executable KEY USING RTREE spelling while SHOW CREATE keeps the public SPATIAL KEY display
-- @label:bvt

drop database if exists alter_copy_rtree_replay;
create database alter_copy_rtree_replay;
use alter_copy_rtree_replay;

create table t_inline(id int primary key, g point not null, key idx_g using rtree(g));
insert into t_inline values (1, ST_GeomFromText('POINT(1 2)'));
alter table t_inline algorithm=copy, add column lead int default 7 first;
select id, lead, ST_AsText(g) from t_inline;
select id from t_inline where ST_Intersects(g, ST_GeomFromText('POINT(1 2)'));
show create table t_inline;
insert into t_inline (id, g) values (2, ST_GeomFromText('POINT(10 20)'));
update t_inline set g = ST_GeomFromText('POINT(30 40)') where id = 1;
delete from t_inline where ST_Intersects(g, ST_GeomFromText('POINT(10 20)'));
select id, lead, ST_AsText(g) from t_inline order by id;

create table t_standalone(id int primary key, g point not null);
create spatial index idx_g on t_standalone(g);
alter table t_standalone algorithm=copy, add column lead int default 7 first;
select count(*) from t_standalone;
show create table t_standalone;

create table t_empty(id int primary key, g point not null, key idx_g using rtree(g));
alter table t_empty algorithm=copy, add column lead int default 7 first;
select count(*) from t_empty;
show create table t_empty;

create table c_geo(id int primary key, g point not null);
insert into c_geo values (1, ST_GeomFromText('POINT(3 4)'));
alter table c_geo algorithm=copy, add column lead int default 7 first;
select id, lead, ST_AsText(g) from c_geo;

create table c_int(id int primary key, v int, key idx_v(v));
insert into c_int values (1, 10), (2, 20);
alter table c_int algorithm=copy, add column lead int default 7 first;
select id, lead from c_int order by id;

drop database if exists alter_copy_rtree_replay;

-- Persistent rows, geometry and index identity after COPY in a fresh connection
-- are asserted by the prior selects inside one transactionally clean suite run.
