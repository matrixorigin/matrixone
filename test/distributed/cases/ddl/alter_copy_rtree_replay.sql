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

-- Review 6094583688: a column comment quoting the SHOW CREATE display fragment
-- must not divert the replay rewrite. The copy rewrites the actual SPATIAL KEY
-- declaration to the executable KEY USING RTREE spelling while the comment
-- bytes, the index definition and the rows survive, and the spatial indexed
-- predicate is still served after the copy.
create table c_collide(id int primary key, g point not null, note varchar(64) default null comment '  SPATIAL KEY `idx_g` (`g`)', key idx_g using rtree(g));
insert into c_collide values (1, ST_GeomFromText('POINT(5 6)'), 'keep');
alter table c_collide algorithm=copy, add column lead int default 7 first;
select id, lead, note, ST_AsText(g) from c_collide;
show create table c_collide;
select id from c_collide where ST_Intersects(g, ST_GeomFromText('POINT(5 6)'));

drop database if exists alter_copy_rtree_replay;

-- Persistent rows, geometry and index identity after COPY in a fresh connection
-- are asserted by the prior selects inside one transactionally clean suite run.
