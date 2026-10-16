-- @suite
-- @case
-- @desc: Scalar lookups in INSERT VALUES use the existing relational source path.
-- @label:bvt
--- @metacmp(true)
drop database if exists insert_values_subquery;
drop user if exists values_lookup_user;
drop role if exists values_lookup_role;
create database insert_values_subquery;
use insert_values_subquery;
create table `user`(id int primary key,`user` varchar(64) unique);
insert into `user` values(1,'alice'),(2,'bob');
create table user_access(user_id int not null,topic varchar(255),`read` bool,`write` bool,owner_user_id int null,provisioned bool,primary key(user_id,topic));

-- Exact #29902 input, then a duplicate carrying a nonempty owner lookup.
insert into user_access(user_id,topic,`read`,`write`,owner_user_id,provisioned)
values((select id from `user` where `user`='alice'),'owned_topic',true,true,(select if(''='',null,(select id from `user` where `user`=''))),false)
on duplicate key update `read`=values(`read`),`write`=values(`write`),owner_user_id=values(owner_user_id),provisioned=values(provisioned);
select * from user_access order by user_id,topic;
insert into user_access(user_id,topic,`read`,`write`,owner_user_id,provisioned)
values((select id from `user` where `user`='alice'),'owned_topic',false,true,(select if('bob'='',null,(select id from `user` where `user`='bob'))),true)
on duplicate key update `read`=values(`read`),`write`=values(`write`),owner_user_id=values(owner_user_id),provisioned=values(provisioned);
select * from user_access order by user_id,topic;

create table dest(id int primary key,v tinyint null,label varchar(20),key idx_v(v));
insert into dest values(100,50,'sentinel');
insert into dest values(1,10,'literal'),(2,(select id from `user` where `user`='alice'),'one'),(3,(select id from `user` where `user`='missing'),'zero');
insert into dest values(4,(select if(''='',null,(select id from `user` where `user`='alice'))),'nested');
insert into dest values(5,case when false then (select id from `user` where `user`='alice') else (select id from `user` where `user`='bob') end,'case');
insert into dest values(6,10,'six'),(7,(select id from `user` where `user`='alice'),'seven'),(8,20,'eight');
select * from dest order by id;

-- Literal batching must not move a duplicate action after the final lookup row.
insert into dest values(10,(select id from `user` where `user`='alice'),'first'),(10,20,'middle'),(10,(select id from `user` where `user`='bob'),'last')
on duplicate key update v=values(v),label=values(label);
select * from dest where id=10;
insert ignore into dest values(10,(select id from `user` where `user`='alice'),'ignored'),(12,(select id from `user` where `user`='bob'),'new');
select * from dest where id in (10,12) order by id;

-- A later scalar cardinality or assignment error must roll back the whole statement.
insert into dest values(13,(select id from `user` where id=1),'before-card'),(14,(select id from `user`),'bad-card');
select count(*) as partial_rows from dest where id in (13,14);
insert into dest values(13,(select id from `user` where id=1),'before-cast'),(14,(select max(id)+200 from `user`),'bad-cast');
select count(*) as partial_rows from dest where id in (13,14);
select * from dest where id=100;
select id from dest force index(idx_v) where v=1 order by id;
select id from dest ignore index(idx_v) where v=1 order by id;

-- Same prepared statement: empty result, error, then successful recovery.
prepare values_lookup_stmt from 'insert into dest values(?,(select id from `user` where id<=?),?)';
set @lookup_id=9;
set @lookup_bound=0;
set @lookup_label='prepared';
execute values_lookup_stmt using @lookup_id,@lookup_bound,@lookup_label;
select * from dest where id=9;
delete from dest where id=9;
set @lookup_bound=2;
execute values_lookup_stmt using @lookup_id,@lookup_bound,@lookup_label;
select count(*) as partial_rows from dest where id=9;
set @lookup_bound=1;
execute values_lookup_stmt using @lookup_id,@lookup_bound,@lookup_label;
select * from dest where id=9;
deallocate prepare values_lookup_stmt;

-- Ordinary DEFAULT/generated values remain on their existing assignment path.
create table defaults_dest(id int primary key,v int default 7,g int generated always as(v+1) stored);
insert into defaults_dest(id,v) values((select id from `user` where id=1),default);
select * from defaults_dest;
insert into dest values(20,(select id from `user` where id=2),'rollback');
begin;
insert into dest values(21,(select id from `user` where id=1),'transaction');
rollback;
select count(*) as rollback_rows from dest where id=21;

-- SQL UDF expansion must not bypass the explicit-subquery row admission bound.
create table udf_dest(v int);
create function values_lookup() returns int language sql as 'select max(id) from `user`';
create function values_nested_lookup() returns int language sql as 'values_lookup()';
create function values_constant() returns int language sql as '1+1';
create function values_upper_lookup() returns int language sql as '(SELECT max(id) FROM `user`)';
create function values_wrapped_lookup() returns int language sql as 'coalesce((SeLeCt max(id) FrOm `user`),0)';
insert into udf_dest values(values_upper_lookup());
insert into udf_dest values(values_wrapped_lookup());
select count(*) as udf_expression_partial_rows from udf_dest;
insert into udf_dest values(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup()),(values_lookup());
select count(*) as udf_partial_rows from udf_dest;
insert into udf_dest values(values_nested_lookup());
insert into udf_dest values((select 1)),(values_lookup());
select count(*) as udf_partial_rows from udf_dest;
insert into udf_dest values(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant()),(values_constant());
select count(*) as expression_rows,sum(v) as expression_sum from udf_dest;
insert into udf_dest values((select values_lookup()));
select count(*) as bounded_lookup_rows from udf_dest where v=2;
insert into udf_dest values((select values_upper_lookup()));
select count(*) as bounded_upper_lookup_rows from udf_dest where v=2;
drop function values_upper_lookup();
drop function values_wrapped_lookup();
drop function values_nested_lookup();
drop function values_lookup();
drop function values_constant();
drop table udf_dest;

-- An INSERT-only user still needs SELECT on the SQL-visible lookup source.
create role values_lookup_role;
create user values_lookup_user identified by 'values_lookup_test' default role values_lookup_role;
grant connect on account * to values_lookup_role;
grant insert on table insert_values_subquery.dest to values_lookup_role;
grant values_lookup_role to values_lookup_user;
-- @session:id=1&user=sys:values_lookup_user:values_lookup_role&password=values_lookup_test
insert into insert_values_subquery.dest values(200,(select id from insert_values_subquery.`user` where id=1),'denied');
-- @session
select count(*) as denied_rows from dest where id=200;
grant select on table insert_values_subquery.`user` to values_lookup_role;
-- @session:id=2&user=sys:values_lookup_user:values_lookup_role&password=values_lookup_test
insert into insert_values_subquery.dest values(200,(select id from insert_values_subquery.`user` where id=1),'allowed');
-- @session
select * from dest where id=200;
drop user values_lookup_user;
drop role values_lookup_role;
drop database insert_values_subquery;
