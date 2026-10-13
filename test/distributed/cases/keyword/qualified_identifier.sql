-- Reserved words after qualification periods are identifiers (MySQL compatibility).
-- Two source rows and one stage row distinguish updated and unmatched values.
drop database if exists qualified_identifier;
create database qualified_identifier;
use qualified_identifier;
create table rank_source (id int primary key, `rank` smallint unsigned, global_rank varchar(255));
create table rank_stage (id int unique, `rank` smallint unsigned, global_rank varchar(255));
insert into rank_source values (1,1,'old'),(2,42,'unchanged 中文');
insert into rank_stage values (1,9,'updated 中文');
select t1.rank from rank_source t1 order by id;
update rank_source as t1, rank_stage as t2 set t1.rank = t2.rank, t1.global_rank = t2.global_rank where t1.id = t2.id;
select id,`rank`,global_rank from rank_source order by id;
select t1.id,t1.rank,t1.global_rank from rank_source t1 order by t1.id;
-- Unqualified RANK remains a window function.
select id,rank() over (order by id) as position from rank_source order by id;
-- Qualified DDL and BEGIN's scanner lookahead use the same identifier rule.
create table qualified_identifier.select (id int primary key, `begin` int, `null` int);
insert into qualified_identifier.select values (1,7,null);
select t.id,t.begin,t.null from qualified_identifier.select t order by t.id;
-- Executable-comment rescanning preserves qualified BEGIN and the closing wrapper.
/*! select t.begin,t.null from qualified_identifier.select t; */;
-- A source marker after qualified BEGIN must still request result saving.
set @qualified_identifier_saved_result = @@session.save_query_result;
set save_query_result = on;
select t.begin,t.null from qualified_identifier.select t /* save_result */;
select * from result_scan(last_query_id()) as saved;
set save_query_result = @qualified_identifier_saved_result;
set @qualified_identifier_saved_result = null;
-- PREPARE extraction traverses qualified keywords inside executable comments.
/*! prepare qualified_keyword_prepare from select t.begin,t.null from qualified_identifier.select t */;
execute qualified_keyword_prepare;
deallocate prepare qualified_keyword_prepare;
prepare qualified_keyword_prepare /*! from select t.begin,t.null from qualified_identifier.select t */;
execute qualified_keyword_prepare;
deallocate prepare qualified_keyword_prepare;
-- Legacy statement recording preserves semicolons inside comments after BEGIN.
select t.begin/* ; */ ,t.null from qualified_identifier.select t;
-- SQL UDF argument rewriting must traverse qualified BEGIN before the marker.
create function qualified_keyword_udf(x int) returns int language sql as 'select t.begin + $1 from qualified_identifier.select t';
select cast(qualified_keyword_udf(5) as bigint) as udf_sum;
drop function qualified_keyword_udf(int);
create function qualified_keyword_udf_comment(x int) returns int language sql as 'select t. /* component */ begin + $1 from qualified_identifier.select t';
select cast(qualified_keyword_udf_comment(9) as bigint) as udf_sum;
drop function qualified_keyword_udf_comment(int);
drop database qualified_identifier;
show databases like 'qualified_identifier';
