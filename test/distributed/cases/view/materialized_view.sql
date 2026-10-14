-- Native materialized views: deferred/manual publication and dependency guards.
drop database if exists mv_bvt;
create database mv_bvt;
use mv_bvt;
create table facts(id int primary key,k int,v bigint);
insert into facts values(1,1,10),(2,1,20),(3,2,null);
set experimental_materialized_view=0;
create materialized view totals refresh manual as select k,count(*) as n,sum(v) as s from facts group by k;
set experimental_materialized_view=1;
create materialized view totals refresh manual as select k,count(*) as n,sum(v) as s from facts group by k;
select * from totals order by k;
refresh materialized view totals;
select k,n,s from totals order by k;
insert into facts values(4,2,7);
update facts set v=11 where id=1;
delete from facts where id=2;
-- A source commit does not update the previous materialization.
select k,n,s from totals order by k;
refresh materialized view totals;
select k,n,s from totals order by k;
refresh materialized view totals;
select k,n,s from totals order by k;
-- IF NOT EXISTS preserves the existing definition and populated rows.
create materialized view if not exists totals refresh manual as select id from facts;
select k,n,s from totals order by k;
show create materialized view totals;
-- Writes and destructive source DDL must leave both relations intact.
insert into totals(k) values(9);
update totals set s=0;
delete from totals;
truncate table totals;
drop table totals;
alter table totals add column extra int;
alter table totals add column (batch_x int,batch_y varchar(8));
alter table facts add column extra int;
alter table facts add column (batch_x int,batch_y varchar(8));
truncate table facts;
drop table facts;
drop database mv_bvt;
select database();
select k,n,s from totals order by k;
select id,k,v from facts order by id;
begin;
refresh materialized view totals;
rollback;
-- Unsupported definitions must not create a relation.
create materialized view unsupported refresh manual as select rand() from facts;
create materialized view unsupported refresh manual as select distinct k from facts;
create materialized view unsupported refresh manual as select * from totals;
create materialized view unsupported refresh every interval 1 second as select * from facts;
show tables;
alter materialized view totals refresh every interval 1 minute;
show create materialized view totals;
alter materialized view totals refresh manual;
show create materialized view totals;
drop materialized view totals;
drop materialized view if exists totals;
-- Removing the dependency permits ordinary source DDL again.
alter table facts add column extra int;
alter table facts add column (batch_x int,batch_y varchar(8));
select id,k,v,extra,batch_x,batch_y from facts order by id;
drop table facts;
drop database mv_bvt;
set experimental_materialized_view=0;
