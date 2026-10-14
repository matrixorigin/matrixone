drop database if exists issue_29888_branch;
create database issue_29888_branch;
use issue_29888_branch;
create table t_same(id int primary key, v varchar(10));
insert into t_same values (8, 'x');
data branch create table t_same_dev from t_same;
update t_same set v = 'same' where id = 8;
update t_same_dev set v = 'same' where id = 8;
-- @regex("t_same_dev UPDATE and t_same UPDATE on pk\(8\)", true)
data branch merge t_same_dev into t_same when conflict fail;
select * from t_same order by id;
select * from t_same_dev order by id;

create table t(id int primary key, v varchar(10));
insert into t values (7, 'x');
data branch create table t_dev from t;
update t set v = 'main' where id = 7;
update t_dev set v = 'dev' where id = 7;
-- @regex("t_dev UPDATE and t UPDATE on pk\(7\)", true)
data branch merge t_dev into t when conflict fail;
-- @regex("t_dev UPDATE and t UPDATE on pk\(7\)", true)
data branch merge t_dev into t;
select * from t order by id;
select * from t_dev order by id;
data branch merge t_dev into t when conflict skip;
select * from t order by id;
data branch merge t_dev into t when conflict accept;
select * from t order by id;
drop database issue_29888_branch;
