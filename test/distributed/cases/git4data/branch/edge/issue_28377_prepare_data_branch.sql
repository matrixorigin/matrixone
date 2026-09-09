-- Regression for #28377: all DATA BRANCH statement families can be prepared.
-- The first table case also checks that unqualified names retain their
-- PREPARE-time database when EXECUTE happens after USE changes the session.

drop database if exists issue_28377;
drop database if exists issue_28377_other;
drop database if exists issue_28377_source;
drop database if exists issue_28377_destination;

create database issue_28377;
create database issue_28377_other;
use issue_28377;

create table base(id int primary key, v varchar(20));
insert into base values (1, 'base'), (2, 'base');

prepare create_table_stmt from 'data branch create table prepared_create from base';
show tables from issue_28377 like 'prepared_create';
use issue_28377_other;
execute create_table_stmt;
show tables from issue_28377 like 'prepared_create';
deallocate prepare create_table_stmt;

use issue_28377;
prepare delete_table_stmt from 'data branch delete table prepared_create';
execute delete_table_stmt;
show tables from issue_28377 like 'prepared_create';
deallocate prepare delete_table_stmt;

use issue_28377;
data branch create table merge_src from base;
data branch create table merge_dst from base;
insert into merge_src values (3, 'merge');

prepare diff_stmt from 'data branch diff merge_src against merge_dst output count';
execute diff_stmt;
deallocate prepare diff_stmt;

prepare merge_stmt from 'data branch merge merge_src into merge_dst when conflict accept';
execute merge_stmt;
select * from merge_dst order by id;
deallocate prepare merge_stmt;

data branch create table pick_src from base;
data branch create table pick_dst from base;
insert into pick_src values (3, 'pick-parameter'), (4, 'pick-subquery');

prepare pick_parameter_stmt from 'data branch pick pick_src into pick_dst keys(?) when conflict accept';
set @pick_key = 3;
execute pick_parameter_stmt using @pick_key;
select * from pick_dst order by id;
deallocate prepare pick_parameter_stmt;

prepare pick_subquery_stmt from 'data branch pick pick_src into pick_dst keys(select id from pick_src where id = 4) when conflict accept';
execute pick_subquery_stmt;
select * from pick_dst order by id;
deallocate prepare pick_subquery_stmt;

-- Composite PK parameter markers must be counted and bound in tuple order.
create table composite_base(id int, shard varchar(20), v varchar(20), primary key(id, shard));
insert into composite_base values (1, 'base-a', 'base'), (2, 'base-b', 'base');
data branch create table composite_pick_src from composite_base;
data branch create table composite_pick_dst from composite_base;
insert into composite_pick_src values (3, 'bound', 'composite-parameters'), (4, 'mixed', 'literal-parameter'), (5, 'first', 'tuple-first'), (6, 'second', 'tuple-second');

prepare pick_composite_parameters_stmt from 'data branch pick composite_pick_src into composite_pick_dst keys((?, ?)) when conflict accept';
set @pick_composite_id = 3;
set @pick_composite_shard = 'bound';
execute pick_composite_parameters_stmt using @pick_composite_id, @pick_composite_shard;
select * from composite_pick_dst order by id, shard;
deallocate prepare pick_composite_parameters_stmt;

-- The marker order spans each component of each value tuple.
prepare pick_composite_multiple_stmt from 'data branch pick composite_pick_src into composite_pick_dst keys((?, ?), (?, ?)) when conflict accept';
set @pick_first_id = 5;
set @pick_first_shard = 'first';
set @pick_second_id = 6;
set @pick_second_shard = 'second';
execute pick_composite_multiple_stmt using @pick_first_id, @pick_first_shard, @pick_second_id, @pick_second_shard;
select * from composite_pick_dst order by id, shard;
deallocate prepare pick_composite_multiple_stmt;

prepare pick_composite_mixed_stmt from 'data branch pick composite_pick_src into composite_pick_dst keys((4, ?)) when conflict accept';
set @pick_composite_shard = 'mixed';
execute pick_composite_mixed_stmt using @pick_composite_shard;
select * from composite_pick_dst order by id, shard;
deallocate prepare pick_composite_mixed_stmt;

-- KEYS subqueries are executed as internal SQL and do not have an execution
-- parameter binding path, so PREPARE must reject their markers explicitly.
-- @regex("prepared DATA BRANCH PICK KEYS subqueries do not support parameter markers",true)
prepare pick_subquery_parameter_stmt from 'data branch pick composite_pick_src into composite_pick_dst keys(select id, shard from composite_pick_src where id = ?) when conflict accept';

create database issue_28377_source;
create table issue_28377_source.db_base(id int primary key, v varchar(20));
insert into issue_28377_source.db_base values (1, 'database-branch');

prepare create_database_stmt from 'data branch create database issue_28377_destination from issue_28377_source';
show databases like 'issue_28377_destination';
execute create_database_stmt;
select * from issue_28377_destination.db_base;
deallocate prepare create_database_stmt;

prepare delete_database_stmt from 'data branch delete database issue_28377_destination';
execute delete_database_stmt;
show databases like 'issue_28377_destination';
deallocate prepare delete_database_stmt;

drop database issue_28377;
drop database issue_28377_other;
drop database issue_28377_source;
show databases like 'issue_28377';
show databases like 'issue_28377_other';
show databases like 'issue_28377_source';
