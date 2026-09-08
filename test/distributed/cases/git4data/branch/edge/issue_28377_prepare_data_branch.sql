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
