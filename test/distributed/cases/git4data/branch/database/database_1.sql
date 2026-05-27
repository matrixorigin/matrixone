drop database if exists db_branch_parent;
drop database if exists db_branch_src;
drop database if exists db_branch_dst;
drop snapshot if exists db_branch_sp_from;
drop snapshot if exists db_branch_sp_to;

create database db_branch_parent;
use db_branch_parent;
create table features(id int primary key, score int);
create table labels(id int primary key, label varchar(20));
insert into features values(1, 10);
insert into labels values(1, 'base');

data branch create database db_branch_src from db_branch_parent;
data branch create database db_branch_dst from db_branch_parent;

insert into db_branch_src.features values(2, 20);
insert into db_branch_src.labels values(2, 'merge');

data branch diff database db_branch_src against database db_branch_dst;
data branch diff database db_branch_src against database db_branch_dst output count;

data branch merge database db_branch_src into database db_branch_dst;
select * from db_branch_dst.features order by id;
select * from db_branch_dst.labels order by id;
data branch diff database db_branch_src against database db_branch_dst output count;

create snapshot db_branch_sp_from for account sys;
insert into db_branch_src.features values(3, 30);
insert into db_branch_src.labels values(3, 'pick');
create snapshot db_branch_sp_to for account sys;
insert into db_branch_src.features values(4, 40);
insert into db_branch_src.labels values(4, 'after');

data branch pick database db_branch_src into database db_branch_dst between snapshot db_branch_sp_from and db_branch_sp_to;
select * from db_branch_dst.features order by id;
select * from db_branch_dst.labels order by id;

drop snapshot db_branch_sp_from;
drop snapshot db_branch_sp_to;
drop database db_branch_src;
drop database db_branch_dst;
drop database db_branch_parent;
