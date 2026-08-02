drop database if exists data_branch_generated_column;
create database data_branch_generated_column;
use data_branch_generated_column;

-- MERGE must omit a STORED generated column from destination writes.
create table stored_base (
    id int primary key,
    a int,
    b int,
    c int generated always as (a + b) stored
);
insert into stored_base(id, a, b) values (1, 10, 20), (2, 30, 40);
data branch create table stored_branch from stored_base;
update stored_branch set a = 11 where id = 1;
insert into stored_branch(id, a, b) values (3, 50, 60);
delete from stored_branch where id = 2;
data branch diff stored_branch against stored_base output summary;
data branch merge stored_branch into stored_base when conflict accept;
select id, a, b, c from stored_base order by id;
data branch diff stored_branch against stored_base output summary;

-- PICK must omit a VIRTUAL generated column from destination writes.
create table virtual_base (
    id int primary key,
    a int,
    b int,
    c int generated always as (a + b) virtual
);
insert into virtual_base(id, a, b) values (1, 10, 20), (2, 30, 40);
data branch create table virtual_branch from virtual_base;
update virtual_branch set a = 11 where id = 1;
insert into virtual_branch(id, a, b) values (3, 50, 60);
delete from virtual_branch where id = 2;
data branch diff virtual_branch against virtual_base output summary;
data branch pick virtual_branch into virtual_base keys(1, 2, 3) when conflict accept;
select id, a, b, c from virtual_base order by id;
data branch diff virtual_branch against virtual_base output summary;

-- A destination generated column is non-writable even when an unrelated
-- source table has an ordinary column with the same name and type.
create table ordinary_source (
    id int primary key,
    a int,
    b int,
    c int
);
insert into ordinary_source values (1, 11, 20, -1), (3, 50, 60, -1);
create table generated_destination (
    id int primary key,
    a int,
    b int,
    c int generated always as (a + b) stored
);
insert into generated_destination(id, a, b) values (1, 10, 20), (2, 30, 40);
data branch merge ordinary_source into generated_destination when conflict accept;
select id, a, b, c from generated_destination order by id;

-- A STORED generated primary key participates in row identity but is still
-- omitted from destination writes.
create table stored_generated_pk_base (
    a int,
    b int generated always as (a * 2) stored,
    payload int,
    primary key (b)
);
insert into stored_generated_pk_base(a, payload) values (1, 10), (2, 20);
data branch create table stored_generated_pk_branch from stored_generated_pk_base;
update stored_generated_pk_branch set payload = 11 where b = 2;
insert into stored_generated_pk_branch(a, payload) values (3, 30);
delete from stored_generated_pk_branch where b = 4;
data branch diff stored_generated_pk_branch against stored_generated_pk_base output summary;
data branch merge stored_generated_pk_branch into stored_generated_pk_base when conflict accept;
select a, b, payload from stored_generated_pk_base order by b;
data branch diff stored_generated_pk_branch against stored_generated_pk_base output summary;

drop database data_branch_generated_column;
