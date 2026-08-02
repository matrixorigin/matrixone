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
data branch diff stored_branch against stored_base;
data branch diff stored_branch against stored_base columns(c, id);
data branch diff stored_branch against stored_base columns(c, id) output as stored_generated_diff;
select __mo_diff_source, __mo_diff_flag, c, id from stored_generated_diff order by id, __mo_diff_source;
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

-- Generated columns are common only when their generation semantics match.
create table formula_source (
    id int primary key,
    a int,
    g int generated always as (a * 2) stored
);
insert into formula_source(id, a) values (1, 11);
create table formula_destination (
    id int primary key,
    a int,
    g int generated always as (a * 3) stored
);
insert into formula_destination(id, a) values (1, 10);
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch diff formula_source against formula_destination output summary;
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch merge formula_source into formula_destination when conflict accept;
select id, a, g from formula_destination order by id;

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

-- Mismatched generated primary-key formulas cannot share row identity.
create table generated_pk_source (
    a int,
    g int generated always as (a * 2) stored,
    primary key (g)
);
insert into generated_pk_source(a) values (1);
create table generated_pk_destination (
    a int,
    g int generated always as (a * 3) stored,
    primary key (g)
);
insert into generated_pk_destination(a) values (1);
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch diff generated_pk_source against generated_pk_destination output summary;
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch merge generated_pk_source into generated_pk_destination when conflict accept;
select a, g from generated_pk_destination order by g;

-- Generated expressions use logical column identity rather than endpoint-local
-- ordinals when unrelated endpoint schemas order their columns differently.
create table reordered_matching_source (
    id int primary key,
    a int,
    b int,
    g int generated always as (a * 2) stored
);
insert into reordered_matching_source(id, a, b) values (1, 10, 7);
create table reordered_matching_destination (
    id int primary key,
    b int,
    a int,
    g int generated always as (a * 2) stored
);
insert into reordered_matching_destination(id, b, a) values (1, 7, 9);
data branch merge reordered_matching_source into reordered_matching_destination when conflict accept;
select id, a, b, g from reordered_matching_destination order by id;
data branch diff reordered_matching_source against reordered_matching_destination output summary;

-- These expressions have protobuf-equal local ordinals (ColPos 1) but refer to
-- different logical columns: source.a versus destination.b.
create table reordered_mismatch_source (
    id int primary key,
    a int,
    b int,
    g int generated always as (a * 2) stored
);
insert into reordered_mismatch_source(id, a, b) values (1, 10, 7);
create table reordered_mismatch_destination (
    id int primary key,
    b int,
    a int,
    g int generated always as (b * 2) stored
);
insert into reordered_mismatch_destination(id, b, a) values (1, 7, 9);
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch diff reordered_mismatch_source against reordered_mismatch_destination output summary;
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch merge reordered_mismatch_source into reordered_mismatch_destination when conflict accept;
select id, a, b, g from reordered_mismatch_destination order by id;

-- Matching generated primary-key expressions remain compatible across a
-- reordered schema and retain consistent row identity.
create table reordered_generated_pk_source (
    a int,
    b int,
    g int generated always as (a * 2) stored,
    primary key (g)
);
insert into reordered_generated_pk_source(a, b) values (10, 7);
create table reordered_generated_pk_destination (
    b int,
    a int,
    g int generated always as (a * 2) stored,
    primary key (g)
);
insert into reordered_generated_pk_destination(b, a) values (8, 10);
data branch merge reordered_generated_pk_source into reordered_generated_pk_destination when conflict accept;
select a, b, g from reordered_generated_pk_destination order by g;
data branch diff reordered_generated_pk_source against reordered_generated_pk_destination output summary;

-- Generated primary-key expressions with colliding local ordinals but different
-- logical references cannot share row identity.
create table reordered_generated_pk_mismatch_source (
    a int,
    b int,
    g int generated always as (a * 2) stored,
    primary key (g)
);
insert into reordered_generated_pk_mismatch_source(a, b) values (10, 7);
create table reordered_generated_pk_mismatch_destination (
    b int,
    a int,
    g int generated always as (b * 2) stored,
    primary key (g)
);
insert into reordered_generated_pk_mismatch_destination(b, a) values (7, 10);
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch diff reordered_generated_pk_mismatch_source against reordered_generated_pk_mismatch_destination output summary;
-- @regex("schema compatibility check: column 'g' has different generated definitions", true)
data branch merge reordered_generated_pk_mismatch_source into reordered_generated_pk_mismatch_destination when conflict accept;
select a, b, g from reordered_generated_pk_mismatch_destination order by g;

drop database data_branch_generated_column;
