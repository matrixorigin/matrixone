-- Regression coverage for the generated-column + secondary-index atomic DDL
-- foundation used by functional indexes.
drop database if exists generated_column_index_atomic;
create database generated_column_index_atomic;
use generated_column_index_atomic;

create table t_inline (
    id int primary key,
    source int,
    generated_key int generated always as (source * 2) stored,
    key idx_generated_key (generated_key)
);
insert into t_inline (id, source) values (1, 3), (2, 4);
select id, source, generated_key from t_inline order by id;
show create table t_inline;
show index from t_inline;

create table t_existing (id int primary key, source int);
insert into t_existing values (1, 10), (2, 20);
alter table t_existing add column generated_key int generated always as (source + 1) virtual;
create index idx_existing_generated on t_existing (generated_key);
select id, source, generated_key from t_existing order by id;
show create table t_existing;

-- A failed compound DDL must not leave a generated column, index, or table
-- catalog entry behind.
-- @regex("Duplicate", true)
create table t_failed (
    id int primary key,
    source int,
    generated_key int generated always as (source * 2) stored,
    key idx_failed (generated_key),
    key idx_failed (generated_key)
);
select count(*) from mo_catalog.mo_tables where relname = 't_failed' and reldatabase = 'generated_column_index_atomic';
select count(*) from mo_catalog.mo_columns where attrelname = 't_failed' and attdatabase = 'generated_column_index_atomic';
select count(*) from mo_catalog.mo_indexes where name = 'idx_failed';

drop database generated_column_index_atomic;
