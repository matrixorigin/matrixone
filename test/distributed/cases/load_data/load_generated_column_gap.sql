-- issue #31737: without a column list LOAD maps the file fields to the
-- writable columns in order; a stored generated column between them must not
-- shift the later fields (CSV stored wrong values, Arrow was rejected).
drop database if exists load_gen_gap;
create database load_gen_gap;
use load_gen_gap;
create table mid (id bigint primary key, base int not null, g int as (base + 1) stored, qty int not null);
load data infile '$resources/load_data/generated_column_gap.csv' into table mid fields terminated by ',' ignore 1 lines;
select id, base, qty, g from mid order by id;
create table after_pk (id bigint primary key, g int as (id * 10) stored, base int, qty int);
load data infile '$resources/load_data/generated_column_gap.csv' into table after_pk fields terminated by ',' ignore 1 lines;
select * from after_pk order by id;
-- a typed column after the gap used to receive the generated column's vector
create table no_pk (a int, g int as (a * 2) stored, b varchar(5), c int);
load data infile '$resources/load_data/generated_column_gap.csv' into table no_pk fields terminated by ',' ignore 1 lines;
select * from no_pk order by a;
create table gen_last (id bigint primary key, base int, qty int, g int as (qty + 1) stored);
load data infile '$resources/load_data/generated_column_gap.csv' into table gen_last fields terminated by ',' ignore 1 lines;
select * from gen_last order by id;
create table listed (id bigint primary key, base int not null, g int as (base + 1) stored, qty int not null);
load data infile '$resources/load_data/generated_column_gap.csv' into table listed fields terminated by ',' ignore 1 lines (id, base, qty);
select * from listed order by id;
create table inl (id bigint primary key, base int not null, g int as (base + 1) stored, qty int not null);
load data inline format='csv', data='7,17,27' into table inl fields terminated by ',';
select * from inl;
create table arr (id bigint primary key, base int not null, g int as (base + 1) stored, qty int not null);
load data infile {'filepath'='$resources/load_data/generated_column_gap.arrow','format'='arrow'} into table arr;
select * from arr order by id;
drop database load_gen_gap;
