-- test pushed down fake_pk_col `in` filter won't panic when decoding bloom filter, see #15118
drop database if exists testdb;
create database testdb;
use testdb;
create table hhh (a int, b int, index(`b`));
insert into hhh select *,* from generate_series(1, 81920)g;
select a from hhh where b = 999;
drop database testdb;
