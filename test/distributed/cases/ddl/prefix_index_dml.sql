drop table if exists prefix_idx_dml_sk;
create table prefix_idx_dml_sk(id int primary key, a varchar(32), b int, index idx_a(a(4)));
insert into prefix_idx_dml_sk values (1, 'abcdef-long', 10);
update prefix_idx_dml_sk set a = 'uvwxyz-long' where id = 1;
select id, a, b from prefix_idx_dml_sk where a = 'uvwxyz-long';
delete from prefix_idx_dml_sk where id = 1;
select count(*) from prefix_idx_dml_sk where a = 'uvwxyz-long';
drop table prefix_idx_dml_sk;

drop table if exists prefix_idx_dml_uk;
create table prefix_idx_dml_uk(id int primary key, a varchar(32), b int, unique index uq_a(a(4)));
insert into prefix_idx_dml_uk values (1, 'abcdef-long', 10);
update prefix_idx_dml_uk set a = 'uvwxyz-long' where id = 1;
insert into prefix_idx_dml_uk values (2, 'abcdef-new', 20);
delete from prefix_idx_dml_uk where id = 1;
insert into prefix_idx_dml_uk values (3, 'uvwxyz-new', 30);
select id, a, b from prefix_idx_dml_uk order by id;
drop table prefix_idx_dml_uk;
