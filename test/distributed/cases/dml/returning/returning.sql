-- issue #7501: DML RETURNING v1 final-row images and fail-closed boundaries.
drop database if exists dml_returning;
create database dml_returning;
use dml_returning;

create table t (id bigint auto_increment primary key, v int default 7, g int generated always as (v + 1), note varchar(30), unique key uk_note(note), key idx_v(v));

insert into t(note) values ('values') returning id, v, g, note;
select row_count();
insert into t(v, note) select 11, 'select' returning t.*, v * 2 as twice;
select row_count();
update t as x set v = v + 10 where note = 'values' returning x.id, x.v, x.g, x.note;
select row_count();
update t set v = 99 where id = -1 returning id, v;
select row_count();
delete from t where note = 'select' returning id, v, g, note;
select row_count();

create table delete_projection_t(pk int primary key, skipped varchar(20), middle_col int, tail_col varchar(20));
insert into delete_projection_t values (1, 'skip-1', 101, 'tail-1'), (2, 'skip-2', 202, 'tail-2');
delete from delete_projection_t where pk = 1 returning middle_col;
delete from delete_projection_t where pk = 2 returning tail_col;
drop table delete_projection_t;

begin;
insert into t(v, note) values (21, 'rolled-back') returning id, v, g, note;
rollback;
select count(*) from t where note = 'rolled-back';

create table indexed_t(id int primary key, email varchar(30) unique, body text, key idx_body_prefix(body(8)));
create fulltext index ft_body on indexed_t(body);
insert into indexed_t values (1, 'a@example.com', 'only the base row is returned') returning *;
update indexed_t set body = 'updated base row' where id = 1 returning id, email, body;
select count(*) from indexed_t where match(body) against('only');
select count(*) from indexed_t where match(body) against('updated');
update indexed_t set id = 2 where id = 1 returning id;
delete from indexed_t where id = 1 returning id, email, body;
select count(*) from indexed_t where match(body) against('updated');

set experimental_ivf_index = 1;
create table vector_t(id bigint primary key, embedding vecf32(3));
create index ivf_idx using ivfflat on vector_t(embedding) lists=1 op_type 'vector_l2_ops';
insert into vector_t values (1, '[1,2,3]') returning id, embedding;
update vector_t set embedding = '[3,2,1]' where id = 1 returning id, embedding;
delete from vector_t where id = 1 returning id, embedding;
drop table vector_t;
set experimental_ivf_index = 0;

create table parent_t(id int primary key);
create table child_t(id int primary key, parent_id int, constraint fk_parent foreign key(parent_id) references parent_t(id) on delete cascade on update cascade);
insert into parent_t values (1);
insert into child_t values (1, 1) returning id, parent_id;
insert into child_t values (2, 999) returning id, parent_id;
select count(*) from child_t where id = 2;
delete from parent_t where id = 1 returning id;
update parent_t set id = 2 where id = 1 returning id;

insert ignore into t(v, note) values (1, 'ignored') returning id;
insert into t(v, note) values (1, 'values') on duplicate key update v = values(v) returning id;
update low_priority t set v = 1 returning id;
update t, parent_t set t.v = 1 returning t.id;
update t join parent_t on t.id = parent_t.id set t.v = 1 returning t.id;
update t set v = 1 returning count(*);
update t set v = 1 returning rand();
delete from t returning (select 1);
delete quick from t returning id;
delete from t partition(p0) returning id;
update t set v = 1 returning old.v;
replace into t(v, note) values (2, 'replace') returning id;

create temporary table temp_t(id int);
insert into temp_t values (1) returning id;
drop table temp_t;

drop database dml_returning;
