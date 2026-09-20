-- #23392: index identity follows sequential renames, including name reuse.
-- Separate from column-rename fixtures: this case tests index lineage, FK
-- binding, COPY reconstruction and statement rejection as one SQL contract.
drop database if exists rename_index_case;
create database rename_index_case;
use rename_index_case;
create table t(id int primary key, a varchar(32), b int,
  unique key ua(a(4)), key ib(b));
insert into t values(1, 'abcd-one', 10), (2, 'efgh-two', 20);
alter table t rename index ua to scratch, rename key ib to ua, rename index scratch to ib;
show create table t;
select distinct name, column_name, coalesce(nullif(algo_params,''),'{}') as algo_params from mo_catalog.mo_indexes
where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
order by name, column_name;
insert into t values(3, 'abcd-duplicate', 30);
insert into t values(3, 'ijkl-three', 30);
update t set b = 31 where id = 3;
delete from t where id = 2;
select id, a, b from t force index(ua) where b = 31;
select id, a, b from t force index(ib) where a = 'abcd-one';
select * from t order by id;

-- Errors are atomic; the old names and contents remain usable.
alter table t rename index ua to ib;
alter table t rename index missing to unused;
alter table t rename index `PRIMARY` to unused;
alter table t rename index ua to `PRIMARY`;
alter table t rename index ua to unused, add column x int;
alter table t rename index ua to unused, algorithm=inplace;
alter table t rename index ua to unused, lock=none;
alter table t rename index ua to ua, algorithm=copy;
select * from t order by id;
show create table t;

-- Deleted fake-PK rows cannot reappear during reconstruction.
create table fake_pk(a int, key old_idx(a));
insert into fake_pk values(1),(2),(3);
delete from fake_pk where a = 2;
alter table fake_pk rename key old_idx to new_idx;
insert into fake_pk values(4);
select a from fake_pk force index(new_idx) where a > 0 order by a;
create temporary table temporary_idx(a int, key old_idx(a));
insert into temporary_idx values(1),(2);
alter table temporary_idx rename index old_idx to new_idx;
select a from temporary_idx force index(new_idx) where a=2;
drop temporary table temporary_idx;

-- Exact FK bindings must follow their original unique-key group, not whichever
-- group later reuses its name. The outgoing FK refers to a different parent.
create table other_parent(a int, unique key ua(a));
insert into other_parent values(1);
create table parent(a int, self_a int, other_a int,
  unique key ua(a), unique key ub(a),
  constraint fk_self foreign key(self_a) references parent(a),
  constraint fk_out foreign key(other_a) references other_parent(a));
insert into parent values(1,null,1);
update parent set self_a = 1;
create table child(a int, constraint fk_child foreign key(a) references parent(a));
insert into child values(1);
-- A successful COPY inside a rolled-back transaction must restore rows,
-- original names and both the child and self FK bindings.
begin;
alter table parent rename index ua to rollback_name;
rollback;
select distinct name from mo_catalog.mo_indexes
where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='parent')
order by name;
select constraint_name, referenced_index_name from mo_catalog.mo_foreign_keys
where db_name = database() order by constraint_name;
select a from parent force index(ua) where a=1;
insert into child values(999);
alter table parent rename index ua to scratch, rename index ub to ua, rename index scratch to ub;
select constraint_name, referenced_index_name from mo_catalog.mo_foreign_keys
where db_name = database() order by constraint_name;
select constraint_name, unique_constraint_name from information_schema.referential_constraints
where constraint_schema = database() order by constraint_name;
insert into child values(999);
update parent set self_a = 999;
update parent set other_a = 999;
alter table parent drop index ub;
alter table parent drop index ua;
select * from parent;
select * from child;
drop database rename_index_case;
