-- Review 5699738215 / issue #30342: a compound ALTER that adds a self foreign
-- key and drops the referenced unique key in one statement must land the
-- durable binding on the surviving covering key immediately; the last
-- covering key stays protected, bad references stay rejected, and recovery
-- through a recreated equivalent key keeps working. Clause order (ADD then
-- DROP vs DROP then ADD) must never bind the removed key.
drop database if exists fk_compound_self_rebind;
create database fk_compound_self_rebind;
use fk_compound_self_rebind;
-- The reviewer's counterexample: two equivalent unique keys on the FK column.
create table t (id int, pid int, unique key a(id), unique key b(id));
alter table t add constraint fks foreign key (pid) references t(id), drop index a;
select referenced_index_name from mo_catalog.mo_foreign_keys
    where db_name='fk_compound_self_rebind' and table_name='t' and constraint_name='fks' order by referenced_index_name;
-- Enforcement continues through the surviving key.
insert into t values (1,1);
insert into t values (2,1);
insert into t values (9,99);
-- The last covering key is protected.
alter table t drop index b;
-- Recovery: recreating an equivalent key re-arms atomic replacement; the
-- binding keeps naming the survivor and follows the replacement.
alter table t add unique index a(id);
select referenced_index_name from mo_catalog.mo_foreign_keys
    where db_name='fk_compound_self_rebind' and table_name='t' and constraint_name='fks' order by referenced_index_name;
alter table t drop index b;
select referenced_index_name from mo_catalog.mo_foreign_keys
    where db_name='fk_compound_self_rebind' and table_name='t' and constraint_name='fks' order by referenced_index_name;
insert into t values (3,2);
insert into t values (9,88);
select id, pid from t order by id;
-- Reversed clause order: DROP before ADD still binds the survivor, never
-- the key removed by the same statement.
create table t2 (id int, pid int, unique key c(id), unique key d(id));
alter table t2 drop index c, add constraint fks2 foreign key (pid) references t2(id);
select referenced_index_name from mo_catalog.mo_foreign_keys
    where db_name='fk_compound_self_rebind' and table_name='t2' and constraint_name='fks2' order by referenced_index_name;
insert into t2 values (1,1);
insert into t2 values (7,77);
alter table t2 drop index d;
-- Repository hygiene: leave nothing behind for same-instance reruns.
drop database if exists fk_compound_self_rebind;
