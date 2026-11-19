-- Review 5695003826 chained to issue #30342: a self-referencing FK's
-- equivalent-key transition rebinds the catalog immediately for both the
-- INPLACE unique-key drop and the COPY primary-key drop, keeps enforcement,
-- survives old-name recreation on equivalent columns, and still rejects
-- removing the last covering key.
drop database if exists fk_self_rebind_durable;
create database fk_self_rebind_durable;
use fk_self_rebind_durable;
-- INPLACE: drop the unique key carrying the self-referencing binding while
-- an equivalent unique key survives.
create table t1 (id int, pid int, unique key zkey_a(id), unique key zkey_b(id), constraint zfk1 foreign key (pid) references t1(id));
insert into t1 values (1,1);
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_self_rebind_durable' order by constraint_name;
alter table t1 drop index zkey_a;
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_self_rebind_durable' order by constraint_name;
-- Recreating the removed key's name on equivalent columns must not flip the
-- rebound binding away from the surviving key.
alter table t1 add unique index zkey_a(id);
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_self_rebind_durable' order by constraint_name;
alter table t1 drop index zkey_a;
insert into t1 values (2,99);
insert into t1 values (2,2);
alter table t1 drop index zkey_b;
-- COPY: drop the primary key carrying the self-referencing binding while an
-- equivalent unique key survives.
create table t2 (id int primary key, pid int, unique key zpk_surv(id), constraint zfk2 foreign key (pid) references t2(id));
insert into t2 values (1,1);
alter table t2 drop primary key;
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_self_rebind_durable' and table_name='t2' order by constraint_name;
insert into t2 values (2,99);
insert into t2 values (2,2);
alter table t2 drop index zpk_surv;
-- No-survivor control keeps the rejection.
create table t3 (id int primary key, pid int, constraint zfk3 foreign key (pid) references t3(id));
insert into t3 values (1,1);
alter table t3 drop primary key;
drop database if exists fk_self_rebind_durable;
