-- Review 5691776262: equivalent-key rebinding must keep durable child
-- bindings correct across child TRUNCATE and recreation, and a same-statement
-- DROP PRIMARY KEY plus its replacement UNIQUE must rebind regardless of the
-- SQL clause order. Each scenario keeps FK enforcement (1452) and rejects
-- removing the last covering key (1553).
drop database if exists fk_rebind_truncate;
create database fk_rebind_truncate;
use fk_rebind_truncate;
create table p (id int primary key, u int, unique key uk1(u), unique key uk2(u));
create table c (id int primary key, u int, constraint fkc foreign key (u) references p(u));
insert into p values (1,10);
insert into c values (1,10);
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' order by constraint_name;
alter table p drop index uk1;
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' order by constraint_name;
-- The stale durable binding must not break child re-materialization.
truncate table c;
insert into p values (2,20);
insert into c values (2,20);
insert into c values(3,999);
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' order by constraint_name;
-- Full recreation of the child must heal the same stale binding.
drop table c;
create table c (id int primary key, u int, constraint fkc foreign key (u) references p(u));
insert into c values (4,20);
insert into c values(5,999);
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' and table_name='c' order by constraint_name;
-- A same-named non-covering key must never be adopted as the survivor.
create unique index uk1 on p(id);
drop index uk2 on p;
-- Same-statement DROP PRIMARY KEY plus its only replacement, both orders.
create table p2 (id int primary key, v int);
create table c2 (id int primary key, pid int, constraint fkc2 foreign key (pid) references p2(id));
insert into p2 values (1,10);
insert into c2 values (1,1);
alter table p2 drop primary key, add unique key uq_id(id);
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' and table_name='c2' order by constraint_name;
insert into p2 values (2,20);
insert into c2 values (2,2);
insert into c2 values(9,99);
drop index uq_id on p2;
create table p3 (id int primary key, v int);
create table c3 (id int primary key, pid int, constraint fkc3 foreign key (pid) references p3(id));
insert into p3 values (1,10);
alter table p3 add unique key uq_id(id), drop primary key;
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' and table_name='c3' order by constraint_name;
-- No replacement keeps the 1553 rejection.
create table p4 (id int primary key, v int);
create table c4 (id int primary key, pid int, constraint fkc4 foreign key (pid) references p4(id));
alter table p4 drop primary key;
-- Review 5691776262: a self-referencing foreign key bound to a covering
-- unique key rebinds when that key is dropped. The durable binding survives
-- the table's own TRUNCATE and recreation with enforcement intact, and the
-- last covering key stays protected.
create table fs2 (id int primary key, v int, pid int, unique key vs_a(v), unique key vs_b(v), constraint fs foreign key (pid) references fs2(v));
insert into fs2 values (1,10,10);
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' and table_name='fs2' order by constraint_name;
drop index vs_a on fs2;
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' and table_name='fs2' order by constraint_name;
truncate table fs2;
insert into fs2 values (2,20,20);
insert into fs2 values(9,90,999);
select constraint_name, table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_rebind_truncate' and table_name='fs2' order by constraint_name;
drop index vs_b on fs2;
drop table fs2;
create table fs2 (id int primary key, v int, pid int, unique key vs_a(v), unique key vs_b(v), constraint fs foreign key (pid) references fs2(v));
insert into fs2 values (1,10,10);
insert into fs2 values(9,90,999);
drop database if exists fk_rebind_truncate;
