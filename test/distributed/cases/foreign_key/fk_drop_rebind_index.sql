-- Issue #30342: dropping an FK-bound parent key rebinds to an equivalent
-- surviving key and keeps enforcement; removing the last covering key is
-- still rejected, and atomic replacement works in one statement.
drop database if exists fk_drop_rebind_index;
create database fk_drop_rebind_index;
use fk_drop_rebind_index;
create table p (id int primary key, u int, unique key uk1(u), unique key uk2(u));
create table c (id int primary key, u int, constraint fkc foreign key (u) references p(u));
insert into p values (1,10);
insert into c values (1,10);
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_drop_rebind_index' order by constraint_name;
-- Drop the recorded key while an equivalent unique key survives.
alter table p drop index uk1;
show create table p;
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_drop_rebind_index' order by constraint_name;
-- Enforcement continues through the rebound key.
insert into c values(2,999);
insert into p values (2,20);
insert into c values (3,20);
-- The last covering key is still protected.
alter table p drop index uk2;
-- Atomic replacement: drop the recorded key and add the replacement together.
create table p2 (id int primary key, u int, unique key uk_old(u));
create table c2 (id int primary key, u int, constraint fkc2 foreign key (u) references p2(u));
insert into p2 values (1,10);
insert into c2 values (1,10);
alter table p2 drop index uk_old, add unique index uk_new(u);
show create table p2;
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_drop_rebind_index' and table_name='c2' order by constraint_name;
insert into c2 values(4,999);
alter table p2 drop index uk_new;
-- PRIMARY-to-unique transition.
create table p3 (id int primary key, unique key uk_id(id));
create table c3 (id int primary key, pid int, constraint fkc3 foreign key (pid) references p3(id));
insert into p3 values (1);
insert into c3 values (1,1);
alter table p3 drop primary key;
select constraint_name, table_name, refer_table_name, referenced_index_name
    from mo_catalog.mo_foreign_keys where db_name='fk_drop_rebind_index' and table_name='c3' order by constraint_name;
insert into p3 values (2);
insert into c3 values (2,2);
insert into c3 values (9,99);
-- No-alternative control keeps the rejection on both syntaxes.
create table p4 (id int primary key, u int, unique key uk_only(u));
create table c4 (id int primary key, u int, constraint fkc4 foreign key (u) references p4(u));
insert into p4 values (1,10);
insert into c4 values (1,10);
alter table p4 drop index uk_only;
drop index uk_only on p4;

-- Repository hygiene: leave nothing behind for same-instance reruns.
drop database if exists fk_drop_rebind_index;
