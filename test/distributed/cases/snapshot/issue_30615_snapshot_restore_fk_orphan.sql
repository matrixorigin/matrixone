-- Issue #30615/#30388: an explicit child-table snapshot restore or historical
-- CLONE must not return success while persisting orphan foreign-key rows.
drop database if exists i30615_1;
create database i30615_1;
create table i30615_1.p (id int primary key);
create table i30615_1.c (id int primary key, pid int, constraint fk_p foreign key (pid) references i30615_1.p(id));
insert into i30615_1.p values (1),(2);
insert into i30615_1.c values (1,1);
drop snapshot if exists i30615_snap1;
create snapshot i30615_snap1 for database i30615_1;
update i30615_1.c set pid=2 where id=1;
delete from i30615_1.p where id=1;
select * from i30615_1.c order by id;
select * from i30615_1.p order by id;
-- The historical child row references parent key 1, absent from the current
-- parent table: the restore must be rejected and keep the current state.
restore table i30615_1.c {snapshot='i30615_snap1'};
select * from i30615_1.c order by id;
select count(*) from i30615_1.c c left join i30615_1.p p on c.pid=p.id where c.pid is not null and p.id is null;
-- A direct historical CLONE of the same child must be rejected the same way.
create table i30615_1.c_copy clone i30615_1.c {snapshot='i30615_snap1'};
show tables from i30615_1;
-- A retry remains valid: bring parent key 1 back, then the same restore succeeds.
insert into i30615_1.p values (1);
restore table i30615_1.c {snapshot='i30615_snap1'};
select * from i30615_1.c order by id;
insert into i30615_1.c values (5,999);
drop snapshot i30615_snap1;
drop database i30615_1;

-- A consistent child-table restore keeps working when the referenced parent
-- keys exist in the current parent table.
drop database if exists i30615_2;
create database i30615_2;
create table i30615_2.p (id int primary key);
create table i30615_2.c (id int primary key, pid int, constraint fk_p foreign key (pid) references i30615_2.p(id));
insert into i30615_2.p values (1),(2);
insert into i30615_2.c values (1,1);
drop snapshot if exists i30615_snap2;
create snapshot i30615_snap2 for database i30615_2;
insert into i30615_2.c values (2,2);
select * from i30615_2.c order by id;
restore table i30615_2.c {snapshot='i30615_snap2'};
select * from i30615_2.c order by id;
insert into i30615_2.c values (9,999);
select count(*) from i30615_2.c c left join i30615_2.p p on c.pid=p.id where c.pid is not null and p.id is null;
drop snapshot i30615_snap2;
drop database i30615_2;

-- NULL foreign-key values are exempt from the orphan check.
drop database if exists i30615_3;
create database i30615_3;
create table i30615_3.p (id int primary key);
create table i30615_3.c (id int primary key, pid int, constraint fk_p foreign key (pid) references i30615_3.p(id));
insert into i30615_3.p values (1),(2);
insert into i30615_3.c values (1,null),(2,null);
drop snapshot if exists i30615_snap3;
create snapshot i30615_snap3 for database i30615_3;
update i30615_3.c set pid=2 where id=1;
delete from i30615_3.p where id=1;
restore table i30615_3.c {snapshot='i30615_snap3'};
select * from i30615_3.c order by id;
select count(*) from i30615_3.c c left join i30615_3.p p on c.pid=p.id where c.pid is not null and p.id is null;
drop snapshot i30615_snap3;
drop database i30615_3;

-- Self-referential tables: the single-table restore is refused loudly by the
-- existing referenced-by-foreign-key (master-table) guard, so no invalid
-- state can be committed; the current rows stay intact and enforcement keeps
-- working afterwards.
drop database if exists i30615_4;
create database i30615_4;
create table i30615_4.t (id int primary key, mid int, constraint fk_m foreign key (mid) references i30615_4.t(id));
insert into i30615_4.t values (1,null),(2,1);
drop snapshot if exists i30615_snap4;
create snapshot i30615_snap4 for database i30615_4;
insert into i30615_4.t values (3,2);
select count(*) from i30615_4.t;
restore table i30615_4.t {snapshot='i30615_snap4'};
select * from i30615_4.t order by id;
insert into i30615_4.t values (4,999);
select * from i30615_4.t order by id;
drop snapshot i30615_snap4;
drop database i30615_4;
