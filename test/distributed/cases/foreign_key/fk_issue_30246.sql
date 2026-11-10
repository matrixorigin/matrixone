drop database if exists fk_issue_30246;
create database fk_issue_30246;
use fk_issue_30246;

-- declaration order 1: CASCADE first, then SET NULL
create table p1 (id int primary key);
create table c1 (id int primary key, cascade_pid int, null_pid int, v int,
    constraint fk_cascade foreign key (cascade_pid) references p1(id) on delete cascade,
    constraint fk_set_null foreign key (null_pid) references p1(id) on delete set null);
insert into p1 values (1), (2), (3);
insert into c1 values (10,1,1,100),(11,1,2,101),(12,2,1,102),(13,2,3,103);
delete from p1 where id = 1;
select id, cascade_pid, null_pid from c1 order by id;

-- declaration order 2: SET NULL first, then CASCADE
create table p2 (id int primary key);
create table c2 (id int primary key, cascade_pid int, null_pid int, v int,
    constraint fk_set_null foreign key (null_pid) references p2(id) on delete set null,
    constraint fk_cascade foreign key (cascade_pid) references p2(id) on delete cascade);
insert into p2 values (1), (2), (3);
insert into c2 values (10,1,1,100),(11,1,2,101),(12,2,1,102),(13,2,3,103);
delete from p2 where id = 1;
select id, cascade_pid, null_pid from c2 order by id;

-- three-way overlap: one child row matched by a CASCADE and two SET NULL actions
create table p3 (id int primary key);
create table c3 (id int primary key, cascade_pid int, a_pid int, b_pid int,
    constraint fk_cascade foreign key (cascade_pid) references p3(id) on delete cascade,
    constraint fk_set_null_a foreign key (a_pid) references p3(id) on delete set null,
    constraint fk_set_null_b foreign key (b_pid) references p3(id) on delete set null);
insert into p3 values (1), (2), (3);
insert into c3 values (10,1,1,1),(11,1,2,3),(12,2,1,3),(13,2,3,3);
delete from p3 where id = 1;
select id, cascade_pid, a_pid, b_pid from c3 order by id;

-- review: two ON DELETE CASCADE constraints to the same parent on a child that
-- also carries a UNIQUE index and a cascade grandchild — a parent delete matched
-- only by the second cascade must ALSO retire the hidden index rows and the
-- grandchild rows (no stale index entries, no orphans)
create table p4 (id int primary key);
create table c4 (id int primary key, a int, b int, u int,
    unique key uk_c4 (u),
    constraint fk_cascade_a foreign key (a) references p4(id) on delete cascade,
    constraint fk_cascade_b foreign key (b) references p4(id) on delete cascade);
create table g4 (id int primary key, cid int,
    key k_cid (cid),
    constraint fk_gc foreign key (cid) references c4(id) on delete cascade);
insert into p4 values (1), (2);
-- child matched ONLY through the second cascade (a=2, b=1); deleting parent
-- id=1 exercises the source merge: the index-maintenance and grandchild
-- branches, built against the first cascade stream, must follow the union
insert into c4 values (10,2,1,100);
insert into g4 values (1000,10);
delete from p4 where id = 1;
select count(*) as c4_rows from c4;
select count(*) as g4_rows from g4;
-- a stale entry in the hidden unique index would reject reusing u=100, and an
-- orphaned grandchild would block the child insert with an FK violation
insert into c4 values (11,2,2,100);
insert into g4 values (1001,11);
select id, a, b, u from c4;
select id, cid from g4;
-- and deleting through the first cascade must still retire index+grandchild
delete from p4 where id = 2;
select count(*) as c4_rows_final from c4;
select count(*) as g4_rows_final from g4;

-- review round 2: ON DELETE SET NULL declared BEFORE ON DELETE CASCADE to the
-- same parent. The SET NULL action registers the shared child through its
-- UPDATE-driven plan, which carries no delete-side maintenance; the CASCADE
-- that follows must still build its full delete plan — hidden unique-index
-- maintenance and the grandchild cascade — or the delete commits invalid
-- state (stale unique entries, orphaned grandchild rows)
create table p5 (id int primary key);
create table c5 (id int primary key, a int, b int, u int,
    unique key uk_c5 (u),
    constraint fk_setnull_b foreign key (b) references p5(id) on delete set null,
    constraint fk_cascade_a foreign key (a) references p5(id) on delete cascade);
insert into p5 values (1), (2);
insert into c5 values (10, 1, 1, 100), (12, 2, 1, 120);
delete from p5 where id = 1;
-- row 10 is cascade-deleted through a; row 12 keeps u=120 with b set to NULL
select id, a, b, u from c5 order by id;
-- the cascade must have retired row 10's unique entry: reusing u=100 succeeds
insert into c5 values (11, 2, 2, 100);
select id, a, b, u from c5 order by id;

-- grandchild variant: the cascade must also retire the grandchild rows of the
-- cascade-deleted child while rows of the set-null-only child survive
create table p6 (id int primary key);
create table c6 (id int primary key, a int, b int,
    constraint fk_setnull_b6 foreign key (b) references p6(id) on delete set null,
    constraint fk_cascade_a6 foreign key (a) references p6(id) on delete cascade);
create table g6 (id int primary key, cid int,
    key k_cid6 (cid),
    constraint fk_gc6 foreign key (cid) references c6(id) on delete cascade);
insert into p6 values (1), (2);
insert into c6 values (10, 1, 1), (12, 2, 1);
insert into g6 values (1000, 10), (1012, 12);
delete from p6 where id = 1;
select id, a, b from c6 order by id;
select id, cid from g6 order by id;

-- CASCADE declared first keeps the same outcomes (its registration already
-- carries the full delete plan, shared through the merged stream)
create table p7 (id int primary key);
create table c7 (id int primary key, a int, b int, u int,
    unique key uk_c7 (u),
    constraint fk_cascade_a7 foreign key (a) references p7(id) on delete cascade,
    constraint fk_setnull_b7 foreign key (b) references p7(id) on delete set null);
insert into p7 values (1), (2);
insert into c7 values (10, 1, 1, 100), (12, 2, 1, 120);
delete from p7 where id = 1;
select id, a, b, u from c7 order by id;
insert into c7 values (11, 2, 2, 100);
select id, a, b, u from c7 order by id;

drop table g6;
drop table c6;
drop table p6;
drop table c5;
drop table p5;
drop table c7;
drop table p7;

drop table g4;
drop table c4;
drop table p4;
drop table c1, c2, c3;
drop table p1, p2, p3;
drop database fk_issue_30246;
