-- #29903 (review P2): LAST_INSERT_ID reflects only a user-visible auto_increment
-- column. The allocator-owned hidden fake primary key MO appends to a table with
-- no user primary key must never surface, and a user column that merely happens to
-- be named __mo_fake_pk_col (and is therefore visible) must still surface.

-- P2-1: a user-visible auto_increment column named __mo_fake_pk_col must surface.
drop table if exists li_p1;
create table li_p1(pk int primary key, `__mo_fake_pk_col` bigint unsigned auto_increment unique key, v int default 0);
insert into li_p1(pk) values(10);
-- the visible column generated 1
select last_insert_id();
select pk, `__mo_fake_pk_col` from li_p1 order by pk;

-- ODKU must use visibility, including for changed-conflict fallback IDs.
drop table if exists li_odku_marker;
create table li_odku_marker(id int primary key auto_increment) auto_increment = 700;
insert into li_odku_marker values();
insert into li_p1(pk, v) values(10, 1) on duplicate key update v = values(v);
select last_insert_id();
insert into li_p1(pk, v) values(11, 2) on duplicate key update v = values(v);
select last_insert_id();
insert into li_p1(pk, `__mo_fake_pk_col`, v) values(12, 100, 3) on duplicate key update v = values(v);
select last_insert_id();
insert into li_p1(pk, `__mo_fake_pk_col`, v) values(11, 3, 2) on duplicate key update v = values(v);
select last_insert_id();
-- The existing unique-column assignment rejection is unchanged; no row writes.
insert into li_p1(pk, v) values(10, 9) on duplicate key update `__mo_fake_pk_col` = 999;
select pk, `__mo_fake_pk_col`, v from li_p1 order by pk;
drop table li_odku_marker;

-- P2-2: a user auto_increment column with no primary key gets a hidden fake
-- primary key; an explicit-id insert generates only the hidden key, whose value
-- must not surface (LAST_INSERT_ID stays at the previous user-column value).
drop table if exists li_p2;
create table li_p2(id bigint auto_increment, v int, unique key uk(id));
insert into li_p2(v) values(1);
-- id auto-generated: 1
select last_insert_id();
insert into li_p2(id,v) values(100,2);
-- unchanged: 1 (the hidden fake primary key must not surface)
select last_insert_id();
select id, v from li_p2 order by id;

drop table if exists li_p1;
drop table if exists li_p2;
