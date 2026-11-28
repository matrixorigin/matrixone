drop database if exists fk_issue_33109;
create database fk_issue_33109;
use fk_issue_33109;

-- #33109: REPLACE on a parent whose ON DELETE CASCADE child is itself the
-- parent of an ON DELETE SET NULL grandchild. The equivalent DELETE already
-- worked; REPLACE used to fail planning with "Column remapping failed".
create table kp (id int primary key);
create table kc (id int primary key, p int, foreign key (p) references kp(id) on delete cascade);
create table kg (gid int primary key, pid int, foreign key (pid) references kc(id) on delete set null);
insert into kp values (1);
insert into kc values (10, 1);
insert into kg values (100, 10);
replace into kp values (1);
select id from kp;
select id, p from kc;
select gid, pid from kg;

-- DELETE control on a fresh chain: cascade the child and SET NULL the grandchild
insert into kp values (2);
insert into kc values (20, 2);
insert into kg values (200, 20);
delete from kp where id = 2;
select id from kp order by id;
select id, p from kc;
select gid, pid from kg order by gid;

-- one-level ON DELETE SET NULL REPLACE still nulls the child
create table sp (id int primary key);
create table sc (id int primary key, pid int, foreign key (pid) references sp(id) on delete set null);
insert into sp values (1);
insert into sc values (10, 1);
replace into sp values (1);
select id from sp;
select id, pid from sc;

drop database fk_issue_33109;
