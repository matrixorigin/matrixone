-- Defect A (#30360): a DEFAULT nextval(...) column must resolve and advance
-- the sequence owned by the TARGET table's database, never a same-named
-- caller-database sequence, when another database is current.
drop database if exists seq_owner_db;
drop database if exists seq_caller_db;
create database seq_owner_db;
create database seq_caller_db;

use seq_owner_db;
create sequence s as bigint increment by 1 start with 1 no cycle;
create table t (id bigint primary key default nextval('s'), v int);

-- Same-named sequence belonging to the caller database. It must stay
-- untouched by qualified inserts into owner_db.t.
use seq_caller_db;
create sequence s as bigint increment by 1 start with 100 no cycle;

-- Qualified insert resolves and advances owner_db.s.
insert into seq_owner_db.t(v) values (10);
select id, v from seq_owner_db.t;
select last_seq_num, is_called from seq_owner_db.s;
select last_seq_num, is_called from seq_caller_db.s;

insert into seq_owner_db.t(v) values (20);
select id, v from seq_owner_db.t order by id;
select last_seq_num, is_called from seq_owner_db.s;
select last_seq_num, is_called from seq_caller_db.s;

-- A caller database with NO same-named sequence must also work (base
-- defect: ERROR 1146).
drop sequence s;
insert into seq_owner_db.t(v) values (30);
select id, v from seq_owner_db.t order by id;
select last_seq_num, is_called from seq_owner_db.s;

-- Unqualified insert through the owner database stays on the same path.
use seq_owner_db;
insert into t(v) values (40);
select id, v from t order by id;
select last_seq_num, is_called from s;

drop database seq_owner_db;
drop database seq_caller_db;
