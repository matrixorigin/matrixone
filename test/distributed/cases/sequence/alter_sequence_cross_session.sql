# issue #30371: ALTER SEQUENCE in a session that never called NEXTVAL must
# not reset is_called to false; otherwise the next NEXTVAL replays the last
# issued value and sequence-backed default inserts fail with duplicate key.
drop database if exists issue30371;
create database issue30371;
use issue30371;

create sequence seq_cs_01 as bigint increment by 1 start with 1;
create table t_cs_01(id bigint primary key default nextval('seq_cs_01'), v int);
insert into t_cs_01(v) values (10);
select id,v from t_cs_01;
select * from seq_cs_01;

-- @session:id=1
use issue30371;
alter sequence seq_cs_01 increment by 2;
select * from seq_cs_01;
-- @session

insert into t_cs_01(v) values (20);
select id,v from t_cs_01;
select * from seq_cs_01;

-- a never-issued sequence must still return its start value after a
-- cross-session alter
create sequence seq_cs_02 as bigint;
-- @session:id=2
use issue30371;
alter sequence seq_cs_02 increment by 5;
select * from seq_cs_02;
-- @session
select nextval('seq_cs_02');
select nextval('seq_cs_02');

-- same-session alter behavior is unchanged: the next value keeps advancing
create sequence seq_cs_03 as bigint;
create table t_cs_03(id bigint primary key default nextval('seq_cs_03'), v int);
insert into t_cs_03(v) values (10);
alter sequence seq_cs_03 increment by 2;
insert into t_cs_03(v) values (20);
select id,v from t_cs_03;

drop database issue30371;
