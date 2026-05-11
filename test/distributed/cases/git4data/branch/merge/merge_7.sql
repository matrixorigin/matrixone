drop database if exists test;
create database test;
use test;

-- case 7: composite primary key with mixed types and nulls
create table t1 (
	org_id int,
	event_id int,
	name varchar(32),
	amount decimal(10,2),
	note text,
	active bool,
	primary key (org_id, event_id)
);
insert into t1 values
	(1, 1, 'Alpha', 10.00, 'Seed', true),
	(1, 2, 'beta', 20.00, '', false),
	(2, 1, 'Gamma', 30.30, null, true);

data branch create table t2 from t1;
update t2 set name = 'ALPHA', amount = 11.00, note = 'seed' where org_id = 1 and event_id = 1;
update t2 set note = null, active = false where org_id = 1 and event_id = 2;
delete from t2 where org_id = 2 and event_id = 1;
insert into t2 values (3, 3, 'path\\dir', 99.99, '', true);

data branch diff t2 against t1;
data branch merge t2 into t1;

select * from t1 order by org_id, event_id;
data branch diff t2 against t1;

drop table t1;
drop table t2;
drop database test;
