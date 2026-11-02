-- #27340: bounds, width, temporal narrowing, warnings, and atomicity share DML assignment policy.
set @issue_27340_saved_mode=@@session.sql_mode;
drop database if exists issue_27340_assignment_modes;
create database issue_27340_assignment_modes;
use issue_27340_assignment_modes;
create table t(id int primary key, u tinyint unsigned, d decimal(5,2), b binary(1), dt date);
create table src(id int, u bigint, d decimal(7,2), b varbinary(2), dt datetime);
insert into src values (3,300,1234.56,0x4142,'2026-10-23 12:34:56');

set sql_mode='STRICT_TRANS_TABLES';
insert into t values (1,7,7.00,0x41,'2026-10-21');
update t set u=300,d=1234.56 where id=1;
select id,u,d,hex(b),dt from t where id=1;
insert into t select * from src;
select count(*) from t where id=3;

set sql_mode='';
insert into t values (2,300,1234.56,0x4142,'2026-10-22 11:22:33');
show count(*) warnings;
select id,u,d,hex(b),length(b),dt from t where id=2;
insert into t select * from src;
show count(*) warnings;
select id,u,d,hex(b),length(b),dt from t where id=3;
update t set u='300',d='1234.56',b=0x4142,dt='2026-10-24 10:20:30' where id=1;
show count(*) warnings;
select id,u,d,hex(b),length(b),dt from t where id=1;

set sql_mode='STRICT_TRANS_TABLES';
prepare assignment_stmt from 'insert ignore into t values (?,?,?,?,?)';
set @id=4,@u=300,@d=1234.56,@b=0x4142,@dt='2026-10-25 12:00:00';
execute assignment_stmt using @id,@u,@d,@b,@dt;
show count(*) warnings;
select id,u,d,hex(b),length(b),dt from t where id=4;
deallocate prepare assignment_stmt;

set sql_mode=@issue_27340_saved_mode;
drop database issue_27340_assignment_modes;
