-- ENUM/SET assignment matching, execution policy and diagnostics (#30538).
set @enum_set_saved_mode=@@session.sql_mode;
set @enum_set_saved_limit=@@session.max_error_count;
drop database if exists enum_set_assignment_30538;
create database enum_set_assignment_30538;
use enum_set_assignment_30538;
create table t(id int primary key,e enum('xs','s','m'),s set('a','b','c'));

-- Valid trailing blanks and quoted zero remain valid in strict mode.
set sql_mode='STRICT_TRANS_TABLES';
insert into t values(1,'xs ','a,c '),(2,'0','0');
show count(*) warnings;
select id,e,e+0,s,s+0 from t order by id;

-- Bad text keeps the ENUM sentinel and all recognized SET members.
set sql_mode='';
insert into t values(3,'zz','a,c,x'),(4,'1_0','a,,c');
show count(*) warnings;
-- @sortkey:0,1,2
show warnings;
select id,e,e+0,s,s+0 from t where id>=3 order by id;

-- Typed out-of-domain bitmaps mask bits; quoted bitmaps become zero.
set sql_mode='STRICT_TRANS_TABLES';
insert ignore into t values(5,'bad','a,2'),(6,'xs',9),(7,'xs','9');
show count(*) warnings;
select id,e+0,s+0 from t where id>=5 order by id;

-- Strict multi-row failure is atomic, for INSERT and UPDATE.
insert into t values(20,'xs','a'),(21,'bad','b');
select count(*) from t where id>=20;
create table u(id int primary key,e enum('xs','s','m'));
insert into u values(1,'xs'),(2,'s');
update u set e='bad';
select id,e from u order by id;

-- Constant assignment diagnostics count logical rows, not function calls.
create table src(id int primary key);
insert into src values(1),(2),(3);
set sql_mode='';
insert into t select id+100,'bad','a,x,y' from src;
show count(*) warnings;
select id,e+0,s+0 from t where id>=100 order by id;
create table one_enum(e enum('xs','s','m'));
insert into one_enum select 'bad' from src;
show count(*) warnings;
set max_error_count=0;
insert into one_enum select 'bad' from src;
show count(*) warnings;
show warnings;
set max_error_count=1;
insert into one_enum select 'bad' from src;
show count(*) warnings;
show warnings;
set max_error_count=1024;

-- One prepared expression resolves the current execution's mode each time.
prepare enum_mode_reuse from 'insert into t values(200,?,?)';
set @ev='bad',@sv='a,c,x';
execute enum_mode_reuse using @ev,@sv;
show count(*) warnings;
select e+0,s+0 from t where id=200;
delete from t where id=200;
set sql_mode='STRICT_TRANS_TABLES';
execute enum_mode_reuse using @ev,@sv;
select count(*) from t where id=200;
set sql_mode='';
execute enum_mode_reuse using @ev,@sv;
show count(*) warnings;
select e+0,s+0 from t where id=200;
deallocate prepare enum_mode_reuse;

-- Internal ORDER/numeric recovery of sentinel 0 must never warn.
create table reads(id int primary key,e enum('z','a'),s set('a','','b'));
insert ignore into reads values(1,'bad',2),(2,'z',7),(3,'a',3),(4,null,null);
select e from reads group by e order by e;
show count(*) warnings;
select d.e from(select e from reads group by e)d order by d.e;
show count(*) warnings;
select e,e+0 from reads group by e order by e+0;
show count(*) warnings;
select d.s,d.s+0 from(select distinct s from reads)d order by d.s;
show count(*) warnings;
create table sink(n bigint);
insert into sink select e+0 from reads group by e;
show count(*) warnings;
select n from sink order by n;

set sql_mode=@enum_set_saved_mode;
set max_error_count=@enum_set_saved_limit;
drop database enum_set_assignment_30538;
