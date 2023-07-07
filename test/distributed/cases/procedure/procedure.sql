drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

-- procedure creation and deletion test
-- [ignore for now]

-- procedure execution test

-- @case
-- @desc:test if-elseif-else (hit if)
-- @label:bvt
drop procedure if exists test_if_hit_if;
-- @delimiter .
create procedure test_if_hit_if () 'begin DECLARE v1 INT; SET v1 = 10; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_if();
drop procedure test_if_hit_if;

-- @case
-- @desc:test if-elseif-else (hit first elseif)
-- @label:bvt
drop procedure if exists test_if_hit_elseif_first_elseif;
-- @delimiter .
create procedure test_if_hit_elseif_first_elseif() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_elseif_first_elseif();
drop procedure test_if_hit_elseif_first_elseif;

-- @case
-- @desc:test if-elseif-else (hit second elseif)
-- @label:bvt
drop procedure if exists test_if_hit_second_elseif;
-- @delimiter .
create procedure test_if_hit_second_elseif() 'begin DECLARE v1 INT; SET v1 = 4; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_second_elseif();
drop procedure test_if_hit_second_elseif;

-- @case
-- @desc:test if-elseif-else (hit else)
-- @label:bvt
drop procedure if exists test_if_hit_else;
-- @delimiter .
create procedure test_if_hit_else() 'begin DECLARE v1 INT; SET v1 = 3; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_else();
drop procedure test_if_hit_else;

-- @case
-- @desc:test while
-- @label:bvt
drop procedure if exists test_while;
-- @delimiter .
create procedure test_while() 'begin DECLARE v1 INT default 10; WHILE v1 < 100 DO insert into tmp(id) values(v1); set v1 = v1 + 10; END WHILE; select * from tmp; end'
.
-- @delimiter ;
create table if not exists tmp(id int); 
call test_while();
drop table if exists tmp; 
drop procedure test_while;

-- @case
-- @desc:test repeat
-- @label:bvt
drop procedure if exists test_repeat;
-- @delimiter .
create procedure test_repeat() 'begin declare p1 int default 10; declare v1 int default 5; repeat set v1 = v1 + 1; until v1 > p1 end repeat; select v1; end';
.
-- @delimiter ;
-- @bvt:issue#10477
call test_repeat();
-- @bvt:issue
drop procedure test_repeat;

-- @case
-- @desc:test loop
-- @label:bvt
drop procedure if exists test_loop;
-- @delimiter .
create procedure test_loop() 'begin declare p1 int default 5; label1: loop set p1 = p1 + 1; if p1 < 10 THEN iterate label1; end if; leave label1; end loop label1; select p1; end'
.
-- @delimiter ;
call test_loop();
drop procedure test_loop;

-- @case
-- @desc:test inner scope variable access
-- @label:bvt
drop procedure if exists test_var_access;
-- @delimiter .
create procedure test_var_access() 'begin declare v1 int default 10; begin declare v1 int default 5; select v1; end; select v1; end'
.
-- @delimiter ;
call test_var_access();
drop procedure test_var_access;

-- @case
-- @desc:test IN parameter access (both expression and variable passing)
-- @label:bvt
drop procedure if exists test_in_param;
-- @delimiter .
create procedure test_in_param(in sid int) 'begin select val from tbh2 where id = sid; end'
.
-- @delimiter ;
call test_in_param(3);
drop procedure test_in_param;

-- @case
-- @desc:test OUT parameter access
-- @label:bvt
drop procedure if exists test_out_param;
-- @delimiter .
create procedure test_out_param(out sid int) 'begin set sid = 1000; end'
.
-- @delimiter ;
call test_out_param(@id);
select @id;
drop procedure test_out_param;

-- @case
-- @desc:test INOUT parameter access
-- @label:bvt
drop procedure if exists test_inout_param;
-- @delimiter .
create procedure test_inout_param(inout sid int) 'begin select sid; set sid = 1000 end'
.
-- @delimiter ;
set @id = 100;
call test_inout_param(@id);
select @id;
drop procedure test_inout_param;
