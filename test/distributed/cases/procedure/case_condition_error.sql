-- @case
-- @desc: simple CASE condition evaluation error must propagate out of the
--        CALL and stop the procedure instead of skipping the branches and
--        continuing with the following statements (issue #33194)
-- @label:bvt
drop database if exists case_cond_error;
create database case_cond_error;
use case_cond_error;

create table choices(id int primary key);
create table effects(id int primary key, stage varchar(20));
insert into choices values(1),(2);

-- direct SQL control: the WHERE-condition expression itself is invalid
select 1 = (select id from choices);

-- the CALL must fail with the same cardinality error: no branch is entered,
-- the statement after CASE does not run, and the transaction of the CALL is
-- rolled back so no effect row survives
create procedure p_case_error() '
BEGIN
  INSERT INTO effects VALUES(1,''before'');
  CASE 1 WHEN (SELECT id FROM choices) THEN
    INSERT INTO effects VALUES(2,''then'');
  ELSE
    INSERT INTO effects VALUES(3,''else'');
  END CASE;
  INSERT INTO effects VALUES(4,''after'');
END';
call p_case_error();
select * from effects order by id;
select count(*) from effects;
drop procedure p_case_error;

-- one-row match control: the WHEN branch runs and execution continues
delete from choices where id = 2;
create procedure p_case_match() '
BEGIN
  INSERT INTO effects VALUES(1,''before'');
  CASE 1 WHEN (SELECT id FROM choices) THEN
    INSERT INTO effects VALUES(2,''then'');
  ELSE
    INSERT INTO effects VALUES(3,''else'');
  END CASE;
  INSERT INTO effects VALUES(4,''after'');
END';
call p_case_match();
select * from effects order by id;
drop procedure p_case_match;

-- nonmatch control: the ELSE branch runs
delete from effects;
create procedure p_case_else() '
BEGIN
  INSERT INTO effects VALUES(1,''before'');
  CASE 1 WHEN (SELECT id + 1 FROM choices) THEN
    INSERT INTO effects VALUES(2,''then'');
  ELSE
    INSERT INTO effects VALUES(3,''else'');
  END CASE;
  INSERT INTO effects VALUES(4,''after'');
END';
call p_case_else();
select * from effects order by id;
drop procedure p_case_else;

-- SQL NULL is a valid WHEN equality result that never matches under the
-- documented CASE contract: the CASE keeps checking the remaining WHEN
-- clauses in order, runs a later matching WHEN, and executes ELSE when none
-- matches. The CALL keeps succeeding.
delete from effects;
create procedure p_case_null_then_else() '
BEGIN
  INSERT INTO effects VALUES(1,''before'');
  CASE 1 WHEN NULL THEN
    INSERT INTO effects VALUES(2,''then'');
  ELSE
    INSERT INTO effects VALUES(3,''else'');
  END CASE;
  INSERT INTO effects VALUES(4,''after'');
END';
select 1 = NULL as null_cond;
call p_case_null_then_else();
select * from effects order by id;
select count(*) from effects;
drop procedure p_case_null_then_else;

-- NULL before a later matching WHEN: the later WHEN branch runs in order.
delete from effects;
create procedure p_case_null_then_match() '
BEGIN
  INSERT INTO effects VALUES(1,''before'');
  CASE 1 WHEN NULL THEN
    INSERT INTO effects VALUES(2,''then'');
  WHEN 1 THEN
    INSERT INTO effects VALUES(5,''second'');
  ELSE
    INSERT INTO effects VALUES(3,''else'');
  END CASE;
  INSERT INTO effects VALUES(4,''after'');
END';
call p_case_null_then_match();
select * from effects order by id;
drop procedure p_case_null_then_match;

-- NULL before a later WHEN that raises a genuine cardinality error: the CALL
-- fails with error 1242, the procedure stops, and the CALL transaction rolls
-- back, so no effect row survives and the later statement never runs.
delete from effects;
insert into choices values(2);
create procedure p_case_null_then_error() '
BEGIN
  INSERT INTO effects VALUES(1,''before'');
  CASE 1 WHEN NULL THEN
    INSERT INTO effects VALUES(2,''then'');
  WHEN (SELECT id FROM choices) THEN
    INSERT INTO effects VALUES(5,''second'');
  ELSE
    INSERT INTO effects VALUES(3,''else'');
  END CASE;
  INSERT INTO effects VALUES(4,''after'');
END';
call p_case_null_then_error();
select * from effects order by id;
select count(*) from effects;
drop procedure p_case_null_then_error;
delete from choices where id = 2;

-- nearest control: no WHEN matches and there is no ELSE, so the CASE falls
-- through and the procedure continues.
delete from effects;
create procedure p_case_noelse() '
BEGIN
  INSERT INTO effects VALUES(1,''before'');
  CASE 1 WHEN 9 THEN
    INSERT INTO effects VALUES(2,''then'');
  END CASE;
  INSERT INTO effects VALUES(4,''after'');
END';
call p_case_noelse();
select * from effects order by id;
drop procedure p_case_noelse;

drop database if exists case_cond_error;
