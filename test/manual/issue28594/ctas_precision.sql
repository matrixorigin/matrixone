-- Run through run_ctas_precision.py: it supplies an isolated database and
-- verifies every result row. Columns: case ID, expected, actual, matches.
-- Expected values describe the inheritance contract, including known failures.
set session div_precision_increment = 10;
create table src (
    a decimal(10,2), b decimal(10,2),
    q decimal(30,12) default (a / b),
    d decimal(30,12) default (1.00 / 3.00)
);
set session div_precision_increment = 4;

-- C01: empty source still transfers the bound row default.
create table empty_copy as select * from src;
insert into empty_copy(a,b) values (1,3);
select 'C01', '0.333333333333', cast(q as char), q = 0.333333333333 from empty_copy;

-- C02/C03: copied rows and future inserts are separate oracles.
insert into src(a,b) values (1,3);
create table populated_copy as select * from src;
select 'C02', '0.333333333333', cast(q as char), q = 0.333333333333 from populated_copy;
insert into populated_copy(a,b) values (2,3);
select 'C03', '0.666666666667', cast(q as char), q = 0.666666666667 from populated_copy where a = 2;

-- C04: inherited constants are a control for inherited row references.
select 'C04', '0.333333333333', cast(d as char), d = 0.333333333333 from populated_copy where a = 2;

-- C05: aliases change reference names, not arithmetic semantics.
create table alias_copy as select a as x, b as y, q from src;
insert into alias_copy(x,y) values (2,3);
select 'C05', '0.666666666667', cast(q as char), q = 0.666666666667 from alias_copy where x = 2;

-- C06: swap names simultaneously; a/b becomes b/a in the target.
create table swapped_copy as select a as b, b as a, q from src;
insert into swapped_copy(b,a) values (2,3);
select 'C06', '0.666666666667', cast(q as char), q = 0.666666666667 from swapped_copy where b = 2;

-- C07/C08: source -> SELECT -> target are independent position mappings.
create table reordered_copy as select b,a,q from src;
insert into reordered_copy(a,b) values (2,3);
select 'C07', '0.666666666667', cast(q as char), q = 0.666666666667 from reordered_copy where a = 2;
create table prepended_copy(note int) as select a,b,q from src;
insert into prepended_copy(a,b) values (2,3);
select 'C08', '0.666666666667', cast(q as char), q = 0.666666666667 from prepended_copy where a = 2;

-- C09: a real operand-scale change requires binding under the current setting.
create table changed_type(a decimal(12,3)) as select a,b,q from src;
insert into changed_type(a,b) values (2,3);
select 'C09', '0.666666700000', cast(q as char), q = 0.666666700000 from changed_type where a = 2;

-- C10: an explicitly authored target default uses the current setting.
create table authored_default (
    a decimal(10,2), b decimal(10,2), q decimal(30,12) default (a / b)
) as select a,b,q from src;
insert into authored_default(a,b) values (2,3);
select 'C10', '0.666667000000', cast(q as char), q = 0.666667000000 from authored_default where a = 2;

-- C11: nullability alone does not change the operand's numeric domain.
create table nullable_copy(a decimal(10,2) not null) as select a,b,q from src;
insert into nullable_copy(a,b) values (2,3);
select 'C11', '0.666666666667', cast(q as char), q = 0.666666666667 from nullable_copy where a = 2;

-- C12: repeated inheritance must not accumulate precision changes.
create table second_copy as select * from empty_copy;
insert into second_copy(a,b) values (2,3);
select 'C12', '0.666666666667', cast(q as char), q = 0.666666666667 from second_copy where a = 2;

-- C13: the same setting is the nearest control for C01.
set session div_precision_increment = 10;
create table same_setting as select a,b,q from src;
insert into same_setting(a,b) values (2,3);
select 'C13', '0.666666666667', cast(q as char), q = 0.666666666667 from same_setting where a = 2;

-- C14: increasing the setting must not silently change an inherited default.
set session div_precision_increment = 4;
create table low_precision(a decimal(10,2), b decimal(10,2), q decimal(30,12) default (a / b));
set session div_precision_increment = 10;
create table low_copy as select * from low_precision;
insert into low_copy(a,b) values (2,3);
select 'C14', '0.666667000000', cast(q as char), q = 0.666667000000 from low_copy;

-- C15: a NULL operand must remain NULL rather than an apparent numeric pass.
insert into low_copy(a,b) values (null,3);
select 'C15', 'NULL', ifnull(cast(q as char),'NULL'), q is null from low_copy where a is null;

-- C16: COPY ALTER is a separate existing preservation path.
set session div_precision_increment = 4;
alter table src add column note int first;
insert into src(a,b) values (2,3);
select 'C16', '0.666666666667', cast(q as char), q = 0.666666666667 from src where a = 2;

-- C17: source metadata must not be changed by any CTAS above.
insert into src(a,b) values (4,3);
select 'C17', '1.333333333333', cast(q as char), q = 1.333333333333 from src where a = 4;

-- C18/C19: making a formerly non-null operand nullable keeps bound numeric
-- precision and must also update nullable expression metadata.
set session div_precision_increment = 10;
create table nonnull_src (
    a decimal(10,2) not null, b decimal(10,2) not null,
    q decimal(30,12) default (a / b)
);
set session div_precision_increment = 4;
create table relaxed_copy(a decimal(10,2) null) as select a,b,q from nonnull_src;
insert into relaxed_copy(a,b) values (2,3);
select 'C18', '0.666666666667', cast(q as char), q = 0.666666666667 from relaxed_copy where a = 2;
insert into relaxed_copy(a,b) values (null,3);
select 'C19', 'NULL', ifnull(cast(q as char),'NULL'), q is null from relaxed_copy where a is null;
