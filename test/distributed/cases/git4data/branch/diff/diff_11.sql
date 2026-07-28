drop database if exists test_diff_columns;
create database test_diff_columns;
use test_diff_columns;

-- ============================================================
-- Case 1: Single PK, various data types, COLUMNS projection
-- Covers: INT, VARCHAR, DECIMAL, TIMESTAMP, DATE
-- ============================================================
create table c1(
id int primary key,
name varchar(30),
balance decimal(12,2),
created_at timestamp,
birthday date
);

insert into c1 values
(1, 'alice', 1000.50, '2024-01-01 10:00:00', '1990-03-15'),
(2, 'bob',   2000.75, '2024-01-02 11:00:00', '1985-07-20'),
(3, 'carol', 3000.00, '2024-01-03 12:00:00', '1992-11-08');

drop snapshot if exists c1_sp0;
create snapshot c1_sp0 for table test_diff_columns c1;

data branch create table c1_br from c1{snapshot="c1_sp0"};
update c1_br set balance = 1500.50, name = 'alice_v2' where id = 1;
delete from c1_br where id = 2;
insert into c1_br values (4, 'dave', 4000.00, '2024-02-01 09:00:00', '1988-12-25');

-- full diff (baseline for comparison)
data branch diff c1_br against c1{snapshot="c1_sp0"};

-- project single column
data branch diff c1_br against c1{snapshot="c1_sp0"} columns (name);

-- project two columns (non-PK)
data branch diff c1_br against c1{snapshot="c1_sp0"} columns (name, balance);

-- project PK + one column
data branch diff c1_br against c1{snapshot="c1_sp0"} columns (id, balance);

-- project temporal columns only
data branch diff c1_br against c1{snapshot="c1_sp0"} columns (created_at, birthday);

-- project all columns explicitly (same as no COLUMNS)
data branch diff c1_br against c1{snapshot="c1_sp0"} columns (id, name, balance, created_at, birthday);

drop snapshot c1_sp0;
drop table c1;
drop table c1_br;

-- ============================================================
-- Case 2: Composite PK (3 columns), COLUMNS projection
-- Covers: BIGINT, CHAR, VARCHAR, DOUBLE, DECIMAL, FLOAT
-- ============================================================
create table c2(
account_id bigint,
instrument char(5),
bucket varchar(6),
exposure double,
delta decimal(10,4),
margin float,
primary key (account_id, instrument, bucket)
);

insert into c2 values
(5001, 'FX001', 'spot', 120000.25, 0.4321, 0.12),
(5001, 'FX002', 'swap', 80000.00, -0.1200, 0.05),
(5002, 'EQ100', 'beta', 8500.00, -0.1250, 0.08),
(5003, 'CB200', 'spot', 62000.00, 0.0050, 0.01);

drop snapshot if exists c2_sp0;
create snapshot c2_sp0 for table test_diff_columns c2;

data branch create table c2_br from c2{snapshot="c2_sp0"};
update c2_br set exposure = 130000.00, delta = 0.5000 where account_id = 5001 and instrument = 'FX001' and bucket = 'spot';
delete from c2_br where account_id = 5003 and instrument = 'CB200' and bucket = 'spot';
insert into c2_br values (5004, 'IR300', 'swap', 45000.00, 0.2500, 0.03);

-- full diff
data branch diff c2_br against c2{snapshot="c2_sp0"};

-- project only non-PK columns
data branch diff c2_br against c2{snapshot="c2_sp0"} columns (exposure, delta, margin);

-- project partial PK + one value column
data branch diff c2_br against c2{snapshot="c2_sp0"} columns (account_id, instrument, exposure);

-- project single non-PK column
data branch diff c2_br against c2{snapshot="c2_sp0"} columns (delta);

drop snapshot c2_sp0;
drop table c2;
drop table c2_br;

-- ============================================================
-- Case 3: No PK (fake PK), COLUMNS projection
-- Covers: INT, VARCHAR, JSON
-- ============================================================
create table c3(
a int,
b varchar(20),
c json
);

insert into c3 values
(1, 'hello', '{"k":"v1"}'),
(2, 'world', '{"k":"v2"}'),
(3, 'test',  '{"k":"v3"}');

drop snapshot if exists c3_sp0;
create snapshot c3_sp0 for table test_diff_columns c3;

data branch create table c3_br from c3{snapshot="c3_sp0"};
update c3_br set b = 'updated' where a = 1;
delete from c3_br where a = 2;
insert into c3_br values (4, 'new', '{"k":"v4"}');

-- full diff
data branch diff c3_br against c3{snapshot="c3_sp0"};

-- project single column
data branch diff c3_br against c3{snapshot="c3_sp0"} columns (a);

-- project two columns
data branch diff c3_br against c3{snapshot="c3_sp0"} columns (a, b);

-- project json column
data branch diff c3_br against c3{snapshot="c3_sp0"} columns (b, c);

drop snapshot c3_sp0;
drop table c3;
drop table c3_br;

-- ============================================================
-- Case 4: NULL values with COLUMNS projection
-- Covers: columns toggling between NULL and non-NULL
-- ============================================================
create table c4(
id int primary key,
name varchar(20),
score int,
memo varchar(40),
ts timestamp
);

insert into c4 values
(1, 'alice', 100, 'good',  '2024-01-01 00:00:00'),
(2, 'bob',   null, null,   null),
(3, null,    80,  'ok',    '2024-01-03 00:00:00'),
(4, 'dave',  90,  null,    '2024-01-04 00:00:00');

drop snapshot if exists c4_sp0;
create snapshot c4_sp0 for table test_diff_columns c4;

data branch create table c4_br from c4{snapshot="c4_sp0"};
-- NULL→value and value→NULL transitions
update c4_br set score = 95, memo = 'improved' where id = 2;
update c4_br set name = null, score = null where id = 1;
delete from c4_br where id = 3;
insert into c4_br values (5, null, null, null, null);

-- full diff
data branch diff c4_br against c4{snapshot="c4_sp0"};

-- project columns with NULLs
data branch diff c4_br against c4{snapshot="c4_sp0"} columns (name, score);

-- project memo and ts (mix of null transitions)
data branch diff c4_br against c4{snapshot="c4_sp0"} columns (memo, ts);

-- project PK + one nullable column
data branch diff c4_br against c4{snapshot="c4_sp0"} columns (id, memo);

drop snapshot c4_sp0;
drop table c4;
drop table c4_br;

-- ============================================================
-- Case 5: COLUMNS + OUTPUT LIMIT
-- ============================================================
create table c5(id int primary key, a int, b int, c int, d int);

insert into c5 values (1, 10, 100, 1000, 10000);
insert into c5 values (2, 20, 200, 2000, 20000);
insert into c5 values (3, 30, 300, 3000, 30000);

drop snapshot if exists c5_sp0;
create snapshot c5_sp0 for table test_diff_columns c5;

data branch create table c5_br from c5{snapshot="c5_sp0"};
update c5_br set a = a + 1, c = c + 1 where id <= 3;
insert into c5_br values (4, 40, 400, 4000, 40000);
insert into c5_br values (5, 50, 500, 5000, 50000);

-- COLUMNS + LIMIT: larger limit than actual rows
data branch diff c5_br against c5{snapshot="c5_sp0"} columns (id, b) output limit 100;

-- verify output count is unaffected by COLUMNS
data branch diff c5_br against c5{snapshot="c5_sp0"} columns (a) output count;

-- verify output summary is unaffected by COLUMNS
data branch diff c5_br against c5{snapshot="c5_sp0"} columns (a) output summary;

drop snapshot c5_sp0;
drop table c5;
drop table c5_br;

-- ============================================================
-- Case 6: Composite PK + NULL + various types, COLUMNS projection
-- Covers: composite PK where some PK parts are projected and some not
-- ============================================================
create table c6(
region varchar(10),
dept int,
emp_id int,
salary decimal(10,2),
hire_date date,
notes varchar(40),
primary key (region, dept, emp_id)
);

insert into c6 values
('east', 1, 101, 5000.00, '2020-01-15', 'senior'),
('east', 1, 102, 4500.50, '2021-06-01', null),
('west', 2, 201, 6000.00, null,         'lead'),
('west', 2, 202, 3500.00, '2023-03-10', 'junior');

drop snapshot if exists c6_sp0;
create snapshot c6_sp0 for table test_diff_columns c6;

data branch create table c6_br from c6{snapshot="c6_sp0"};
update c6_br set salary = 5500.00, notes = 'promoted' where region = 'east' and dept = 1 and emp_id = 101;
update c6_br set hire_date = '2022-01-01', notes = null where region = 'west' and dept = 2 and emp_id = 201;
delete from c6_br where region = 'west' and dept = 2 and emp_id = 202;
insert into c6_br values ('south', 3, 301, 7000.00, '2024-01-01', 'new hire');

-- full diff
data branch diff c6_br against c6{snapshot="c6_sp0"};

-- project only value columns (no PK in projection)
data branch diff c6_br against c6{snapshot="c6_sp0"} columns (salary, notes);

-- project partial PK + value
data branch diff c6_br against c6{snapshot="c6_sp0"} columns (region, emp_id, salary);

-- project hire_date which has NULL transitions
data branch diff c6_br against c6{snapshot="c6_sp0"} columns (hire_date);

drop snapshot c6_sp0;
drop table c6;
drop table c6_br;

-- ============================================================
-- Case 7: No PK with NULL-heavy data, COLUMNS projection
-- Covers: all-NULL row, vecf32 type
-- ============================================================
create table c7(
x int,
y float,
z varchar(20),
v vecf32(3)
);

insert into c7 values
(1, 1.5,  'aaa', '[0.1,0.2,0.3]'),
(2, null, null,  null),
(3, 3.5,  'ccc', '[0.7,0.8,0.9]');

drop snapshot if exists c7_sp0;
create snapshot c7_sp0 for table test_diff_columns c7;

data branch create table c7_br from c7{snapshot="c7_sp0"};
update c7_br set y = 2.5, z = 'bbb' where x = 2;
delete from c7_br where x = 3;
insert into c7_br values (4, null, null, null);

-- full diff
data branch diff c7_br against c7{snapshot="c7_sp0"};

-- project only numeric columns
data branch diff c7_br against c7{snapshot="c7_sp0"} columns (x, y);

-- project varchar + vector
data branch diff c7_br against c7{snapshot="c7_sp0"} columns (z, v);

drop snapshot c7_sp0;
drop table c7;
drop table c7_br;

-- ============================================================
-- Case 8: Duplicate columns in COLUMNS list (dedup behavior)
-- ============================================================
create table c8(id int primary key, a int, b int);
insert into c8 values (1, 10, 100), (2, 20, 200);

drop snapshot if exists c8_sp0;
create snapshot c8_sp0 for table test_diff_columns c8;

data branch create table c8_br from c8{snapshot="c8_sp0"};
update c8_br set a = 15 where id = 1;

-- duplicate column in list should be deduplicated
data branch diff c8_br against c8{snapshot="c8_sp0"} columns (a, a, b);

drop snapshot c8_sp0;
drop table c8;
drop table c8_br;

-- ============================================================
-- Case 9: Case-insensitive column names
-- ============================================================
create table c9(Id int primary key, Name varchar(20), Score int);
insert into c9 values (1, 'test', 99);

drop snapshot if exists c9_sp0;
create snapshot c9_sp0 for table test_diff_columns c9;

data branch create table c9_br from c9{snapshot="c9_sp0"};
update c9_br set Score = 100 where Id = 1;

-- mixed case column names
data branch diff c9_br against c9{snapshot="c9_sp0"} columns (NAME, score);

drop snapshot c9_sp0;
drop table c9;
drop table c9_br;

-- ============================================================
-- Case 10: COLUMNS with snapshot on both sides
-- ============================================================
create table c10(a int primary key, b int, c varchar(20));
insert into c10 values (1, 10, 'v1'), (2, 20, 'v2');
drop snapshot if exists c10_sp0;
create snapshot c10_sp0 for table test_diff_columns c10;

update c10 set b = 15, c = 'v1_mod' where a = 1;
insert into c10 values (3, 30, 'v3');
drop snapshot if exists c10_sp1;
create snapshot c10_sp1 for table test_diff_columns c10;

-- snapshot vs snapshot with COLUMNS
data branch diff c10{snapshot="c10_sp1"} against c10{snapshot="c10_sp0"} columns (b);
data branch diff c10{snapshot="c10_sp1"} against c10{snapshot="c10_sp0"} columns (c);
data branch diff c10{snapshot="c10_sp1"} against c10{snapshot="c10_sp0"} columns (b, c);

drop snapshot c10_sp0;
drop snapshot c10_sp1;
drop table c10;

-- ============================================================
-- Case 11: COLUMNS backward compat — no COLUMNS clause
-- Verify existing behavior is unchanged
-- ============================================================
create table c11(k int primary key, v int);
insert into c11 values (1, 100);
drop snapshot if exists c11_sp0;
create snapshot c11_sp0 for table test_diff_columns c11;

data branch create table c11_br from c11{snapshot="c11_sp0"};
insert into c11_br values (2, 200);

-- no COLUMNS: shows all columns as before
data branch diff c11_br against c11{snapshot="c11_sp0"};

-- COLUMNS with all columns: same result
data branch diff c11_br against c11{snapshot="c11_sp0"} columns (k, v);

drop snapshot c11_sp0;
drop table c11;
drop table c11_br;

drop database test_diff_columns;
