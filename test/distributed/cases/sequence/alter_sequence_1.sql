create sequence alter_seq_01  as smallint;
show sequences;
alter sequence alter_seq_01 as bigint;
show sequences;
select nextval('alter_seq_01'),currval('alter_seq_01');
alter sequence alter_seq_01 minvalue 1 maxvalue 100;
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
alter sequence alter_seq_01 cycle;
select * from alter_seq_01;
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
alter sequence alter_seq_01 maxvalue 1000;
alter sequence alter_seq_01 increment by 10;
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
alter sequence alter_seq_01 start with 900;
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
drop sequence alter_seq_01;

-- alter sequence increment and minvalue
create sequence if not exists alter_seq_01 as bigint increment by 100 minvalue 20  start with 50 cycle;
select * from alter_seq_01;
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
alter sequence if exists alter_seq_01 as int increment by 200 minvalue 10 no cycle;
select * from alter_seq_01;
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
select nextval('alter_seq_01'),currval('alter_seq_01');
drop sequence alter_seq_01;

-- alter sequence increment
create sequence alter_seq_02 increment 3;
select nextval('alter_seq_02');
select nextval('alter_seq_02'),currval('alter_seq_02');
select * from alter_seq_02;
alter sequence alter_seq_02 increment 10;
select nextval('alter_seq_02'),currval('alter_seq_02');
select nextval('alter_seq_02'),currval('alter_seq_02');
alter sequence alter_seq_02 increment by -10;
select nextval('alter_seq_02');
select nextval('alter_seq_02'),currval('alter_seq_02');
select nextval('alter_seq_02'),currval('alter_seq_02');
drop sequence alter_seq_02;

-- alter sequence start value
create sequence alter_seq_03 start 10000;
select nextval('alter_seq_03');
select nextval('alter_seq_03'),currval('alter_seq_03');
-- START value cannot be less than MINVALUE
alter sequence alter_seq_03 minvalue 999 maxvalue 1999;
-- Syntax error
alter sequence alter_seq_03;
alter sequence alter_seq_03 start 9999;
select nextval('alter_seq_03'),currval('alter_seq_03');
select nextval('alter_seq_03'),currval('alter_seq_03');
drop sequence alter_seq_03;

-- alter MINVALUE equal MAXVALUE
create sequence alter_seq_04 start 9;
select nextval('alter_seq_04'),currval('alter_seq_04');
select nextval('alter_seq_04'),currval('alter_seq_04');
alter sequence alter_seq_04 minvalue 10 maxvalue 10 no cycle;
select * from alter_seq_04;
select nextval('alter_seq_04'),currval('alter_seq_04');
drop sequence alter_seq_04 ;

-- alter sequence no cycle to cycle
create sequence alter_seq_05 increment 2 minvalue 1 maxvalue 6 no cycle;
select nextval('alter_seq_05'),currval('alter_seq_05');
select nextval('alter_seq_05'),currval('alter_seq_05');
alter sequence alter_seq_05 increment 2 minvalue 1 maxvalue 6 cycle;
select nextval('alter_seq_05'),currval('alter_seq_05');
select nextval('alter_seq_05'),currval('alter_seq_05');
select nextval('alter_seq_05'),currval('alter_seq_05');
select * from alter_seq_05;

-- alter sequence maxvalue
create sequence alter_seq_06 increment 20 minvalue 140 maxvalue 200 no cycle;
select nextval('alter_seq_06'),currval('alter_seq_06');
select nextval('alter_seq_06'),currval('alter_seq_06');
alter sequence alter_seq_06 maxvalue 220;
select nextval('alter_seq_06'),currval('alter_seq_06');
select nextval('alter_seq_06'),currval('alter_seq_06');
select nextval('alter_seq_06'),currval('alter_seq_06');
select nextval('alter_seq_06'),currval('alter_seq_06');

-- abnormal: alter not exists sequence
alter sequence alter_seq_02 increment by -10;
