-- row_count() returns the number of rows affected by the previous statement,
-- following MySQL semantics: affected rows for DML, -1 after a result-set
-- statement or a failed statement, and statement-specific counts for DDL.
drop database if exists row_count_db;
create database row_count_db;
use row_count_db;

create table t(id int primary key, v int);

-- DML reports affected rows
insert into t values (1,10),(2,20);
select row_count();

insert into t values (3,30);
select row_count();

-- insert ignore: a brand-new row is inserted
insert ignore into t values (4,40);
select row_count();

-- insert ignore: duplicate primary key is silently ignored
insert ignore into t values (1,99);
select row_count();

-- update matching rows
update t set v=v+1 where id in (1,2,3);
select row_count();

-- update matching no row
update t set v=0 where id=999;
select row_count();

-- delete
delete from t where id=4;
select row_count();

-- replace that inserts a brand-new row affects 1 row
replace into t values (50,500);
select row_count();

-- replace that collides with an existing key counts the delete and insert
replace into t values (50,999);
select row_count();

-- after a result-set statement, row_count() is -1
select v from t where id=1;
select row_count();

-- two consecutive selects: the second sees the previous select, so -1
select row_count();

-- DDL reports 0
create table t2(a int);
select row_count();

-- a status statement that still affects rows (create table ... as select)
-- reports the number of rows it inserted, not 0
create table src(a int);
insert into src values (1),(2),(3),(4),(5),(6);
select row_count();
create table ctas as select * from src;
select row_count();

-- insert ... on duplicate key update that inserts a brand-new row affects 1 row
create table odku(id int primary key, v int);
insert into odku values (1,1) on duplicate key update v=v+1;
select row_count();

-- on duplicate key update hitting an existing row that actually changes reports 2
insert into odku values (1,1) on duplicate key update v=v+1;
select row_count();

-- on duplicate key update hitting an existing row with no real change (set to its
-- current value) reports 0.
insert into odku values (1,100) on duplicate key update v=v;
select row_count();

-- load data reports the number of imported rows
create table load_t(id int, v varchar(10));
load data infile '$resources/load_data/row_count.csv' into table load_t fields terminated by ',';
select row_count();

-- prepared statement: row_count() must be evaluated at EXECUTE time, not frozen
-- at PREPARE time
insert into t values (6,60),(7,70);
prepare stmt1 from 'select row_count()';
delete from t where id in (1,2,3);
execute stmt1;
deallocate prepare stmt1;

drop database if exists row_count_db;
select row_count();

drop database if exists row_count_db;
select row_count();

create database row_count_empty_db;
drop database row_count_empty_db;
select row_count();

create database row_count_drop_db;
use row_count_drop_db;
create table d1(id int primary key, v int, index idx_v(v));
create table d2(id int);
drop database row_count_drop_db;
select row_count();

-- stored procedures retain affected rows between statements
create database row_count_procedure_db;
use row_count_procedure_db;
create table proc_t(id int primary key, v int);

create procedure proc_inner_counts() '
begin
    insert into proc_t values (1, 10), (2, 20), (3, 30);
    select row_count();
    update proc_t set v = v + 1 where id in (1, 2);
    select row_count();
    delete from proc_t where id = 3;
    select row_count();
end';
call proc_inner_counts();
select row_count();

create procedure proc_update_count() '
begin
    update proc_t set v = v + 1 where id in (1, 2);
    select row_count();
end';
call proc_update_count();

create procedure proc_delete_count() '
begin
    delete from proc_t where id = 2;
    select row_count();
end';
call proc_delete_count();

create procedure proc_final_dml() '
begin
    insert into proc_t values (4, 40), (5, 50);
end';
call proc_final_dml();
select row_count();

create procedure proc_final_result() '
begin
    select v from proc_t where id = 1;
end';
call proc_final_result();
select row_count();

create procedure proc_inner_dml() '
begin
    insert into proc_t values (6, 60);
end';
create procedure proc_outer_call() '
begin
    call proc_inner_dml();
    select row_count();
end';
call proc_outer_call();
select row_count();

create procedure proc_ddl_count() '
begin
    create table proc_ddl_t(id int);
    select row_count();
end';
call proc_ddl_count();

create procedure proc_if_count() '
begin
    insert into proc_t values (7, 70), (8, 80);
    if 1 = 1 then
        select row_count();
    end if;
end';
call proc_if_count();

create procedure proc_elseif_count() '
begin
    insert into proc_t values (9, 90), (10, 100), (11, 110);
    if 1 = 0 then
        select 0;
    elseif 1 = 1 then
        select row_count();
    end if;
end';
call proc_elseif_count();

create procedure proc_while_count() '
begin
    declare keep_going int default 1;
    insert into proc_t values (12, 120), (13, 130), (14, 140), (15, 150);
    while keep_going = 1 do
        select row_count();
        set keep_going = 0;
    end while;
end';
call proc_while_count();

create procedure proc_repeat_count() '
begin
    repeat
        insert into proc_t values (16, 160), (17, 170), (18, 180), (19, 190), (20, 200);
    until 1 = 1
    end repeat;
    select row_count();
end';
call proc_repeat_count();

create procedure proc_case_count() '
begin
    insert into proc_t values (21, 210), (22, 220), (23, 230), (24, 240), (25, 250), (26, 260);
    case 1
        when 1 then select row_count();
        else select 0;
    end case;
end';
call proc_case_count();

drop database row_count_procedure_db;
