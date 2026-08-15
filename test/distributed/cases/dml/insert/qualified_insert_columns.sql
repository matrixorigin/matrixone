drop database if exists qualified_insert_columns;
create database qualified_insert_columns;
use qualified_insert_columns;
create database qualified_insert_columns_other;

create table t (
    id int primary key,
    value int not null,
    note varchar(20) not null
);
create table qualified_insert_columns_other.t like qualified_insert_columns.t;

insert into qualified_insert_columns.t(id, value, note)
values (1, 10, 'unqualified');
insert into qualified_insert_columns.t(t.id, t.value, t.note)
values (2, 20, 'table');
insert into qualified_insert_columns.t(
    qualified_insert_columns.t.id,
    qualified_insert_columns.t.value,
    qualified_insert_columns.t.note
) values (3, 30, 'database');
insert into `qualified_insert_columns`.`t`(
    `qualified_insert_columns`.`t`.`id`,
    `qualified_insert_columns`.`t`.`value`,
    `qualified_insert_columns`.`t`.`note`
) values (4, 40, 'quoted');

select id, value, note from t order by id;

prepare qualified_insert from
    'insert into `qualified_insert_columns`.`t`(
        `qualified_insert_columns`.`t`.`id`,
        `qualified_insert_columns`.`t`.`value`,
        `qualified_insert_columns`.`t`.`note`
    ) values (?, ?, ?)';
set @id = 5;
set @value = 50;
set @note = 'prepared';
execute qualified_insert using @id, @value, @note;
deallocate prepare qualified_insert;

select count(*) as row_count, sum(id) as id_sum, sum(value) as value_sum from t;

insert into qualified_insert_columns.t(
    qualified_insert_columns.t.id,
    qualified_insert_columns.t.missing
) values (6, 60);

insert into qualified_insert_columns.t(
    qualified_insert_columns_other.t.id,
    qualified_insert_columns.t.value,
    qualified_insert_columns.t.note
) values (6, 60, 'wrong_database');
insert into qualified_insert_columns.t(
    qualified_insert_columns.other.id,
    qualified_insert_columns.t.value,
    qualified_insert_columns.t.note
) values (6, 60, 'wrong_table');
insert into qualified_insert_columns.t(
    nonexistent_database.t.id,
    qualified_insert_columns.t.value,
    qualified_insert_columns.t.note
) values (6, 60, 'nonexistent_database');
insert into qualified_insert_columns.t(
    qualified_insert_columns_other.t.id,
    qualified_insert_columns.t.value,
    qualified_insert_columns.t.note
) select 6, 60, 'insert_select';
insert into qualified_insert_columns.t set
    qualified_insert_columns_other.t.id = 6,
    qualified_insert_columns.t.value = 60,
    qualified_insert_columns.t.note = 'insert_set';
replace into qualified_insert_columns.t(
    qualified_insert_columns_other.t.id,
    qualified_insert_columns.t.value,
    qualified_insert_columns.t.note
) values (6, 60, 'replace_values');
replace into qualified_insert_columns.t set
    qualified_insert_columns_other.t.id = 6,
    qualified_insert_columns.t.value = 60,
    qualified_insert_columns.t.note = 'replace_set';
prepare wrong_qualified_insert from
    'insert into qualified_insert_columns.t(
        qualified_insert_columns_other.t.id,
        qualified_insert_columns.t.value,
        qualified_insert_columns.t.note
    ) values (?, ?, ?)';

select count(*) as row_count, sum(id) as id_sum, sum(value) as value_sum from t;
select count(*) as other_row_count from qualified_insert_columns_other.t;

insert into qualified_insert_columns.t(id, value, note)
values (1, 999, 'wrong_values')
on duplicate key update value = values(qualified_insert_columns.other.value);
select id, value, note from t where id = 1;

insert into qualified_insert_columns.t(t.id, t.value, t.note)
values (1, 20, 'values_table')
on duplicate key update value = values(t.value);
insert into qualified_insert_columns.t(
    qualified_insert_columns.t.id,
    qualified_insert_columns.t.value,
    qualified_insert_columns.t.note
) values (1, 30, 'values_database')
on duplicate key update value = values(qualified_insert_columns.t.value);
prepare qualified_values from
    'insert into qualified_insert_columns.t(t.id, t.value, t.note) values (?, ?, ?)
     on duplicate key update value = values(qualified_insert_columns.t.value)';
set @id = 1;
set @value = 40;
set @note = 'values_prepared';
execute qualified_values using @id, @value, @note;
deallocate prepare qualified_values;
select id, value, note from t where id = 1;

create table temp_shadow (
    id int primary key,
    value int not null,
    note varchar(20) not null
);
insert into temp_shadow values (100, 1000, 'permanent');
create temporary table temp_shadow like temp_shadow;
insert into temp_shadow(temp_shadow.id, temp_shadow.value, temp_shadow.note)
values (1, 10, 'temp_values');
insert into temp_shadow set
    temp_shadow.id = 2,
    temp_shadow.value = 20,
    temp_shadow.note = 'temp_set';
replace into temp_shadow(temp_shadow.id, temp_shadow.value, temp_shadow.note)
values (3, 30, 'replace_values');
replace into temp_shadow set
    temp_shadow.id = 4,
    temp_shadow.value = 40,
    temp_shadow.note = 'replace_set';
insert into temp_shadow(temp_shadow.id, temp_shadow.value, temp_shadow.note)
values (1, 11, 'temp_values_table')
on duplicate key update value = values(temp_shadow.value);
insert into temp_shadow(
    qualified_insert_columns.temp_shadow.id,
    qualified_insert_columns.temp_shadow.value,
    qualified_insert_columns.temp_shadow.note
) values (1, 12, 'temp_values_database')
on duplicate key update value = values(qualified_insert_columns.temp_shadow.value);
select id, value, note from temp_shadow order by id;
drop temporary table temp_shadow;
select id, value, note from temp_shadow;
drop table temp_shadow;

insert into qualified_insert_columns.t(id, value, note)
values (6, 60, 'after_error');
select id, value, note from t order by id;

drop database qualified_insert_columns;
drop database qualified_insert_columns_other;

set global lower_case_table_names = 0;
-- @session

-- @session:id=2&user=sys:root&password=111
create database CaseMode;
create table CaseMode.T(id int primary key, value int not null);
insert into CaseMode.T(CaseMode.T.id, CaseMode.T.value) values (0, 0);
insert into CaseMode.T(casemode.T.id, CaseMode.T.value) values (1, 10);
insert into CaseMode.T(CaseMode.T.id, CaseMode.t.value) select 2, 20;
insert into CaseMode.T set CaseMode.t.id = 3, CaseMode.T.value = 30;
replace into CaseMode.T(casemode.T.id, CaseMode.T.value) values (4, 40);
replace into CaseMode.T set CaseMode.t.id = 5, CaseMode.T.value = 50;
prepare wrong_case_qualified_insert from
    'insert into CaseMode.T(casemode.T.id, CaseMode.T.value) values (?, ?)';
select id, value from CaseMode.T order by id;
drop database CaseMode;
set global lower_case_table_names = 1;
-- @session
