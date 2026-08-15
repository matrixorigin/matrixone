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
values (6, 60, 'after_error');
select id, value, note from t order by id;

drop database qualified_insert_columns;
drop database qualified_insert_columns_other;
