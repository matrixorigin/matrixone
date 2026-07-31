-- DATA BRANCH SQL materialization must preserve every non-finite FLOAT/DOUBLE value.

drop database if exists br_float_special_values;
create database br_float_special_values;
use br_float_special_values;

create table base_t(
    id int primary key,
    f32 float,
    f64 double
);
insert into base_t values (1, 1.25, -2.5);

-- DIFF OUTPUT AS and MERGE share the row-to-SQL materialization path.
data branch create table src_t from base_t;
data branch create table dst_t from base_t;
insert into src_t values
    (2, cast('NaN' as float), cast('NaN' as double)),
    (3, cast('Inf' as float), cast('Inf' as double)),
    (4, cast('-Inf' as float), cast('-Inf' as double));

data branch diff src_t against dst_t output as diff_out;
select __mo_diff_flag, id, f32, f64 from diff_out order by id;

data branch merge src_t into dst_t;
select id, f32, f64 from dst_t order by id;

-- PICK uses the same formatter through a separate SQL appender.
data branch create table pick_src from base_t;
data branch create table pick_dst from base_t;
insert into pick_src values
    (2, cast('NaN' as float), cast('NaN' as double)),
    (3, cast('Inf' as float), cast('Inf' as double)),
    (4, cast('-Inf' as float), cast('-Inf' as double));

data branch pick pick_src into pick_dst keys(2, 3, 4);
select id, f32, f64 from pick_dst order by id;

-- No-PK MERGE deletes by full row value before applying updates and deletes.
create table no_pk_base(f double, note varchar(16));
insert into no_pk_base values
    (cast('NaN' as double), 'remove'),
    (cast('Inf' as double), 'update'),
    (cast('-Inf' as double), 'keep');
data branch create table no_pk_src from no_pk_base;
data branch create table no_pk_dst from no_pk_base;
delete from no_pk_src where note = 'remove';
update no_pk_src set note = 'updated' where note = 'update';

data branch merge no_pk_src into no_pk_dst;
select f, note from no_pk_dst order by note;

drop database br_float_special_values;
