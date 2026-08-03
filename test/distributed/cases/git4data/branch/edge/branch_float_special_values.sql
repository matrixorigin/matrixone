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

-- Portable SQL cannot use the hidden fake PK, so it deletes by full row.
-- Generate the portable script, then apply its exact NaN/NULL/infinity predicate
-- forms to the destination. Delete plus insert models the generated update and
-- proves that the old NaN row does not survive beside the new row.
create table portable_base(f32 float, f64 double, marker double, note varchar(16));
insert into portable_base values
    (cast('NaN' as float), cast('NaN' as double), null, 'remove'),
    (cast('NaN' as float), cast('NaN' as double), cast('Inf' as double), 'update'),
    (cast('Inf' as float), cast('-Inf' as double), null, 'keep');
data branch create table portable_src from portable_base;
data branch create table portable_dst from portable_base;
delete from portable_src where note = 'remove';
update portable_src set marker = cast('-Inf' as double), note = 'updated' where note = 'update';

-- @ignore:0,1
data branch diff portable_src against portable_dst output file '/tmp/';

delete from portable_dst
where serial(f32) = serial(cast('NaN' as float))
    and serial(f64) = serial(cast('NaN' as double))
    and marker is null and note = 'remove'
limit 1;
delete from portable_dst
where serial(f32) = serial(cast('NaN' as float))
    and serial(f64) = serial(cast('NaN' as double))
    and serial(marker) = serial(cast('Inf' as double)) and note = 'update'
limit 1;
insert into portable_dst values
    (cast('NaN' as float), cast('NaN' as double), cast('-Inf' as double), 'updated');

select f32, f64, marker, note from portable_dst order by note;

-- A real FLOAT primary key must use exact bit identity in both the LCA probe
-- and the staged delete. Infinity and finite keys retain ordinary equality.
create table real_float_base(k float primary key, note varchar(24));
insert into real_float_base values
    (cast('NaN' as float), 'update'),
    (cast('Inf' as float), 'remove'),
    (cast('-Inf' as float), 'keep'),
    (1.5, 'finite');
data branch create table real_float_src from real_float_base;
data branch create table real_float_dst from real_float_base;
update real_float_src set note = 'updated' where k != k;
delete from real_float_src where k = cast('Inf' as float);

data branch merge real_float_src into real_float_dst;
select k, note from real_float_dst order by note;

-- Composite real keys need the same rule independently for every FLOAT and
-- DOUBLE component. These rows place NaN in each component and retain
-- finite/+Inf/-Inf controls.
create table real_composite_base(
    f32 float,
    f64 double,
    tag int,
    note varchar(24),
    primary key(f32, f64, tag)
);
insert into real_composite_base values
    (cast('NaN' as float), 1.0, 1, 'update_f32'),
    (1.0, cast('NaN' as double), 2, 'update_f64'),
    (cast('Inf' as float), cast('-Inf' as double), 3, 'remove_inf'),
    (cast('-Inf' as float), cast('Inf' as double), 4, 'keep_inf'),
    (2.0, 3.0, 5, 'finite');
data branch create table real_composite_src from real_composite_base;
data branch create table real_composite_dst from real_composite_base;
update real_composite_src set note = 'updated_f32' where f32 != f32;
update real_composite_src set note = 'updated_f64' where f64 != f64;
delete from real_composite_src where tag = 3;

data branch merge real_composite_src into real_composite_dst;
select f32, f64, tag, note from real_composite_dst order by tag;

-- Generate the public portable-SQL path for the same composite real key. The
-- deterministic statements below apply the generator's exact staged-delete
-- predicate shape so the expected final table remains an executable oracle.
data branch create table real_portable_src from real_composite_base;
data branch create table real_portable_dst from real_composite_base;
update real_portable_src set note = 'updated_f32' where f32 != f32;
update real_portable_src set note = 'updated_f64' where f64 != f64;
delete from real_portable_src where tag = 3;

-- @ignore:0,1
data branch diff real_portable_src against real_portable_dst output file '/tmp/';

create table real_portable_delete_stage as
select f32 as branch_apply_key_0, f64 as branch_apply_key_1, tag as branch_apply_key_2
from real_portable_dst where 1 = 0;
insert into real_portable_delete_stage values
    (cast('NaN' as float), 1.0, 1),
    (1.0, cast('NaN' as double), 2),
    (cast('Inf' as float), cast('-Inf' as double), 3);
delete branch_apply_base
from real_portable_dst as branch_apply_base
join real_portable_delete_stage as branch_apply_stage on
    serial(branch_apply_base.f32) = serial(branch_apply_stage.branch_apply_key_0)
    and serial(branch_apply_base.f64) = serial(branch_apply_stage.branch_apply_key_1)
    and branch_apply_base.tag = branch_apply_stage.branch_apply_key_2;
insert into real_portable_dst values
    (cast('NaN' as float), 1.0, 1, 'updated_f32'),
    (1.0, cast('NaN' as double), 2, 'updated_f64');
drop table real_portable_delete_stage;

select f32, f64, tag, note from real_portable_dst order by tag;

-- MatrixOne primary keys preserve FLOAT/DOUBLE bits. Scalar equality cannot
-- distinguish NaN payloads or signed zero, so exercise each representation
-- through the public MERGE path and use serial() as an independent bit oracle.
create table bit_float_base(k float primary key, note varchar(24));
insert into bit_float_base values(bit_cast(unhex('0000c07f') as float), 'nan0');
insert into bit_float_base values(bit_cast(unhex('0100c07f') as float), 'nan1');
insert into bit_float_base values(0.0, 'poszero');
insert into bit_float_base values(bit_cast(unhex('00000080') as float), 'negzero');
data branch create table bit_float_src from bit_float_base;
data branch create table bit_float_dst from bit_float_base;
update bit_float_src set note = 'nan1_updated'
where serial(k) = serial(bit_cast(unhex('0100c07f') as float));
update bit_float_src set note = 'negzero_updated'
where serial(k) = serial(bit_cast(unhex('00000080') as float));
data branch merge bit_float_src into bit_float_dst;
select note, hex(serial(k)) from bit_float_dst order by note;

create table bit_double_base(k double primary key, note varchar(24));
insert into bit_double_base values(bit_cast(unhex('000000000000f87f') as double), 'nan0');
insert into bit_double_base values(bit_cast(unhex('010000000000f87f') as double), 'nan1');
insert into bit_double_base values(0.0, 'poszero');
insert into bit_double_base values(bit_cast(unhex('0000000000000080') as double), 'negzero');
data branch create table bit_double_src from bit_double_base;
data branch create table bit_double_dst from bit_double_base;
update bit_double_src set note = 'nan1_updated'
where serial(k) = serial(bit_cast(unhex('010000000000f87f') as double));
update bit_double_src set note = 'negzero_updated'
where serial(k) = serial(bit_cast(unhex('0000000000000080') as double));
data branch merge bit_double_src into bit_double_dst;
select note, hex(serial(k)) from bit_double_dst order by note;

-- Composite keys apply exact identity independently to both float widths;
-- paired rows differ only in the selected float representation.
create table bit_composite_base(
    f32 float,
    f64 double,
    tag int,
    note varchar(32),
    primary key(f32, f64, tag)
);
insert into bit_composite_base values
    (bit_cast(unhex('0000c07f') as float), 2.0, 1, 'keep_f32_nan0');
insert into bit_composite_base values
    (bit_cast(unhex('0100c07f') as float), 2.0, 1, 'update_f32_nan1');
insert into bit_composite_base values
    (3.0, bit_cast(unhex('000000000000f87f') as double), 2, 'keep_f64_nan0');
insert into bit_composite_base values
    (3.0, bit_cast(unhex('010000000000f87f') as double), 2, 'update_f64_nan1');
insert into bit_composite_base values(0.0, 0.0, 3, 'keep_poszero');
insert into bit_composite_base values(
    bit_cast(unhex('00000080') as float),
    bit_cast(unhex('0000000000000080') as double), 3, 'update_negzero');
data branch create table bit_composite_src from bit_composite_base;
data branch create table bit_composite_dst from bit_composite_base;
update bit_composite_src set note = 'updated_f32_nan1'
where serial(f32, f64, tag) = serial(
    bit_cast(unhex('0100c07f') as float), 2.0, 1);
update bit_composite_src set note = 'updated_f64_nan1'
where serial(f32, f64, tag) = serial(
    3.0, bit_cast(unhex('010000000000f87f') as double), 2);
update bit_composite_src set note = 'updated_negzero'
where serial(f32, f64, tag) = serial(
    bit_cast(unhex('00000080') as float),
    bit_cast(unhex('0000000000000080') as double), 3);
data branch merge bit_composite_src into bit_composite_dst;
select note, hex(serial(f32)), hex(serial(f64)), tag
from bit_composite_dst order by note;

-- Portable real-key SQL updates rows through exact serial-key predicates, so
-- storage-level key bits never pass through a delete/reinsert cycle.
data branch create table bit_portable_src from bit_double_base;
data branch create table bit_portable_dst from bit_double_base;
update bit_portable_src set note = 'nan1_updated'
where serial(k) = serial(bit_cast(unhex('010000000000f87f') as double));
update bit_portable_src set note = 'negzero_updated'
where serial(k) = serial(bit_cast(unhex('0000000000000080') as double));
-- @ignore:0,1
data branch diff bit_portable_src against bit_portable_dst output file '/tmp/';
update bit_portable_dst set note = 'nan1_updated'
where serial(k) = serial(bit_cast(unhex('010000000000f87f') as double)) limit 1;
update bit_portable_dst set note = 'negzero_updated'
where serial(k) = serial(bit_cast(unhex('0000000000000080') as double)) limit 1;
select note, hex(serial(k)) from bit_portable_dst order by note;

drop database br_float_special_values;
