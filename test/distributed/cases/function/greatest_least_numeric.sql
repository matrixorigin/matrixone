-- issue #25145: GREATEST / LEAST should implicitly promote mixed numeric
-- argument types instead of rejecting them.

-- constant literals: bigint + decimal / bigint + double
select greatest(1, 2.0);
select least(1, 2.0);
select greatest(cast(1 as bigint), cast(2.0 as double));
select least(cast(1 as bigint), cast(2.0 as double));

-- explicit casts covering the documented promotion matrix
select greatest(cast(1 as bigint), cast(2.0 as double)) as g, least(cast(1 as bigint), cast(2.0 as double)) as l;
select greatest(cast(5 as bigint), cast(2.5 as decimal(10,2)));
select least(cast(5 as bigint), cast(2.5 as decimal(10,2)));

-- same-type still works (regression guard)
select greatest(cast(1 as bigint), cast(2 as bigint));
select greatest(cast(1.0 as double), cast(2.0 as double));

-- mixed integer widths
select greatest(cast(1 as tinyint), cast(2000 as int));
select least(cast(1 as tinyint), cast(2000 as int));

-- NULL handling is preserved under promotion
select greatest(1, null, 2.0);
select least(1, null, 2.0);

-- table-driven: aggregate-vs-aggregate comparisons from the issue
drop table if exists toll_transactions;
create table toll_transactions(id int, gate_processing_sec double, lanes int);
insert into toll_transactions values
  (1, 3.5, 2), (2, 7.0, 2), (3, 1.0, 4), (4, 12.5, 3);

-- greatest(count(*), avg(x)) : bigint vs double
select greatest(count(*), avg(gate_processing_sec)) from toll_transactions;
select least(count(*), avg(gate_processing_sec)) from toll_transactions;

-- clamp pattern: greatest(0, expr) with a decimal-bearing expression
select greatest(0, count(*) - lanes * (3600.0 / avg(gate_processing_sec))) as est
from toll_transactions group by lanes order by lanes;

-- per-row mixed column types
drop table if exists mixed_num;
create table mixed_num(a bigint, b double, c decimal(20,4));
insert into mixed_num values (10, 2.5, 7.25), (3, 9.0, 4.5), (-5, -1.0, 100.0);
select a, b, c, greatest(a, b) as gab, least(a, b) as lab from mixed_num order by a;
select a, c, greatest(a, c) as gac, least(a, c) as lac from mixed_num order by a;
select greatest(a, b, c) as g3, least(a, b, c) as l3 from mixed_num order by a;

-- unsigned-only mixed widths promote to the wider unsigned type
drop table if exists uns;
create table uns(u1 tinyint unsigned, u2 int unsigned, u3 bigint unsigned);
insert into uns values (1, 300, 5000000000), (200, 50, 1), (100, 70000, 9000000000);
select greatest(u1, u2) as g_u, least(u1, u2) as l_u from uns order by u1;
select greatest(u1, u2, u3) as g3u, least(u1, u2, u3) as l3u from uns order by u1;

-- signed + unsigned that exceeds int64 promotes to decimal128 (lossless)
select greatest(cast(-3 as bigint), cast(9000000000000000000 as bigint unsigned)) as g_su;
select least(cast(-3 as bigint), cast(9000000000000000000 as bigint unsigned)) as l_su;

-- bit + integer
drop table if exists bt;
create table bt(b bit(16), i int);
insert into bt values (10, 3), (20, 25), (7, 7);
select greatest(b, i) as g_bi, least(b, i) as l_bi from bt order by i;

-- decimal128 result from mixed-scale decimals
select greatest(cast(1.5 as decimal(10,1)), cast(2.25 as decimal(20,2))) as g_dec;
select least(cast(1.5 as decimal(10,1)), cast(2.25 as decimal(20,2))) as l_dec;

-- decimal256 result (wide decimal mixed with bigint exercises the decimal256
-- branch of the executor)
select greatest(cast(1234567890123456789012345678901234567890.12 as decimal(60,2)), cast(2 as bigint)) as g_d256;
select least(cast(1234567890123456789012345678901234567890.12 as decimal(60,2)), cast(2 as bigint)) as l_d256;

-- same-Oid DECIMAL256 values whose common width exceeds DECIMAL256 must use
-- the FLOAT64 fallback instead of comparing their different-scale raw values.
-- SQL DECIMAL precision is limited to 65, but these types require 65 + 65 =
-- 130 digits after scale alignment.
select greatest(cast(0.9 as decimal(65,65)), cast(1 as decimal(65,0))) as g_d256_scale_overflow;
select least(cast(0.9 as decimal(65,65)), cast(1 as decimal(65,0))) as l_d256_scale_overflow;

-- issue #25215: mixed string and numeric arguments compare as strings.
select greatest('10', 2) as greatest_str_num;
select least('10', 2) as least_str_num;
select greatest(2, '10') as greatest_num_str;
select least(2, '10') as least_num_str;
select greatest('10', 2, 11.0) as greatest_three_mixed;
select least('10', 2, 11.0) as least_three_mixed;
select greatest('B', binary 'A') as greatest_nonbinary_binary;
select least('B', binary 'A') as least_nonbinary_binary;
select greatest(cast('10' as text), 2) as greatest_text_num;
select least(cast('10' as text), 2) as least_text_num;
select hex(greatest(cast('61' as blob), 2)) as greatest_blob_num;
select hex(least(cast('61' as blob), 2)) as least_blob_num;
select hex(greatest(cast('61' as binary), 2)) as greatest_binary_num;
select hex(least(cast('61' as binary), 2)) as least_binary_num;
select greatest(cast(2020 as year), 1999) as greatest_year_num;
select least(cast(2020 as year), 1999) as least_year_num;

-- JSON and date-bearing temporal use the dedicated json-temporal overload.
select greatest(json_extract('{"a":1}', '$'), '2') as greatest_json_varchar;
select greatest(json_extract('{"a":1}', '$'), json_extract('{"b":2}', '$')) as greatest_json_json;
select greatest(cast('2020-01-01' as date), json_extract('"2020-01-02"', '$')) as greatest_date_json;
select greatest(json_extract('"2020-01-02"', '$'), cast('2020-01-01' as date), cast(2021 as year)) as greatest_json_date_year;

-- Date-bearing temporal values compare as packed datetime values. Invalid
-- temporal text returns MatrixOne's invalid-input error. TIME uses the
-- statement date, so assert that it wins instead of snapshotting that date.
select greatest(cast('2020-01-01' as date), '2020-01-02') as greatest_date_varchar;
select greatest(cast('2020-01-01' as date), 'not-a-date');
select least(cast('2020-01-01' as date), 'not-a-date');
select cast(greatest(cast('2020-01-01' as date), cast('12:00:00' as time)) as datetime) > cast('2020-01-01' as datetime) as greatest_date_time_uses_time;
select greatest(cast(2020 as year), cast('2020-01-01' as date), 1) as greatest_year_date_num;
select hex(greatest(cast('2020-01-01' as date), cast('2020-01-02' as blob))) as greatest_date_blob;
select hex(greatest(cast('2020-01-01' as date), cast('2020-01-02' as varbinary))) as greatest_date_varbinary;

-- TIME without a date-bearing peer remains in the text domain.
select least(cast('10:00:00' as time), '09:00:00') as least_time_varchar;

-- YEAR + numeric keeps the YEAR vector and uses the year-numeric overload.
select greatest(cast(2020 as year), cast(2021.5 as decimal(10,1))) as greatest_year_decimal;
select least(cast(2020 as year), cast(2019.5 as decimal(10,1))) as least_year_decimal;
select greatest(cast(2020 as year), 2021.5) as greatest_year_double;
select least(cast(2020 as year), cast(2021 as bit(16))) as least_year_bit;
select greatest(1999, cast(2020 as year), cast(2021 as year)) as greatest_multi_year_numeric;
select greatest(cast('10:00:00' as time), 2) as greatest_time_numeric;
select greatest(cast('10:00:00' as time), cast(2 as bit(4))) as greatest_time_bit;

-- Same-Oid temporal metadata aligns scale before comparison.
select greatest(cast('10:00:00.1' as time(1)), cast('10:00:00.99' as time(2))) as greatest_time_scale;

-- All T_any arguments return a VARCHAR NULL result, and any NULL argument
-- makes the strict function result NULL.
select greatest(null, null) as greatest_all_null, least(null, null) as least_all_null;
select greatest('10', null, 2) as greatest_mixed_null, least('10', null, 2) as least_mixed_null;

-- Same-Oid normal executor paths and metadata alignment value semantics.
select greatest(cast('a' as varchar), cast('b' as varchar)) as greatest_varchar_same_oid;
select least(cast('a' as blob), cast('b' as blob)) as least_blob_same_oid;
select greatest(cast(1.5 as decimal(10,1)), cast(1.49 as decimal(12,2))) as greatest_decimal_scale_align;
select least(cast(1.5 as decimal(10,1)), cast(1.49 as decimal(12,2))) as least_decimal_scale_align;
select greatest(cast('2020-01-01 00:00:00.1' as datetime(1)), cast('2020-01-01 00:00:00.99' as datetime(2))) as greatest_datetime_scale;
select greatest(cast('2020-01-01 00:00:00.1' as timestamp(1)), cast('2020-01-01 00:00:00.99' as timestamp(2))) as greatest_timestamp_scale;

-- JSON rules: pure JSON is text, allowed text peers choose TEXT/VARCHAR, and
-- JSON is rejected before any binary or unsupported mixed-type rule.
select least(json_extract('{"a":1}', '$'), json_extract('{"b":2}', '$')) as least_json_json;
select greatest(json_extract('"b"', '$'), cast('a' as text)) as greatest_json_text;
select greatest(json_extract('10', '$'), 2) as greatest_json_numeric;
select greatest(json_extract('"10:00:00"', '$'), cast('09:00:00' as time)) as greatest_json_time;
select greatest(json_extract('2020', '$'), cast(2019 as year)) as greatest_json_year;
select greatest(json_extract('1', '$'), cast('1' as blob));
select greatest(json_extract('1', '$'), cast('1' as binary));
select greatest(json_extract('1', '$'), cast('1' as varbinary));
select greatest(json_extract('"2020-01-02"', '$'), cast('2020-01-01' as date), cast('x' as text));
select greatest(json_extract('"2020-01-02"', '$'), cast('2020-01-01' as date), cast('2020-01-03' as blob));
select greatest(json_extract('"2020-01-02 12:00:00"', '$'), cast('2020-01-01 00:00:00' as datetime)) as greatest_json_datetime;
select greatest(json_extract('"2020-01-02 12:00:00"', '$'), cast('2020-01-01 00:00:00' as timestamp)) as greatest_json_timestamp;
select greatest(json_extract('"2020-01-02"', '$'), cast('2020-01-01' as date), 20200103) as greatest_json_date_bigint;
select cast(greatest(json_extract('"2020-01-02"', '$'), cast('2020-01-01' as date), cast('12:00:00' as time)) as date) > cast('2020-01-02' as date) as greatest_json_date_time_uses_time;
select greatest(json_extract('"2020-01-02"', '$'), cast('2020-01-01' as date), interval 1 day);

-- The date-bearing temporal matrix uses packed datetime comparison and chooses
-- the documented result family from the non-temporal peer.
select greatest(cast('2020-01-01' as datetime), '2020-01-02') as greatest_datetime_varchar;
select greatest(cast('2020-01-01' as date), cast('2020-01-01 12:00:00' as datetime)) as greatest_date_datetime;
select cast(greatest(cast('2020-01-01' as date), cast('12:00:00' as time)) as datetime) > cast('2020-01-01' as datetime) as greatest_date_time_format_uses_time;
select greatest(cast('2020-01-01' as date), cast('2020-01-02' as text)) as greatest_date_text;
select least(cast('10:00:00' as time), cast('09:00:00' as text)) as least_time_text;
select greatest(cast('10:00:00' as time), cast('09:00:00' as blob)) as greatest_time_blob;
select greatest(cast('2020-01-01' as date), 20200102) as greatest_date_numeric;
select greatest(cast('2020-01-01' as date), cast(20200102 as bit(25))) as greatest_date_bit;
select greatest(cast(2020 as year), cast('10:00:00' as time)) as greatest_year_time;
select cast(greatest(cast('2020-01-01' as date), cast('12:00:00' as time), '2020-01-02') as date) > cast('2020-01-02' as date) as greatest_date_time_varchar_uses_time;
select cast(greatest(cast('2020-01-01' as date), cast('12:00:00' as time), 20200102) as date) > cast('2020-01-02' as date) as greatest_date_time_bigint_uses_time;
select cast(greatest(cast('2020-01-01' as date), cast('12:00:00' as time), cast('2020-01-02' as blob)) as date) > cast('2020-01-02' as date) as greatest_date_time_blob_uses_time;

-- String-family precedence is TEXT > CHAR/VARCHAR > BLOB > BINARY/VARBINARY.
select greatest(cast('b' as text), cast('a' as blob)) as greatest_text_blob;
select greatest(cast('b' as char), cast('a' as varbinary)) as greatest_char_varbinary;
select hex(greatest(cast('a' as blob), cast('b' as varbinary))) as greatest_blob_varbinary;
select hex(greatest(cast('a' as binary), cast('b' as varbinary))) as greatest_binary_varbinary;

-- Existing executor-supported Oids remain valid only for same-Oid calls;
-- every corresponding mixed call is rejected before a normal executor runs.
drop table if exists greatest_least_supported_oid;
create table greatest_least_supported_oid (id int primary key, v32 vecf32(3), v64 vecf64(3));
insert into greatest_least_supported_oid values (1, '[1,2,3]', '[1,2,3]');
select greatest(cast('00000000-0000-0000-0000-000000000001' as uuid), cast('00000000-0000-0000-0000-000000000002' as uuid)) as greatest_uuid_same_oid;
select greatest(cast('00000000-0000-0000-0000-000000000001' as uuid), 'x');
-- __mo_rowid is regenerated for every table creation. Assert its self-comparison
-- rather than snapshotting the physical value.
select greatest(__mo_rowid, __mo_rowid) = __mo_rowid as greatest_rowid_same_oid from greatest_least_supported_oid;
select greatest(__mo_rowid, 1) from greatest_least_supported_oid;
select greatest(v32, v32) as greatest_vecf32_same_oid from greatest_least_supported_oid;
select greatest(v32, v64) from greatest_least_supported_oid;
select greatest(cast('file://greatest-least-a' as datalink), cast('file://greatest-least-b' as datalink)) as greatest_datalink_same_oid;
select greatest(cast('file://greatest-least-a' as datalink), 'x');

-- Unsupported Oids must fail both for same-Oid and mixed calls rather than
-- reaching the normal executor's unreachable branch. ENUM is normalized to
-- VARCHAR before function resolution, so it follows the string/temporal rules.
select greatest(interval 1 day, interval 2 day);
select greatest(interval 1 day, cast('2020-01-01' as date));
select greatest(st_geomfromtext('POINT(1 1)'), st_geomfromtext('POINT(2 2)'));
select greatest(st_geomfromtext('POINT(1 1)'), 'x');
drop table if exists greatest_least_enum;
create table greatest_least_enum (e enum('a', 'b'), d date);
insert into greatest_least_enum values ('a', '2020-01-01');
select greatest(e, e) from greatest_least_enum;
select greatest(e, 'b') from greatest_least_enum;
select least(e, 'b') from greatest_least_enum;
select greatest(e, d) from greatest_least_enum;

drop table if exists toll_transactions;
drop table if exists mixed_num;
drop table if exists uns;
drop table if exists bt;
drop table if exists greatest_least_supported_oid;
drop table if exists greatest_least_enum;
