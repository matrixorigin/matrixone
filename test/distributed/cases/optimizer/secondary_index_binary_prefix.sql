-- @suite

-- @case
-- @desc: secondary-index binary prefix candidates retain exact SQL semantics
-- @label:bvt

drop database if exists secondary_index_binary_prefix;
create database secondary_index_binary_prefix;
use secondary_index_binary_prefix;

create table t_binary_prefix (
  id int,
  tenant int,
  b varbinary(16),
  b4 varbinary(4),
  fixed4 binary(4),
  fixed8 binary(8),
  amount decimal(12,6),
  status varchar(8),
  code varchar(16),
  primary key(id, tenant),
  key idx_b_amount_pk(b, amount, status, id, tenant),
  key idx_amount_b_pk(amount, b, id, tenant),
  key idx_b4_pk(b4, id, tenant),
  key idx_fixed4_pk(fixed4, id, tenant),
  key idx_fixed8_pk(fixed8, id, tenant)
);

insert into t_binary_prefix values
  (1, 1, unhex(''),       unhex(''),       unhex(''),       unhex(''),       1000.000000, 'a', 'code-1'),
  (2, 1, unhex('00'),     unhex('00'),     unhex('00'),     unhex('00'),     1100.000000, 'a', 'code-2'),
  (3, 1, unhex('0000'),   unhex('0000'),   unhex('0000'),   unhex('0000'),   1200.000000, 'b', 'code-3'),
  (4, 1, unhex('0001'),   unhex('0001'),   unhex('0001'),   unhex('0001'),   1300.000000, 'b', 'code-4'),
  (5, 1, unhex('01'),     unhex('01'),     unhex('01'),     unhex('01'),     1400.000000, 'c', 'code-5'),
  (6, 1, unhex('410042'), unhex('410042'), unhex('410042'), unhex('410042'), 1500.000000, 'c', 'code-6'),
  (7, 1, unhex('41004200'), unhex('41004200'), unhex('41004200'), unhex('41004200'), 1600.000000, 'd', 'code-7'),
  (8, 1, unhex('41'),     unhex('41'),     unhex('41'),     unhex('41'),     1700.000000, 'd', 'code-8'),
  (9, 1, unhex('ff'),     unhex('ff'),     unhex('ff'),     unhex('ff'),     1800.000000, 'e', 'code-9'),
  (10, 1, null, null, null, null, 1900.000000, 'e', 'code-10'),
  (11, 1, unhex('000000'), unhex('000000'), unhex('000000'), unhex('000000'), 2000.000000, 'f', 'code-11'),
  (12, 1, unhex('420043'), unhex('420043'), unhex('420043'), unhex('420043'), 2100.000000, 'f', 'code-12');

-- The prefix access condition remains visible, but covering plans must also
-- retain a typed serial_extract residual for exact binary comparison.
-- @separator:table
-- @regex("prefix_eq.*serial_extract",true)
explain select count(*) from t_binary_prefix force index(idx_b_amount_pk) where b = unhex('');

select count(*) as force_empty, sum(id) as force_empty_sum
from t_binary_prefix force index(idx_b_amount_pk) where b = unhex('');
select count(*) as scan_empty, sum(id) as scan_empty_sum
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk) where b = unhex('');

select count(*) as force_nul, sum(id) as force_nul_sum
from t_binary_prefix force index(idx_b_amount_pk) where b = unhex('00');
select count(*) as scan_nul, sum(id) as scan_nul_sum
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk) where b = unhex('00');

select count(*) as force_embedded_nul, sum(id) as force_embedded_nul_sum
from t_binary_prefix force index(idx_b_amount_pk) where b = unhex('410042');
select count(*) as scan_embedded_nul, sum(id) as scan_embedded_nul_sum
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk) where b = unhex('410042');

select id, hex(b) as b_hex
from t_binary_prefix force index(idx_b_amount_pk)
where b in (unhex(''), unhex('00'), unhex('410042'))
order by id;
select id, hex(b) as b_hex
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b in (unhex(''), unhex('00'), unhex('410042'))
order by id;

-- A closed range with equal byte-string bounds has the same encoded-prefix
-- collision as equality. The access predicate remains useful, but the
-- covering scan must recheck the SQL range predicate.
select count(*) as force_binary_exact_range, sum(id) as force_binary_exact_range_sum
from t_binary_prefix force index(idx_b_amount_pk)
where b between unhex('00') and unhex('00');
select count(*) as scan_binary_exact_range, sum(id) as scan_binary_exact_range_sum
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b between unhex('00') and unhex('00');

-- A point-prefix branch nested below OR is still only a candidate predicate.
-- Keep the whole original disjunction as the exact residual.
select id, hex(b) as b_hex
from t_binary_prefix force index(idx_b_amount_pk)
where b between unhex('01') and unhex('01')
   or b in (unhex(''), unhex('00'))
order by id;
select id, hex(b) as b_hex
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b between unhex('01') and unhex('01')
   or b in (unhex(''), unhex('00'))
order by id;

-- An inclusive prefix upper bound is also only a candidate: the encoded key
-- for 0x41 prefixes 0x410042, but BETWEEN must return only the exact bound.
-- @regex("prefix_between.*serial_extract",true)
explain select id, hex(b) as b_hex
from t_binary_prefix force index(idx_b_amount_pk)
where b between unhex('41') and unhex('41');
select id, hex(b) as b_hex
from t_binary_prefix force index(idx_b_amount_pk)
where b between unhex('41') and unhex('41')
order by id;
select id, hex(b) as b_hex
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b between unhex('41') and unhex('41')
order by id;

-- An open lower prefix bound has the opposite failure mode: PrefixCompare
-- treats a longer value as equal to the shorter bound and would under-fetch it.
-- Referencing code forces the index-join path, whose candidate range must be
-- widened before the base-table predicate performs the exact recheck.
-- @regex("prefix_in_range",true)
explain select id, hex(b) as b_hex, code
from t_binary_prefix force index(idx_b_amount_pk)
where b > unhex('41') and b <= unhex('410042');
select id, hex(b) as b_hex, code
from t_binary_prefix force index(idx_b_amount_pk)
where b > unhex('41') and b <= unhex('410042')
order by id;
select id, hex(b) as b_hex, code
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b > unhex('41') and b <= unhex('410042')
order by id;

-- A lower-inclusive/upper-open range is an exact candidate control and must
-- retain its selective index range.
select count(*) as force_binary_range
from t_binary_prefix force index(idx_b_amount_pk)
where b >= unhex('00') and b < unhex('01');
select count(*) as scan_binary_range
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b >= unhex('00') and b < unhex('01');

-- Decimal-leading and binary-leading index orders are independent controls.
select count(*) as force_amount_range
from t_binary_prefix force index(idx_amount_b_pk)
where amount between 1000.000000 and 1800.000000;
select count(*) as scan_amount_range
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where amount between 1000.000000 and 1800.000000;

select count(*) as force_binary_then_amount
from t_binary_prefix force index(idx_b_amount_pk)
where b in (unhex(''), unhex('00'))
  and amount between 1000.000000 and 1800.000000;
select count(*) as scan_binary_then_amount
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b in (unhex(''), unhex('00'))
  and amount between 1000.000000 and 1800.000000;
select count(*) as default_binary_then_amount
from t_binary_prefix
where b in (unhex(''), unhex('00'))
  and amount between 1000.000000 and 1800.000000;

-- Referencing a non-index column forces base-table backfill; the base scan
-- remains the independent exact oracle.
select count(code) as force_backfill
from t_binary_prefix force index(idx_b_amount_pk) where b = unhex('');
select count(code) as scan_backfill
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk) where b = unhex('');

select count(*) as literal_null_eq
from t_binary_prefix force index(idx_b_amount_pk) where b = null;
select count(*) as literal_null_in
from t_binary_prefix force index(idx_b_amount_pk) where b in (unhex(''), null);

prepare binary_eq from
  'select count(*) as prepared_eq from t_binary_prefix force index(idx_b_amount_pk) where b = ?';
set @binary_value = unhex('');
execute binary_eq using @binary_value;
set @binary_value = unhex('00');
execute binary_eq using @binary_value;
set @binary_value = null;
execute binary_eq using @binary_value;
deallocate prepare binary_eq;

prepare binary_eq_amount from
  'select count(*) as prepared_eq_amount from t_binary_prefix force index(idx_b_amount_pk) where b = ? and amount < ?';
set @binary_value = unhex('');
set @amount_bound = 1800.000000;
execute binary_eq_amount using @binary_value, @amount_bound;
set @binary_value = unhex('00');
execute binary_eq_amount using @binary_value, @amount_bound;
set @binary_value = null;
execute binary_eq_amount using @binary_value, @amount_bound;
deallocate prepare binary_eq_amount;

prepare binary_in from
  'select count(*) as prepared_in from t_binary_prefix force index(idx_b_amount_pk) where b in (?, ?)';
set @binary_value_1 = unhex('');
set @binary_value_2 = null;
execute binary_in using @binary_value_1, @binary_value_2;
deallocate prepare binary_in;

-- Width and fixed/variable representation controls.
select count(*) as varbinary4_empty
from t_binary_prefix force index(idx_b4_pk) where b4 = unhex('');
select count(*) as binary4_zero
from t_binary_prefix force index(idx_fixed4_pk)
where fixed4 = cast(unhex('') as binary(4));
select count(*) as binary8_zero
from t_binary_prefix force index(idx_fixed8_pk)
where fixed8 = cast(unhex('') as binary(8));
select id, hex(fixed4) as fixed4_hex
from t_binary_prefix force index(idx_fixed4_pk)
where fixed4 = cast(unhex('410042') as binary(4))
order by id;
select id, hex(fixed4) as fixed4_hex
from t_binary_prefix ignore index(primary, idx_fixed4_pk)
where fixed4 = cast(unhex('410042') as binary(4))
order by id;

set @idx_table = (select distinct index_table_name from mo_catalog.mo_indexes
  where name = 'idx_b_amount_pk'
    and table_id in (select rel_id from mo_catalog.mo_tables
      where reldatabase = database() and relname = 't_binary_prefix')
  limit 1);
set @physical_sql = concat('select count(*) as physical_rows, ',
  'count(distinct __mo_index_idx_col) as distinct_keys, ',
  'count(distinct __mo_index_pri_col) as distinct_pks from `', @idx_table, '`');
prepare physical_check from @physical_sql;
execute physical_check;
deallocate prepare physical_check;

set @idx_table = (select distinct index_table_name from mo_catalog.mo_indexes
  where name = 'idx_amount_b_pk'
    and table_id in (select rel_id from mo_catalog.mo_tables
      where reldatabase = database() and relname = 't_binary_prefix')
  limit 1);
set @physical_sql = concat('select count(*) as physical_rows, ',
  'count(distinct __mo_index_idx_col) as distinct_keys, ',
  'count(distinct __mo_index_pri_col) as distinct_pks from `', @idx_table, '`');
prepare physical_check from @physical_sql;
execute physical_check;
deallocate prepare physical_check;

set @idx_table = (select distinct index_table_name from mo_catalog.mo_indexes
  where name = 'idx_fixed4_pk'
    and table_id in (select rel_id from mo_catalog.mo_tables
      where reldatabase = database() and relname = 't_binary_prefix')
  limit 1);
set @physical_sql = concat('select count(*) as physical_rows, ',
  'count(distinct __mo_index_idx_col) as distinct_keys, ',
  'count(distinct __mo_index_pri_col) as distinct_pks from `', @idx_table, '`');
prepare physical_check from @physical_sql;
execute physical_check;
deallocate prepare physical_check;

-- Indexed UPDATE changes binary length and leading/embedded NUL bytes without
-- changing hidden-table cardinality or point semantics.
update t_binary_prefix
set b = unhex('00000000'), b4 = unhex('0000'),
    fixed4 = unhex('0001'), fixed8 = unhex('0001'), amount = 1750.000000
where id = 8 and tenant = 1;
update t_binary_prefix
set b = unhex('41'), b4 = unhex('41'),
    fixed4 = unhex('41'), fixed8 = unhex('41')
where id = 6 and tenant = 1;

select id, hex(b) as b_hex
from t_binary_prefix force index(idx_b_amount_pk)
where b in (unhex(''), unhex('00'), unhex('41'), unhex('00000000'))
order by id;
select id, hex(b) as b_hex
from t_binary_prefix ignore index(primary, idx_b_amount_pk, idx_amount_b_pk)
where b in (unhex(''), unhex('00'), unhex('41'), unhex('00000000'))
order by id;

set @idx_table = (select distinct index_table_name from mo_catalog.mo_indexes
  where name = 'idx_b_amount_pk'
    and table_id in (select rel_id from mo_catalog.mo_tables
      where reldatabase = database() and relname = 't_binary_prefix')
  limit 1);
set @physical_sql = concat('select count(*) as physical_rows, ',
  'count(distinct __mo_index_idx_col) as distinct_keys, ',
  'count(distinct __mo_index_pri_col) as distinct_pks from `', @idx_table, '`');
prepare physical_check from @physical_sql;
execute physical_check;
deallocate prepare physical_check;

set @idx_table = (select distinct index_table_name from mo_catalog.mo_indexes
  where name = 'idx_amount_b_pk'
    and table_id in (select rel_id from mo_catalog.mo_tables
      where reldatabase = database() and relname = 't_binary_prefix')
  limit 1);
set @physical_sql = concat('select count(*) as physical_rows, ',
  'count(distinct __mo_index_idx_col) as distinct_keys, ',
  'count(distinct __mo_index_pri_col) as distinct_pks from `', @idx_table, '`');
prepare physical_check from @physical_sql;
execute physical_check;
deallocate prepare physical_check;

set @idx_table = (select distinct index_table_name from mo_catalog.mo_indexes
  where name = 'idx_fixed4_pk'
    and table_id in (select rel_id from mo_catalog.mo_tables
      where reldatabase = database() and relname = 't_binary_prefix')
  limit 1);
set @physical_sql = concat('select count(*) as physical_rows, ',
  'count(distinct __mo_index_idx_col) as distinct_keys, ',
  'count(distinct __mo_index_pri_col) as distinct_pks from `', @idx_table, '`');
prepare physical_check from @physical_sql;
execute physical_check;
deallocate prepare physical_check;

-- CHAR/VARCHAR use the same zero-escaped tuple encoding.  The encoded value
-- for 'a' is a byte prefix of the encoding for 'a\0', so these point lookups
-- require the same typed residual even though the source column is textual.
create table t_varchar_prefix (
  id int primary key,
  v varchar(16),
  key idx_v_id(v, id)
);
create table t_varchar_prefix_scan (
  id int primary key,
  v varchar(16)
);
insert into t_varchar_prefix values
  (1, cast(unhex('61') as varchar(16))),
  (2, cast(unhex('6100') as varchar(16))),
  (3, cast(unhex('62') as varchar(16))),
  (4, null);
insert into t_varchar_prefix_scan select id, v from t_varchar_prefix;

select count(*) as force_varchar_exact
from t_varchar_prefix force index(idx_v_id)
where v = cast(unhex('61') as varchar(16));
select count(*) as scan_varchar_exact
from t_varchar_prefix_scan
where v = cast(unhex('61') as varchar(16));

select count(*) as force_varchar_in
from t_varchar_prefix force index(idx_v_id)
where v in (cast(unhex('61') as varchar(16)), cast(unhex('62') as varchar(16)));
select count(*) as scan_varchar_in
from t_varchar_prefix_scan
where v in (cast(unhex('61') as varchar(16)), cast(unhex('62') as varchar(16)));

drop database secondary_index_binary_prefix;
