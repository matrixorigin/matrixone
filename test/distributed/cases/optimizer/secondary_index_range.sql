-- BVT: Range queries on secondary indexes (#23451)
-- Verifies that >=, <=, >, <, BETWEEN on secondary indexed columns
-- use index range scans instead of full table scans.

drop database if exists d1;
create database d1;
use d1;

-- Setup: table with int PK, secondary index on int column
drop table if exists t1;
create table t1(id int primary key, val int, name varchar(50));
create index idx_val on t1(val);
insert into t1 select result, result * 10, concat('row', result) from generate_series(1, 100) g;
select mo_ctl('dn', 'flush', 'd1.t1');
select Sleep(1);

-- 1. Paired range: >= and <= (closed interval) -> prefix_between
-- @regex("prefix_between",true)
explain select * from t1 where val >= 50 and val <= 80;
-- @sortkey:0
select * from t1 where val >= 50 and val <= 80 order by id;

-- 2. Paired range: > and < (open interval) -> prefix_in_range
-- @regex("prefix_in_range",true)
explain select * from t1 where val > 50 and val < 80;
-- @sortkey:0
select * from t1 where val > 50 and val < 80 order by id;

-- 3. Paired range: >= and < (half-open) -> prefix_in_range
-- @regex("prefix_in_range",true)
explain select * from t1 where val >= 50 and val < 80;
-- @sortkey:0
select * from t1 where val >= 50 and val < 80 order by id;

-- 4. Paired range: > and <= (half-open) -> prefix_in_range
-- @regex("prefix_in_range",true)
explain select * from t1 where val > 50 and val <= 80;
-- @sortkey:0
select * from t1 where val > 50 and val <= 80 order by id;

-- 5. Single >= on multi-part index (safe)
-- @regex("Index",true)
explain select * from t1 where val >= 900;
-- @sortkey:0
select * from t1 where val >= 900 order by id;

-- 6. Single < on multi-part index (safe)
-- @regex("Index",true)
explain select * from t1 where val < 30;
-- @sortkey:0
select * from t1 where val < 30 order by id;

-- 7. Const-on-left: 50 <= val AND 80 >= val (same as val >= 50 AND val <= 80)
-- @regex("prefix_between",true)
explain select * from t1 where 50 <= val and 80 >= val;
-- @sortkey:0
select * from t1 where 50 <= val and 80 >= val order by id;

-- 8. BETWEEN (already supported, verify still works)
-- @regex("prefix_between",true)
explain select * from t1 where val between 200 and 300;
-- @sortkey:0
select * from t1 where val between 200 and 300 order by id;

-- 9. Correctness: boundary values included/excluded correctly
-- @sortkey:0
select * from t1 where val >= 990 and val <= 1000 order by id;
-- @sortkey:0
select * from t1 where val > 990 and val < 1000 order by id;
-- @sortkey:0
select * from t1 where val >= 1000 and val <= 1000 order by id;

-- 10. Setup: composite secondary index (multi-column)
drop table if exists t2;
create table t2(id int primary key, a int, b int, c varchar(30));
create index idx_ab on t2(a, b);
insert into t2 values(1, 10, 100, 'x');
insert into t2 values(2, 10, 200, 'y');
insert into t2 values(3, 20, 100, 'z');
insert into t2 values(4, 20, 200, 'w');
insert into t2 values(5, 30, 150, 'v');
select mo_ctl('dn', 'flush', 'd1.t2');
select Sleep(1);

-- 11. Range on leading column of composite index
-- @regex("prefix_between",true)
explain select * from t2 where a >= 10 and a <= 20;
-- @sortkey:0
select * from t2 where a >= 10 and a <= 20 order by id;

-- 12. Range with open bounds on composite index
-- @regex("prefix_in_range",true)
explain select * from t2 where a > 10 and a < 30;
-- @sortkey:0
select * from t2 where a > 10 and a < 30 order by id;

-- 13. Setup: table with varchar PK and secondary index
drop table if exists t3;
create table t3(id varchar(30) primary key, city varchar(50), score int);
create index idx_city on t3(city);
insert into t3 values('a', 'Boston', 85);
insert into t3 values('b', 'Chicago', 92);
insert into t3 values('c', 'Denver', 78);
insert into t3 values('d', 'Boston', 90);
insert into t3 values('e', 'Chicago', 88);
insert into t3 values('f', 'Austin', 95);
select mo_ctl('dn', 'flush', 'd1.t3');
select Sleep(1);

-- 14. Range on varchar column: paired bounds
-- @regex("prefix_between",true)
explain select * from t3 where city >= 'Boston' and city <= 'Denver';
-- @sortkey:0
select * from t3 where city >= 'Boston' and city <= 'Denver' order by id;

-- 15. Range on varchar column: open bounds
-- @regex("prefix_in_range",true)
explain select * from t3 where city > 'Austin' and city < 'Denver';
-- @sortkey:0
select * from t3 where city > 'Austin' and city < 'Denver' order by id;

-- 16. Single >= on varchar (safe)
-- @regex("Index",true)
explain select * from t3 where city >= 'Chicago';
-- @sortkey:0
select * from t3 where city >= 'Chicago' order by id;

-- 17. Single < on varchar (safe)
-- @regex("Index",true)
explain select * from t3 where city < 'Chicago';
-- @sortkey:0
select * from t3 where city < 'Chicago' order by id;

-- 18. Setup: table with no explicit PK (uses fake pk)
drop table if exists t4;
create table t4(x int, y int);
create index idx_x on t4(x);
insert into t4 select result, result * 2 from generate_series(1, 50) g;
select mo_ctl('dn', 'flush', 'd1.t4');
select Sleep(1);

-- 19. Range query on table with fake pk
-- @regex("prefix_between",true)
explain select * from t4 where x >= 10 and x <= 20;
-- @sortkey:0
select * from t4 where x >= 10 and x <= 20 order by x;

-- 20. Open interval on table with fake pk
-- @regex("prefix_in_range",true)
explain select * from t4 where x > 45 and x < 50;
-- @sortkey:0
select * from t4 where x > 45 and x < 50 order by x;

-- 21. OR-of-range on multi-part composite index should NOT use unsafe <= / >
-- The guard must recurse into OR arms and reject composite index usage
-- @regex("Table Scan",true)
explain select * from t2 where a <= 10 or a > 25;
-- @sortkey:0
select * from t2 where a <= 10 or a > 25 order by id;

-- 22. AND-of-range with safe ops (>= and <) on composite index SHOULD use index
-- @regex("Index",true)
explain select * from t2 where a >= 10 and a < 25;
-- @sortkey:0
select * from t2 where a >= 10 and a < 25 order by id;

-- 22b. OR-of-range with safe ops (>= and <) on single-part index SHOULD use index
-- @regex("Index",true)
explain select * from t1 where val >= 5 or val < 2;
-- @sortkey:0
select * from t1 where val >= 5 or val < 2 order by id;

-- 23. NULL values in indexed column
drop table if exists t5;
create table t5(id int primary key, val int, key idx_val(val));
insert into t5 values(1, NULL);
insert into t5 values(2, 10);
insert into t5 values(3, NULL);
insert into t5 values(4, 20);
insert into t5 values(5, 30);
select mo_ctl('dn', 'flush', 'd1.t5');
select Sleep(1);

-- NULL should not appear in range results
-- @sortkey:0
select * from t5 where val >= 10 and val <= 30 order by id;
-- @sortkey:0
select * from t5 where val > 10 and val < 30 order by id;
select count(*) from t5 where val is null;

-- 24. Empty range (lower > upper) should return no rows
-- @regex("Index",true)
explain select * from t5 where val >= 100 and val <= 50;
select * from t5 where val >= 100 and val <= 50;
select * from t5 where val > 30 and val < 10;

-- 25. Unique index with open range (exercises in_range on single-part PK)
drop table if exists t6;
create table t6(id int primary key, val int unique key);
insert into t6 select result, result * 5 from generate_series(1, 100) g;
select mo_ctl('dn', 'flush', 'd1.t6');
select Sleep(1);

-- @regex("Index Table Scan",true)
explain select * from t6 where val > 100 and val < 200;
-- @sortkey:0
select * from t6 where val > 100 and val < 200 order by id;
-- @sortkey:0
select * from t6 where val >= 100 and val <= 200 order by id;

-- 26. DATE column with secondary index
drop table if exists t7;
create table t7(id int primary key, d date, key idx_d(d));
insert into t7 values(1, '2024-01-01');
insert into t7 values(2, '2024-03-15');
insert into t7 values(3, '2024-06-30');
insert into t7 values(4, '2024-09-01');
insert into t7 values(5, '2024-12-31');
select mo_ctl('dn', 'flush', 'd1.t7');
select Sleep(1);

-- @regex("Index Table Scan",true)
explain select * from t7 where d >= '2024-03-01' and d <= '2024-09-30';
-- @sortkey:0
select * from t7 where d >= '2024-03-01' and d <= '2024-09-30' order by id;
-- @regex("prefix_in_range",true)
explain select * from t7 where d > '2024-01-01' and d < '2024-12-31';
-- @sortkey:0
select * from t7 where d > '2024-01-01' and d < '2024-12-31' order by id;

-- 27. DECIMAL column with secondary index
drop table if exists t8;
create table t8(id int primary key, price decimal(10,2), key idx_price(price));
insert into t8 values(1, 9.99);
insert into t8 values(2, 19.99);
insert into t8 values(3, 29.99);
insert into t8 values(4, 49.99);
insert into t8 values(5, 99.99);
select mo_ctl('dn', 'flush', 'd1.t8');
select Sleep(1);

-- @regex("prefix_between",true)
explain select * from t8 where price >= 19.99 and price <= 49.99;
-- @sortkey:0
select * from t8 where price >= 19.99 and price <= 49.99 order by id;
-- @regex("prefix_in_range",true)
explain select * from t8 where price > 9.99 and price < 99.99;
-- @sortkey:0
select * from t8 where price > 9.99 and price < 99.99 order by id;

-- 28. Regression #25341: predicates on a covering non-unique composite
-- secondary index with an owner-bound composite PK. The DML lifecycle checks
-- that the hidden index table is maintained after insert, update, delete,
-- replace, and ON DUPLICATE KEY UPDATE.
drop table if exists t9;
create table t9 (
    id varchar(64) not null,
    user_id varchar(64) not null,
    session_id varchar(64) not null,
    status varchar(32) null,
    due datetime(6) null,
    attempts int not null,
    primary key (user_id, session_id, id),
    index idx_ret(status, due, user_id, session_id, id)
);
insert into t9 values
    ('a1', 'u1', 's1', 'active', '2026-07-02 00:00:00.000001', 10),
    ('a2', 'u1', 's2', 'expired', '2026-07-03 00:00:00.000002', 20),
    ('a3', 'u2', 's1', 'expiring', '2026-07-04 00:00:00.000003', 30),
    ('a4', 'u2', 's2', 'active', NULL, 40),
    ('a5', 'u3', 's1', NULL, '2026-07-05 00:00:00.000004', 50);
select mo_ctl('dn', 'flush', 'd1.t9');
select Sleep(1);

-- @regex("Index Table Scan",true)
explain select count(*) from t9 where status = 'active';
-- @regex("prefix_in",true)
explain select count(*) from t9 where status in ('active', 'expiring');
select count(*) as count_eq from t9 where status = 'active';
select count(*) as count_in from t9 where status in ('active', 'expiring');
select count(*) as count_range from t9 where status >= 'active' and status <= 'expired';
select count(*) as count_nullsafe from t9 where status <=> 'active';
-- @sortkey:0
select id, status, status = 'active' as row_eq, status <=> 'active' as row_nullsafe from t9 order by id;
prepare stmt_t9_status_in from 'select count(*) as count_prepare_in from t9 where status in (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
set @t9_active = 'active';
set @t9_expired = 'expired';
set @t9_expiring = 'expiring';
set @t9_missing = 'missing';
set @t9_null = NULL;
execute stmt_t9_status_in using @t9_active,@t9_expired,@t9_expiring,@t9_missing,@t9_missing,@t9_missing,@t9_missing,@t9_missing,@t9_missing,@t9_missing;
deallocate prepare stmt_t9_status_in;
prepare stmt_t9_status_eq_null from 'select count(*) as count_prepare_eq_null from t9 where status = ?';
execute stmt_t9_status_eq_null using @t9_null;
deallocate prepare stmt_t9_status_eq_null;
prepare stmt_t9_status_in_null from 'select count(*) as count_prepare_in_null from t9 where status in (?, ?)';
execute stmt_t9_status_in_null using @t9_null,@t9_missing;
execute stmt_t9_status_in_null using @t9_null,@t9_active;
deallocate prepare stmt_t9_status_in_null;
prepare stmt_t9_status_between_null from 'select count(*) as count_prepare_between_null from t9 where status between ? and ?';
execute stmt_t9_status_between_null using @t9_null,@t9_active;
deallocate prepare stmt_t9_status_between_null;
prepare stmt_t9_status_or_between_null from 'select count(*) as count_prepare_or_between_null from t9 where status between ? and ? or status between ? and ?';
execute stmt_t9_status_or_between_null using @t9_null,@t9_active,@t9_active,@t9_active;
deallocate prepare stmt_t9_status_or_between_null;

update t9 set status = 'active', due = '2026-07-05 00:00:00.000004' where id = 'a2';
select count(*) as count_after_update_into_active from t9 where status = 'active';
select count(*) as count_after_update_out_of_expired from t9 where status = 'expired';

update t9 set status = 'done' where id = 'a1';
select count(*) as count_after_update_out_of_active from t9 where status = 'active';
select count(*) as count_after_update_into_done from t9 where status = 'done';

delete from t9 where id = 'a4';
select count(*) as count_after_delete_active from t9 where status = 'active';

replace into t9 values ('a3', 'u2', 's1', 'active', '2026-07-06 00:00:00.000005', 44);
select count(*) as count_after_replace_active from t9 where status = 'active';
select count(*) as count_after_replace_expiring from t9 where status = 'expiring';

insert into t9 values ('a3', 'u2', 's1', 'closed', '2026-07-07 00:00:00.000006', 55)
    on duplicate key update status = values(status), due = values(due), attempts = values(attempts);
select count(*) as count_after_odku_active from t9 where status = 'active';
select count(*) as count_after_odku_closed from t9 where status = 'closed';
-- @sortkey:0
select id, status, attempts from t9 order by id;

-- 29. Regression #26821: a strict upper bound on a nullable leading
-- secondary-index part must recheck SQL NULL semantics on covering scans.
-- DECIMAL and VARCHAR controls prove that the rule is serialization-generic;
-- the payload query exercises the non-covering index-join/backfill path.
drop table if exists t11;
create table t11 (
    id bigint primary key,
    k_bigint bigint null,
    k_decimal decimal(20,4) null,
    k_varchar varchar(16) null,
    payload varchar(16) not null,
    key idx_bigint(k_bigint),
    key idx_decimal(k_decimal),
    key idx_varchar(k_varchar)
);
insert into t11 values
    (1, NULL, NULL, NULL, 'null'),
    (2, 98, 98.0000, '098', 'above'),
    (3, 95, 95.0000, '095', 'below-95'),
    (4, 96, 96.0000, '096', 'below-96'),
    (5, 97, 97.0000, '097', 'boundary'),
    (6, 1000, 1000.0000, '1000', 'upper');

-- @regex("Index Table Scan",true)
explain select id from t11 where k_bigint < 97;
select id as bigint_auto from t11 where k_bigint < 97 order by id;
select id as bigint_primary from t11 ignore index(idx_bigint) where k_bigint < 97 order by id;
select id as decimal_auto from t11 where k_decimal < 97.0000 order by id;
select id as decimal_primary from t11 ignore index(idx_decimal) where k_decimal < 97.0000 order by id;
select id as varchar_auto from t11 where k_varchar < '097' order by id;
select id as varchar_primary from t11 ignore index(idx_varchar) where k_varchar < '097' order by id;

-- LIMIT without ORDER BY makes the serialized NULL entry the first visible
-- wrong row unless the decoded SQL residual is retained.
select id, k_bigint from t11 force index(idx_bigint) where k_bigint < 97 limit 1;

-- Constant-left normalization reaches the same strict upper-bound rule.
select id as constant_left from t11 where 97 > k_bigint order by id;

-- Prepared bounds already retain a residual; preserve that control.
prepare stmt_t11_lt from 'select id as prepared_bound from t11 where k_bigint < ? order by id';
set @t11_bound = 97;
execute stmt_t11_lt using @t11_bound;
deallocate prepare stmt_t11_lt;

-- @regex("Join Type: INDEX",true)
explain select id, payload from t11 where k_bigint < 97;
select id, payload from t11 where k_bigint < 97 order by id;

-- A paired lower bound excludes NULL before the strict upper bound.
-- @regex("Index",true)
explain select id from t11 where k_bigint >= 95 and k_bigint < 97;
select id as paired_range from t11 where k_bigint >= 95 and k_bigint < 97 order by id;

-- Both arms are safe serialized prefix comparisons, but the strict upper arm
-- still requires the decoded residual to reject NULL.
select id as safe_or from t11 force index(idx_bigint) where k_bigint < 97 or k_bigint >= 1000 order by id;

-- <= and > remain unsafe against a serialized prefix. The unsafe OR arm must
-- also keep the whole predicate on the base scan.
-- @regex("Index",false)
explain select id from t11 where k_bigint <= 97;
select id as inclusive_upper from t11 where k_bigint <= 97 order by id;
-- @regex("Index",false)
explain select id from t11 where k_bigint > 97;
select id as strict_lower from t11 where k_bigint > 97 order by id;
-- @regex("Index",false)
explain select id from t11 where k_bigint < 97 or k_bigint > 1000;
select id as unsafe_or from t11 where k_bigint < 97 or k_bigint > 1000 order by id;

-- UPDATE must select exactly the same rows and maintain the serialized index.
update t11
set payload = concat(payload, '-matched'), k_bigint = k_bigint + 100
where k_bigint < 97;
select id, k_bigint, payload from t11 order by id;

set @t11_idx = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'idx_bigint'
      and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'd1' and relname = 't11')
    limit 1
);
set @t11_idx_sql = concat(
    'select count(*) as physical_rows, count(distinct __mo_index_pri_col) as primary_mappings from d1.`',
    @t11_idx, '`'
);
prepare stmt_t11_idx from @t11_idx_sql;
execute stmt_t11_idx;
deallocate prepare stmt_t11_idx;
 -- 30. Regression #26801: DECIMAL range bounds must be serialized at the
-- indexed part's scale. The composite PK and direct hidden-table checks keep
-- index maintenance independent from the range-result oracle.
drop table if exists t10;
create table t10 (
    tenant_id int not null,
    id bigint not null,
    price decimal(10,2) not null,
    primary key (tenant_id, id),
    key idx_price_owner (price, tenant_id, id)
);
insert into t10
select 1 + result % 8, result, cast(result / 100.0 as decimal(10,2))
from generate_series(1, 10000) g;

set @t10_idx = (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx_price_owner' limit 1);
set @t10_idx_sql = concat(
    'select count(*) as physical_rows, count(distinct __mo_index_idx_col) as physical_keys, ',
    'count(distinct __mo_index_pri_col) as primary_mappings from d1.`', @t10_idx, '`'
);
prepare stmt_t10_idx from @t10_idx_sql;
execute stmt_t10_idx;
deallocate prepare stmt_t10_idx;

select count(*) as closed_default from t10 where price between 10.250000 and 15.750000;
select count(*) as closed_force from t10 force index(idx_price_owner) where price between 10.250000 and 15.750000;
select count(*) as closed_ignore from t10 ignore index(idx_price_owner) where price between 10.250000 and 15.750000;

select count(*) as open_default from t10 where price > 10.250000 and price < 15.750000;
select count(*) as open_force from t10 force index(idx_price_owner) where price > 10.250000 and price < 15.750000;
select count(*) as open_ignore from t10 ignore index(idx_price_owner) where price > 10.250000 and price < 15.750000;

select count(*) as lower_force from t10 force index(idx_price_owner) where price >= 10.250000;
select count(*) as lower_ignore from t10 ignore index(idx_price_owner) where price >= 10.250000;
select count(*) as upper_force from t10 force index(idx_price_owner) where price < 15.750000;
select count(*) as upper_ignore from t10 ignore index(idx_price_owner) where price < 15.750000;

select count(*) as equality_force from t10 force index(idx_price_owner) where price = 10.250000;
select count(*) as equality_ignore from t10 ignore index(idx_price_owner) where price = 10.250000;
select count(*) as equal_scale_force from t10 force index(idx_price_owner) where price between 10.25 and 15.75;
select count(*) as equal_scale_ignore from t10 ignore index(idx_price_owner) where price between 10.25 and 15.75;
select count(*) as rounding_force from t10 force index(idx_price_owner) where price > 10.255000 and price <= 15.755000;
select count(*) as rounding_ignore from t10 ignore index(idx_price_owner) where price > 10.255000 and price <= 15.755000;
select count(*) as rounding_between_force from t10 force index(idx_price_owner) where price between 10.255000 and 15.755000;
select count(*) as rounding_between_ignore from t10 ignore index(idx_price_owner) where price between 10.255000 and 15.755000;

-- Cleanup
drop database d1;
