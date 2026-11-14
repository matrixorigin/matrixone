-- @suite
-- @case
-- @desc: information_schema publishes observed table rows, data/index sizes, and index cardinality (#30910, #31164, #31752, #31878)
-- @label:bvt

drop database if exists information_schema_stats_publication;
create database information_schema_stats_publication;
use information_schema_stats_publication;

-- populated fixture with a primary and a secondary key, 8 distinct values each
create table stats_pub_t (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_t values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80);
analyze table stats_pub_t;

-- actual query rows remain the semantic control for the published estimates
select count(*) as tbl_rows, count(distinct v) as distinct_v from stats_pub_t;

-- lower-NDV control: 100 rows but only 10 distinct secondary keys, so the
-- secondary cardinality cannot be the table row count
create table stats_pub_rept (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_rept values
    (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),
    (11,1),(12,2),(13,3),(14,4),(15,5),(16,6),(17,7),(18,8),(19,9),(20,10),
    (21,1),(22,2),(23,3),(24,4),(25,5),(26,6),(27,7),(28,8),(29,9),(30,10),
    (31,1),(32,2),(33,3),(34,4),(35,5),(36,6),(37,7),(38,8),(39,9),(40,10),
    (41,1),(42,2),(43,3),(44,4),(45,5),(46,6),(47,7),(48,8),(49,9),(50,10),
    (51,1),(52,2),(53,3),(54,4),(55,5),(56,6),(57,7),(58,8),(59,9),(60,10),
    (61,1),(62,2),(63,3),(64,4),(65,5),(66,6),(67,7),(68,8),(69,9),(70,10),
    (71,1),(72,2),(73,3),(74,4),(75,5),(76,6),(77,7),(78,8),(79,9),(80,10),
    (81,1),(82,2),(83,3),(84,4),(85,5),(86,6),(87,7),(88,8),(89,9),(90,10),
    (91,1),(92,2),(93,3),(94,4),(95,5),(96,6),(97,7),(98,8),(99,9),(100,10);
analyze table stats_pub_rept;

-- composite part 1 and unique single-column indexes report the published
-- distinct-value estimate of their prefix, which is the part column itself
create table stats_pub_multi (
    id int primary key,
    a int,
    b int,
    unique key uq_b (b),
    key ix_ab (a, b)
);
insert into stats_pub_multi values
    (1,1,6),(2,1,7),(3,2,8),(4,2,9),(5,3,10),(6,3,11),(7,3,12),(8,4,13);
analyze table stats_pub_multi;

-- composite-prefix counterexample with repeated second-column values: every
-- (a, b) pair is distinct while the b values repeat (3 distinct), so the
-- second part's prefix estimate is not the part column's own NDV
create table stats_pub_pref (
    id int primary key,
    a int,
    b int,
    key ix_ab (a, b)
);
insert into stats_pub_pref values
    (1,1,10),(2,2,10),(3,3,20),(4,4,20),(5,5,30),(6,6,30);
analyze table stats_pub_pref;

-- the distinct-prefix controls: 6 rows, 6 distinct a, 3 distinct b, 6
-- distinct (a, b) pairs
select count(*) as tbl_rows, count(distinct a) as distinct_a,
       count(distinct b) as distinct_b from stats_pub_pref;

-- empty control: an analyzed empty table publishes the known zero
create table stats_pub_empty (
    id int primary key,
    v int,
    key ix_v (v)
);
analyze table stats_pub_empty;

-- all-NULL key population: the fixtures above have no NULLs, so this
-- control isolates the value group the NULL rows form. MySQL index
-- statistics count the NULLs as exactly one key group, so the analyzed
-- secondary cardinality is 1: neither the table row count nor the false
-- zero the distinct-value population alone would publish
create table stats_pub_nulls (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_nulls values (1,null),(2,null),(3,null);
analyze table stats_pub_nulls;

-- control oracle: 3 rows, no non-NULL value, one NULL value group
select count(*) as nulls_rows, count(distinct v) as nulls_distinct_nonnull,
       count(*) - count(v) as nulls_null_rows
from stats_pub_nulls;

-- mixed population: two distinct non-NULL values plus the NULL group, so
-- the published distinct-value count alone (2) misses exactly one group
create table stats_pub_mixed (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_mixed values (1,10),(2,20),(3,null);
analyze table stats_pub_mixed;

-- control oracle: 3 rows, 2 distinct non-NULL values, 1 NULL row
select count(*) as mixed_rows, count(distinct v) as mixed_distinct_nonnull,
       count(*) - count(v) as mixed_null_rows
from stats_pub_mixed;

-- index cardinality reflects the published distinct-value population
select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_t'
order by index_name, seq_in_index;

select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_rept'
order by index_name, seq_in_index;

select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_multi'
order by index_name, seq_in_index;

-- a composite cardinality part reports the distinct count of the index
-- prefix ending at that part: the first part's prefix is the part column
-- itself (its published estimate), while the engine publishes no
-- multi-column prefix counts, so later parts render NULL instead of
-- substituting the part column's own NDV
select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_pref'
order by index_name, seq_in_index;

select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_empty'
order by index_name, seq_in_index;

-- the NULL population joins the distinct-value estimate as one value
-- group: an all-NULL column renders 1 (ndv 0 plus the NULL group) and the
-- mixed column renders 3 (ndv 2 plus the NULL group); the primary keys
-- keep their own populations
select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_nulls'
order by index_name, seq_in_index;

select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_mixed'
order by index_name, seq_in_index;

-- mixed-case declarations: mo_indexes and mo_columns keep the declared
-- spelling while the statistics publish the canonical lowercased keys, so a
-- mixed-case index part must resolve its estimate; STATISTICS keeps
-- displaying the declared COLUMN_NAME spelling
create table stats_pub_case (
    ID int primary key,
    V int,
    key ix_V (V)
);
insert into stats_pub_case (ID, V) values (1,10),(2,20),(3,30);
analyze table stats_pub_case;

-- controls: 3 rows, 3 distinct values in each declared-case column
select count(*) as case_rows, count(distinct ID) as distinct_id,
       count(distinct V) as distinct_v
from stats_pub_case;

select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_case'
order by index_name, seq_in_index;

-- the estimator resolves the declared spelling to the canonical key itself
select mo_table_col_ndv('information_schema_stats_publication', 'stats_pub_case', 'ID') as ndv_declared_id,
       mo_table_col_ndv('information_schema_stats_publication', 'stats_pub_case', 'V') as ndv_declared_v,
       mo_table_col_ndv('information_schema_stats_publication', 'stats_pub_case', 'v') as ndv_canonical_v;

-- character-prefix key part: KEY ix_prefix_part (v(4)) indexes LEFT(v, 4),
-- whose truncated population has no published estimate, so that key part
-- renders NULL cardinality and never the full-column NDV; the full-column
-- index on the same column keeps its published estimate
create table stats_pub_prefix_ix (
    id int primary key,
    v varchar(16),
    key ix_full_part (v),
    key ix_prefix_part (v(4))
);
insert into stats_pub_prefix_ix (id, v) values
    (1,'abcd0101'),(2,'abcd0202'),(3,'abcd0303'),(4,'abcd0404'),
    (5,'abcd0505'),(6,'abcd0606'),(7,'abcd0707'),(8,'abcd0808');
analyze table stats_pub_prefix_ix;

-- controls: 8 rows, 8 distinct full strings sharing one 4-character prefix
select count(*) as prefix_ix_rows, count(distinct v) as distinct_full_string,
       count(distinct left(v, 4)) as distinct_truncated
from stats_pub_prefix_ix;

select index_name, seq_in_index, column_name, cardinality
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_prefix_ix'
order by index_name, seq_in_index;

-- a functional index part (index ix_f ((v+1))) is the publicly constructible
-- hidden-generated fixture: the engine records the part against a hidden
-- generated backing column, so the STATISTICS row keeps COLUMN_NAME NULL and
-- renders the expression, and the part has no published estimate so
-- CARDINALITY renders NULL, never a false zero
create table stats_pub_fi (
    id int primary key,
    v int,
    index ix_f ((v+1))
);
insert into stats_pub_fi values (1,10),(2,20),(3,30);
analyze table stats_pub_fi;

select index_name, seq_in_index, column_name, cardinality,
       case when expression is null then 'N' else 'Y' end as expr_present
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_fi'
order by index_name, seq_in_index;

-- a visible stored generated column is an ordinary analyzed column: its
-- ordinary index part keeps the published estimate, unlike the hidden
-- functional part above
create table stats_pub_gen (
    id int primary key,
    v int,
    g int generated always as (v + 1) stored,
    key ix_g (g)
);
insert into stats_pub_gen (id, v) values (1,10),(2,20),(3,30);
analyze table stats_pub_gen;

select index_name, seq_in_index, column_name, cardinality,
       case when expression is null then 'N' else 'Y' end as expr_present
from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_gen'
order by index_name, seq_in_index;

-- SHOW INDEX publishes the same per-part estimates as information_schema
-- .STATISTICS (#30910): the first visible part of each index renders the
-- published distinct-value population plus one NULL value group, later
-- composite parts, hidden functional parts and character-prefix parts
-- render NULL, and an analyzed empty table keeps its known zero
show index from stats_pub_t;
show index from stats_pub_multi;
show index from stats_pub_rept;
show index from stats_pub_empty;
show index from stats_pub_nulls;
show index from stats_pub_mixed;
show index from stats_pub_case;
show index from stats_pub_prefix_ix;
show index from stats_pub_fi;
show index from stats_pub_gen;

-- table rows/sizes: force one exact statistics tick so the assertions do not
-- race the background refresh, then restore the default
set mo_table_stats.force_update = yes;

select table_name, table_rows from information_schema.tables
where table_schema = 'information_schema_stats_publication'
  and table_name in ('stats_pub_t', 'stats_pub_rept', 'stats_pub_multi', 'stats_pub_empty')
order by table_name;

select table_name,
       (data_length > 0) as data_positive,
       (index_length > 0) as index_positive,
       (avg_row_length > 0) as avg_row_positive
from information_schema.tables
where table_schema = 'information_schema_stats_publication'
  and table_name in ('stats_pub_t', 'stats_pub_rept', 'stats_pub_multi')
order by table_name;

select table_name, data_length, index_length, avg_row_length, data_free, max_data_length
from information_schema.tables
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_empty';

-- views keep NULL statistics projections in information_schema
create view stats_pub_v as select id, v from stats_pub_t;
select table_rows, data_length, index_length, avg_row_length
from information_schema.tables
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_v';

-- the mo_table_rows/mo_table_size controls and the new index-size publication
-- stay consistent through the same forced statistics read
select mo_table_rows('information_schema_stats_publication', 'stats_pub_t') as forced_rows,
       (mo_table_size('information_schema_stats_publication', 'stats_pub_t') > 0) as forced_size,
       (mo_table_index_size('information_schema_stats_publication', 'stats_pub_t') > 0) as forced_index;

select mo_table_col_ndv('information_schema_stats_publication', 'stats_pub_t', 'v') as ndv_v,
       mo_table_col_ndv('information_schema_stats_publication', 'stats_pub_rept', 'v') as rept_ndv_v,
       mo_table_col_ndv('information_schema_stats_publication', 'stats_pub_empty', 'id') as empty_ndv;

-- mo_table_col_ndv marks an unavailable estimate as NULL, never a false zero
select mo_table_col_ndv('information_schema_stats_publication', 'stats_pub_t', 'missing_column') is null as unknown_column_null;

-- the per-column NULL population accessor publishes the analyzed NULL-row
-- counts: an all-NULL column has its whole population, a no-NULL column
-- publishes the known zero, and an unavailable column renders NULL
select mo_table_col_nullcnt('information_schema_stats_publication', 'stats_pub_nulls', 'v') as nulls_nullcnt,
       mo_table_col_nullcnt('information_schema_stats_publication', 'stats_pub_mixed', 'v') as mixed_nullcnt,
       mo_table_col_nullcnt('information_schema_stats_publication', 'stats_pub_t', 'v') as t_nullcnt;
select mo_table_col_nullcnt('information_schema_stats_publication', 'stats_pub_t', 'missing_column') is null as nullcnt_unavailable_null;

-- SHOW TABLE STATUS publishes the same statistics root the TABLES view
-- reads under the default mode: observed rows and the base-data
-- Data_length, with the hidden-index bytes carried only by Index_length
-- Create_time (column 10, zero-based) is a per-run wall-clock value and is
-- masked from comparison like tenant/pub_sub2.sql masks it
-- @ignore:10
show table status from information_schema_stats_publication like 'stats_pub_t';

-- the default mode keeps SHOW TABLE STATUS and information_schema.TABLES on the
-- same maintained roots the estimating builtins expose: rows on the observed
-- row count, data on the base-data size source and index bytes on the
-- index-size builtin, with the hidden-index bytes positive and the average
-- row length exactly the data spread over the rows
select (table_rows = mo_table_rows('information_schema_stats_publication', 'stats_pub_t')) as default_tables_rows_eq,
       (data_length = mo_table_data_size('information_schema_stats_publication', 'stats_pub_t')) as default_tables_data_eq,
       (index_length = mo_table_index_size('information_schema_stats_publication', 'stats_pub_t')) as default_tables_index_eq,
       (index_length > 0) as default_index_bytes_positive,
       (data_length = avg_row_length * table_rows) as default_avg_tie
from information_schema.tables
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_t';

-- restore the default forced-read setting before the unhappy path
set mo_table_stats.force_update = no;

-- explicit legacy statistics mode keeps the data/index decomposition: the
-- legacy mo_table_size contract reports data plus index bytes, so
-- information_schema.TABLES sources DATA_LENGTH from mo_table_data_size and
-- the index bytes are never counted next to INDEX_LENGTH twice
set mo_table_stats.use_old_impl = yes;

select mo_table_data_size('information_schema_stats_publication', 'stats_pub_t') as legacy_data_bytes,
       mo_table_index_size('information_schema_stats_publication', 'stats_pub_t') as legacy_index_bytes,
       mo_table_size('information_schema_stats_publication', 'stats_pub_t') as legacy_total_bytes;

select mo_table_data_size('information_schema_stats_publication', 'stats_pub_t')
         + mo_table_index_size('information_schema_stats_publication', 'stats_pub_t')
         = mo_table_size('information_schema_stats_publication', 'stats_pub_t') as legacy_decomposition_ok,
       mo_table_data_size('information_schema_stats_publication', 'stats_pub_t')
         < mo_table_size('information_schema_stats_publication', 'stats_pub_t') as legacy_data_below_total;

-- the legacy SHOW TABLE STATUS must agree with information_schema.TABLES:
-- Data_length/Avg_row_length render the base-data bytes while Index_length
-- keeps the hidden-index bytes, and the legacy mo_table_size blend no
-- longer counts the index bytes into Data_length a second time
-- Create_time is a per-run wall-clock value masked like every other
-- SHOW TABLE STATUS golden in this suite (tenant/pub_sub2.sql pattern)
-- @ignore:10
show table status from information_schema_stats_publication like 'stats_pub_t';

select (data_length = mo_table_data_size('information_schema_stats_publication', 'stats_pub_t')) as legacy_tables_data_eq,
       (index_length = mo_table_index_size('information_schema_stats_publication', 'stats_pub_t')) as legacy_tables_index_eq
from information_schema.tables
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_t';

select table_name, table_rows, data_length, index_length, avg_row_length
from information_schema.tables
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_t';

set mo_table_stats.use_old_impl = no;

-- an ANALYZE of a missing table fails and leaves the connection, data, and
-- published statistics intact
analyze table information_schema_stats_publication.stats_pub_missing;
select count(*) as readable_rows, count(distinct v) as readable_distinct from stats_pub_t;
select index_name, cardinality from information_schema.statistics
where table_schema = 'information_schema_stats_publication'
  and table_name = 'stats_pub_t'
order by index_name;
select 'AFTER_MISSING_ANALYZE';

-- a subscription whose publishing table carries a quoted identifier must
-- keep the information_schema.TABLES statistics readable: the shared
-- subscription resolver interpolates the publisher's database and table
-- names into a mo_catalog query, so a quote inside the valid table name
-- must never break the read (the engine accepts quotes in TABLE names but
-- not in DATABASE names, so the publication databases stay plain-named).
-- Publications drop before their publishing databases and accounts: the
-- engine refuses to drop a database that is still being published, and a
-- dropped account does not cascade another account's publications.
drop publication if exists p_qt;
drop publication if exists p_ok;
drop account if exists sp01q;
drop account if exists sp01c;
drop database if exists pubq_oddtbl;
drop database if exists pubq_ok;
create database pubq_oddtbl;
create table pubq_oddtbl.`odd'name` (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into pubq_oddtbl.`odd'name` values (1,10),(2,20),(3,30);
create database pubq_ok;
create table pubq_ok.plainname (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into pubq_ok.plainname values (1,10),(2,20),(3,30);
create table pubq_oddtbl.plain (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into pubq_oddtbl.plain values (1,10),(2,20),(3,30);
-- the publishing account's ANALYZE publishes the index NDV estimates that
-- the subscribers' STATISTICS cardinality later reads; a subscriber-side
-- ANALYZE of a subscription database is unsupported virtual-object setup, so
-- the published estimates must originate here (the two following statements
-- share the quoted table's unpaired quote and pair back-to-back like the
-- create/insert pair above)
analyze table pubq_oddtbl.`odd'name`;
select count(*) as origin_odd_analyzed from pubq_oddtbl.`odd'name`;
analyze table pubq_oddtbl.plain;
analyze table pubq_ok.plainname;
create account sp01q admin_name = 'sub_admin' identified by '111';
create account sp01c admin_name = 'sub_admin' identified by '111';
create publication p_qt database pubq_oddtbl account sp01q;
create publication p_ok database pubq_ok account sp01c;

-- @session:id=1&user=sp01q:sub_admin&password=111
create database s_qt from sys publication p_qt;
set mo_table_stats.force_update = yes;
select table_schema, table_name,
       (table_rows > 0) as subscribed_rows_ok,
       (data_length > 0) as subscribed_data_ok,
       (index_length > 0) as subscribed_index_ok
from information_schema.tables
where table_schema = 's_qt';
-- continuation: the subscriber keeps reading the subscribed data and the
-- TABLES statistics after the projection resolved the quoted identifier.
-- The two views over the quoted table carry its two unpaired quote
-- characters back-to-back, and every later statement stays quote-neutral:
-- the shared mo-tester splitter tracks single- and double-quotes but not
-- backticks, so a lone one-quote identifier would merge all following
-- statements and swallow the session markers and the cleanup.
create database subq_views;
create view subq_views.subscribed_odd_rows as select * from s_qt.`odd'name`;
create view subq_views.odd_v10 as select v from s_qt.`odd'name` where v >= 10;
select count(*) as subscribed_count from subq_views.subscribed_odd_rows;
select count(*) as subscribed_v10_count from subq_views.odd_v10;
select table_name, table_rows, data_length, index_length
from information_schema.tables
where table_schema = 's_qt';
drop database subq_views;
-- the subscriber reads the published index cardinality through the
-- subscription alias after the origin-side ANALYZE: one branch per
-- subscription relabels TABLE_SCHEMA to the alias and the estimate resolves
-- through the alias path, so the multi-table shape stays readable even with
-- a quoted table name in the IN list
select table_name, index_name, cardinality
from information_schema.statistics
where table_schema = 's_qt' and table_name in ('odd''name', 'plain')
order by table_name, index_name;
select index_name, cardinality
from information_schema.statistics
where table_schema = 's_qt' and table_name = 'plain'
order by index_name;
select index_name, cardinality
from information_schema.statistics
where table_schema = 's_qt' and table_name = 'odd''name'
order by index_name;
-- SHOW INDEX resolves the same published estimates through the
-- subscription alias on the unquoted subscribed table
show index from s_qt.plain;
-- @session

-- @session:id=2&user=sp01c:sub_admin&password=111
create database s_ok from sys publication p_ok;
set mo_table_stats.force_update = yes;
select table_schema, table_name,
       (table_rows > 0) as control_rows_ok,
       (data_length > 0) as control_data_ok,
       (index_length > 0) as control_index_ok
from information_schema.tables
where table_schema = 's_ok';
select count(*) as control_count from s_ok.plainname;
-- the plain-named control subscription reads its published cardinality
-- through the same alias path, including a schema-qualified single-table
-- projection
select index_name, cardinality
from information_schema.statistics
where table_schema = 's_ok' and table_name = 'plainname'
order by index_name;
-- the plain-named control subscription reads its published cardinality
-- through the same alias path on the SHOW INDEX surface as well
show index from s_ok.plainname;
-- @session

-- estimating builtins boundary (#30910): the estimators resolve any physical
-- table the session can see and publish its maintained observations, so
-- catalog-internal tables are served rather than rejected; only unresolvable
-- objects (a virtual-catalog view surface like information_schema.TABLES, a
-- missing database/table) raise errors. Columns without a published estimate
-- (a never-analyzed catalog table's column, the synthetic __mo_rowid, an
-- unknown column) render NULL, never a false zero
set mo_table_stats.force_update = yes;
select coalesce(mo_table_rows('mo_catalog','mo_tables'), 0) > 0 as catalog_rows_positive;
select coalesce(mo_table_size('mo_catalog','mo_tables'), 0) > 0 as catalog_size_positive;
select coalesce(mo_table_data_size('mo_catalog','mo_tables'), 0) > 0 as catalog_data_positive;
select coalesce(mo_table_index_size('mo_catalog','mo_tables'), 0) > 0 as catalog_index_positive;
set mo_table_stats.force_update = no;
select mo_table_col_ndv('mo_catalog','mo_tables','relname') is null as catalog_col_ndv_unavailable;
select mo_table_col_ndv('mo_catalog','mo_tables','__mo_rowid') is null as catalog_rowid_ndv_unavailable;
select mo_table_col_ndv('information_schema_stats_publication','stats_pub_t','__mo_rowid') is null as user_rowid_ndv_unavailable;
select mo_table_col_ndv('information_schema_stats_publication','stats_pub_t','no_such_col') is null as unknown_col_ndv_unavailable;

-- tenant isolation (#30910 pre-PR review): the estimator's publication-config
-- argument is a compiler-owned channel, so a tenant without a subscription
-- and a subscriber that names an unpublished object resolve every call
-- under their own identity. A foreign or nonexistent database raises the
-- documented db-not-exist error instead of leaking the publisher's
-- statistics, while the legitimate subscription keeps publishing
create database stats_pub_priv;
create table stats_pub_priv.secret_t (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_priv.secret_t values (1,10),(2,20),(3,30),(4,40);
analyze table stats_pub_priv.secret_t;
-- a second publisher database that no publication covers: subscribers and
-- foreign tenants must not resolve it under any publication-config spelling
create database stats_pub_priv_unpublished;
create table stats_pub_priv_unpublished.hidden_t (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_priv_unpublished.hidden_t values (1,10),(2,20);
create account sp_att admin_name = 'att_admin' identified by '111';
create account sp_leg admin_name = 'sub_admin' identified by '111';
create publication p_priv database stats_pub_priv account sp_leg;
-- publisher-side ground truth the isolation rows below contrast against
select mo_table_col_ndv('stats_pub_priv','secret_t','v') as publisher_ndv_v;

-- @session:id=3&user=sp_att:att_admin&password=111
-- an unpublish-entitled tenant of its own: the attacker's legitimate
-- per-account objects and analyses keep working while config-shaped calls
-- degrade to its own identity
create database att_owned;
create table att_owned.own_t (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into att_owned.own_t values (1,10),(2,20),(3,30);
analyze table att_owned.own_t;
select mo_table_col_ndv('att_owned','own_t','id') as own_ndv_plain,
       mo_table_col_ndv('att_owned','own_t','id','','pubacct=0') as own_ndv_stripped;
-- a config-shaped call over the attacker's own table cannot select foreign
-- statistics: it binds to the same call as the plain form
select mo_table_col_ndv('att_owned','own_t','v') as own_ndv_v_plain,
       mo_table_col_ndv('att_owned','own_t','v','pubacct=0') as own_ndv_v_stripped;
-- an account without a subscription may not read the sys-published database
-- by typing the publication-config argument: the call resolves under the
-- caller identity, so the foreign database does not exist for it
select mo_table_col_ndv('stats_pub_priv','secret_t','v','','pubacct=0') as stolen_ndv_sys;
select mo_table_col_nullcnt('stats_pub_priv','secret_t','v','pubacct=0') as stolen_nullcnt;
select mo_table_col_ndv('stats_pub_priv','secret_t','v','pubacct=0') as stolen_ndv_algoconfig;
select mo_table_col_ndv('stats_pub_priv','secret_t','v','','pubacct=1') as stolen_ndv_guess;
-- the same isolation holds on the view surfaces: without a subscription the
-- foreign schema renders zero rows, never the published statistics
select count(*) as leaked_stats_rows
from information_schema.statistics
where table_schema = 'stats_pub_priv';
select count(*) as leaked_tables_rows
from information_schema.tables
where table_schema = 'stats_pub_priv';
-- @session

-- @session:id=4&user=sp_leg:sub_admin&password=111
-- the legitimate subscription keeps publishing the publisher's analyses
-- through the alias on every surface
create database s_priv from sys publication p_priv;
set mo_table_stats.force_update = yes;
select table_name,
       (table_rows > 0) as leg_rows_ok,
       (data_length > 0) as leg_data_ok,
       (index_length > 0) as leg_index_ok
from information_schema.tables
where table_schema = 's_priv';
select index_name, cardinality
from information_schema.statistics
where table_schema = 's_priv' and table_name = 'secret_t'
order by index_name;
-- a config-shaped call degrades to the caller's own resolution: whichever
-- value the plain alias call publishes, the config-shaped spelling must bind
-- to the same call and cannot redirect the estimate to another account
select mo_table_col_ndv('s_priv','secret_t','v') as leg_ndv_plain,
       mo_table_col_ndv('s_priv','secret_t','v','','pubacct=0') as leg_ndv_stripped;
-- the argument slot a config-shaped literal is typed into cannot matter:
-- spelled as the ndv call's index parameter it must still bind to the plain
-- caller call and publish the same estimate, whatever account it guesses
select mo_table_col_ndv('s_priv','secret_t','v') as leg_ndv_slot_plain,
       mo_table_col_ndv('s_priv','secret_t','v','pubacct=0') as leg_ndv_slot_stripped,
       mo_table_col_ndv('s_priv','secret_t','v','pubacct=999999') as leg_ndv_slot_bigguess;
select mo_table_col_nullcnt('s_priv','secret_t','v') as leg_nullcnt_plain,
       mo_table_col_nullcnt('s_priv','secret_t','v','pubacct=0') as leg_nullcnt_stripped;
-- a subscriber that names an unpublished publisher database resolves it
-- under its own identity too, with and without the config spelling
select mo_table_col_ndv('stats_pub_priv_unpublished','hidden_t','v','pubacct=0') as unpublished_ndv_config_shape;
select mo_table_col_ndv('stats_pub_priv_unpublished','hidden_t','v') as unpublished_ndv_plain;
set mo_table_stats.force_update = no;
-- @session

-- pre-PR review P2: an identifier that legally equals the publication-config
-- marker text is an object name, not a config argument. Outside the bake
-- window the bind-time filter removes config literals only from
-- mo_table_col_ndv's algo-params slot (and the whole trailing argument at
-- the baked arities), so db/table/column spellings survive binding: SHOW
-- INDEX - whose rewrite supplies db/table as string literals - and the
-- information_schema surfaces keep serving objects named 'pubacct=...',
-- and direct estimator calls resolve them as spelled under the caller's
-- own identity
create database stats_pub_styled;
create table stats_pub_styled.`pubacct=0` (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_styled.`pubacct=0` values (1,10),(2,20),(3,30);
analyze table stats_pub_styled.`pubacct=0`;
-- the styled table name is an identity argument in every call below, so
-- each call binds the same plan as the identical call over a plain name,
-- while the algo-params slot keeps its config-strip parity
select mo_table_col_ndv('stats_pub_styled','pubacct=0','v') as styled_tbl_ndv_plain,
       mo_table_col_ndv('stats_pub_styled','pubacct=0','v','pubacct=0') as styled_tbl_ndv_slotcfg,
       mo_table_col_ndv('stats_pub_styled','pubacct=0','v','pubacct=999999') as styled_tbl_ndv_slotguess,
       mo_table_col_nullcnt('stats_pub_styled','pubacct=0','v') as styled_tbl_nullcnt;
-- SHOW INDEX rewrites into estimator calls with the db/table literals in
-- identity positions: the miscompiled invalid-argument regression was
-- reported exactly over this statement shape
show index from stats_pub_styled.`pubacct=0`;
select index_name, column_name, cardinality
from information_schema.statistics
where table_schema = 'stats_pub_styled' and table_name = 'pubacct=0'
order by index_name, column_name;
-- a database legally named 'pubacct=0' exercises the first identity
-- position on the same surfaces
create database `pubacct=0`;
create table `pubacct=0`.styled_t (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into `pubacct=0`.styled_t values (1,10),(2,20),(3,30);
analyze table `pubacct=0`.styled_t;
select mo_table_col_ndv('pubacct=0','styled_t','v') as styled_db_ndv_plain,
       mo_table_col_nullcnt('pubacct=0','styled_t','v') as styled_db_nullcnt_plain;
show index from `pubacct=0`.styled_t;
select count(*) as styled_db_stats_rows
from information_schema.statistics
where table_schema = 'pubacct=0' and table_name = 'styled_t';
-- a column legally named 'pubacct=0' exercises the third identity position
-- through direct estimator calls over a plain-named table
create table stats_pub_styled.plain_t (
    id int primary key,
    `pubacct=0` int,
    key ix_p (`pubacct=0`)
);
insert into stats_pub_styled.plain_t values (1,10),(2,20),(3,30);
analyze table stats_pub_styled.plain_t;
select mo_table_col_nullcnt('stats_pub_styled','plain_t','pubacct=0') as styled_col_nullcnt,
       mo_table_col_ndv('stats_pub_styled','plain_t','pubacct=0') as styled_col_ndv;

-- pre-PR review concurrency regression: volatile direct estimator calls stay
-- runtime expressions, so sibling scopes of one statement - the arms of a
-- UNION of direct estimator calls over two subscription aliases - share one
-- session and may resolve their aliases concurrently. Each branch must
-- resolve its own alias's published estimate; the shared session must never
-- let one branch's subscription resolution clear or read another branch's
-- catalog results. The two aliases publish different populations so a
-- cross-branch value bleed changes the recorded rows.
drop publication if exists stats_pub_union_p1;
drop publication if exists stats_pub_union_p2;
drop account if exists sp_union;
drop database if exists stats_pub_union_u1;
drop database if exists stats_pub_union_u2;
create database stats_pub_union_u1;
create table stats_pub_union_u1.t (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_union_u1.t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6);
create database stats_pub_union_u2;
create table stats_pub_union_u2.t (
    id int primary key,
    v int,
    key ix_v (v)
);
insert into stats_pub_union_u2.t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,null);
-- actual query rows remain the semantic control for the published estimates:
-- plain 6/6 here and 9 rows with 8 distinct values plus one NULL group there
select count(*) as union_u1_rows, count(distinct v) as union_u1_distinct from stats_pub_union_u1.t;
select count(*) as union_u2_rows, count(v) as union_u2_nonnull, count(distinct v) as union_u2_distinct from stats_pub_union_u2.t;
analyze table stats_pub_union_u1.t;
analyze table stats_pub_union_u2.t;
create account sp_union admin_name = 'sub_admin' identified by '111';
create publication stats_pub_union_p1 database stats_pub_union_u1 account sp_union;
create publication stats_pub_union_p2 database stats_pub_union_u2 account sp_union;

-- @session:id=5&user=sp_union:sub_admin&password=111
create database sub_a from sys publication stats_pub_union_p1;
create database sub_b from sys publication stats_pub_union_p2;
set mo_table_stats.force_update = yes;
-- single-branch controls establish each alias's own published estimates
-- before the same resolutions share a statement
select mo_table_col_ndv('sub_a','t','v') as ndv_alias_a;
select mo_table_col_ndv('sub_b','t','v') as ndv_alias_b;
select mo_table_col_nullcnt('sub_a','t','v') as nullcnt_alias_a;
select mo_table_col_nullcnt('sub_b','t','v') as nullcnt_alias_b;
select mo_table_rows('sub_a','t') as rows_alias_a;
select mo_table_rows('sub_b','t') as rows_alias_b;
-- the review's witness shape: one statement resolving two different
-- published populations over sibling scopes of the same shared session
select mo_table_col_ndv('sub_a','t','v') as ndv_union_a
union
select mo_table_col_ndv('sub_b','t','v') as ndv_union_b
order by 1;
select mo_table_col_nullcnt('sub_a','t','v') as nullcnt_union_a
union
select mo_table_col_nullcnt('sub_b','t','v') as nullcnt_union_b
order by 1;
select mo_table_rows('sub_a','t') as rows_union_a
union
select mo_table_rows('sub_b','t') as rows_union_b
order by 1;
-- repeating the identical ndv UNION re-resolves both branches statement
-- after statement
select mo_table_col_ndv('sub_a','t','v') as ndv_union_repeat
union
select mo_table_col_ndv('sub_b','t','v') as ndv_union_repeat
order by 1;
-- the same subscriber session resolves both aliases on the view surface
select index_name, cardinality
from information_schema.statistics
where table_schema = 'sub_a' and table_name = 't'
order by index_name;
select index_name, cardinality
from information_schema.statistics
where table_schema = 'sub_b' and table_name = 't'
order by index_name;
-- subscriber aliases drop before the publications and the publishing
-- databases
drop database if exists sub_a;
drop database if exists sub_b;
-- @session

drop publication if exists stats_pub_union_p1;
drop publication if exists stats_pub_union_p2;
drop account if exists sp_union;
drop database if exists stats_pub_union_u1;
drop database if exists stats_pub_union_u2;

drop publication if exists p_priv;
drop account if exists sp_att;
drop account if exists sp_leg;
drop database if exists stats_pub_priv;
drop database if exists stats_pub_priv_unpublished;
drop database if exists `pubacct=0`;
drop database if exists stats_pub_styled;

drop publication if exists p_qt;
drop publication if exists p_ok;
drop account if exists sp01q;
drop account if exists sp01c;
drop database if exists pubq_oddtbl;
drop database if exists pubq_ok;

drop view stats_pub_v;
drop database information_schema_stats_publication;
