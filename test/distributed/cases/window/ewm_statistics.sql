-- ewm window statistics (issue #29945): the five entry names, the
-- canonical alpha=0.5/adjust=false profile with the issue's self-join
-- control, tie/partition semantics, NULL clock policies, minPeriods
-- gating, event-time half-life decay across the ORDER BY key domains,
-- covariance and correlation, the per-execution configuration boundary,
-- and the §7 bind-time rejection rows.
-- The expected values are the frozen ewm oracle corpus rows (the named
-- cases of pkg/sql/colexec/window/ewm/testdata/oracle_vectors.json),
-- hand-checked in review.

drop database if exists ewm_stat_test;
create database ewm_stat_test;
use ewm_stat_test;

-- the ewm names remain ordinary identifiers
create table ewmmean (ewmStd int);
insert into ewmmean values (7);
select ewmStd from ewmmean;
drop table ewmmean;

create table s (id bigint primary key, x double, y double);
insert into s values (1, 1, null),(2, 2, null),(3, 3, null),(4, 4, null);

-- profile_adjF: the issue's canonical alpha=0.5 adjust=false profile
select id, ewmmean(x, 0.5, false) over (order by id) from s order by id;
select id, ewmvar(x, 0.5, false) over (order by id) from s order by id;
select id, ewmstd(x, 0.5, false) over (order by id) from s order by id;
-- profile_adjT and profile_alpha1_adjF
select id, ewmmean(x, 0.5, true) over (order by id) from s order by id;
select id, ewmmean(x, 1, false) over (order by id) from s order by id;
select id, ewmvar(x, 1, false) over (order by id) from s order by id;
-- minPeriods=2 gates the single-observation prefix to NULL
select id, ewmmean(x, 0.5, false, false, 2) over (order by id) from s order by id;

-- the ordinary window aggregates are unchanged beside the new names
select id, avg(x) over (order by id) from s order by id;
select id, cast(var_pop(x) over (order by id) as decimal(18,6)) from s order by id;
select id, cast(stddev_pop(x) over (order by id) as decimal(18,6)) from s order by id;

-- the issue's working control, kept verbatim; its outputs cross-assert
-- the adjust=false profile above
WITH raw AS (SELECT *
FROM (
  SELECT 1 id,CAST(1 AS DOUBLE) v
  UNION ALL SELECT 2,2
  UNION ALL SELECT 3,3
  UNION ALL SELECT 4,4) fixture) SELECT a.id,SUM(b.v*CASE WHEN b.id=1 THEN POWER(CAST(0.5 AS DOUBLE),a.id-1) ELSE CAST(0.5 AS DOUBLE)*POWER(CAST(0.5 AS DOUBLE),a.id-b.id) END) result FROM raw a JOIN raw b ON b.id<=a.id GROUP BY a.id ORDER BY a.id;

-- §7: the issue's entry probe (no OVER clause) and the bind rows
select ewmmean(cast(1 as double),3) result;
select ewmmean(x) over (order by id) from s order by id;
select ewmmean(x, 0.5, false) over (partition by id) from s order by id;
select ewmmean(x, 0.5, false) over (order by id rows between 1 preceding and current row) from s order by id;
select ewmmean(x, 3, false) over (order by id) from s order by id;
select ewmmean(x, 0, false) over (order by id) from s order by id;
-- a decimal alpha literal is fenced by the base's decimal-literal
-- protocol and surfaces the range error at the execution boundary
select ewmmean(x, 1.5, false) over (order by id) from s order by id;
select ewmmean(x, 'halflife') over (order by id) from s order by id;
select ewmmean(x, 'halflife', 0) over (order by id) from s order by id;
select ewmmean(x, 'span') over (order by id) from s order by id;
select ewmmean(x, 'halflife', 10, true) over (order by id desc) from s order by id;
select ewmmean(x, 'halflife', 10, true) over (order by id, x) from s order by id;
select ewmmean(x, y) over (order by id) from s order by id;

-- the event-time ORDER BY key type rule
create table named (id bigint primary key, nm varchar(8), x double);
insert into named values (1, 'a', 1),(2, 'b', 2);
select id, ewmmean(x, 'halflife', 10, true) over (order by nm) from named order by id;
drop table named;

-- non-constant policy (a decaying argument is a column here)
create table pol (id bigint primary key, k int, x double);
insert into pol values (1, 1, 1),(2, 1, 2);
select id, ewmmean(x, 0.5, k > 1) over (order by k) from pol order by id;
drop table pol;

-- constant-expression configs (review 22): §4 admits constant
-- expressions at every decay/policy position, and binding lowers IFNULL
-- to CASE, so a statement-constant CASE/IFNULL must publish exactly like
-- its literal equivalent, while a column-dependent CASE keeps the §7
-- rejection
select id, ewmmean(x, 0.5, case when true then false else true end) over (order by id) from s order by id;
select id, ewmmean(x, case when 1 = 1 then 0.5 else 0.25 end) over (order by id) from s order by id;
select id, ewmmean(x, 0.5, ifnull(null, false)) over (order by id) from s order by id;
select id, ewmvar(x, 0.5, ifnull(null, false)) over (order by id) from s order by id;
select id, ewmmean(x, case when true then 0.5 else 0.25 end, case when false then true else false end) over (order by id) from s order by id;
select id, ewmmean(x, 0.5, case when id > 2 then true else false end) over (order by id) from s order by id;
select id, ewmmean(x, case when id > 2 then 0.5 else 0.25 end) over (order by id) from s order by id;
select id, ewmmean(x, 0.5, ifnull(null, id > 2)) over (order by id) from s order by id;
select 1 as recover_case;

-- ties_keys122: a tied batch shares one group-post value at every row
create table t122 (id bigint primary key, k bigint, x double);
insert into t122 values (1, 1, 0),(2, 2, 10),(3, 2, 20),(4, 3, 30);
select id, ewmmean(x, 0.5, true) over (order by k) from t122 order by id;
select id, ewmvar(x, 0.5, true) over (order by k) from t122 order by id;
select id, ewmstd(x, 0.5, true) over (order by k) from t122 order by id;
select id, ewmmean(x, 0.5, false) over (order by k) from t122 order by id;
select id, ewmvar(x, 0.5, false) over (order by k) from t122 order by id;
select id, ewmstd(x, 0.5, false) over (order by k) from t122 order by id;

-- tie_pair: covariance and correlation across the tied batch (adjT and adjF)
create table tp (id bigint primary key, k bigint, x double, y double);
insert into tp values (1, 1, 10, 10),(2, 2, 20, -20),(3, 2, 40, -40);
select id, ewmcov(x, y, 0.5, true) over (order by k) from tp order by id;
select id, ewmcorr(x, y, 0.5, true) over (order by k) from tp order by id;
select id, ewmmean(x, 0.5, true) over (order by k) from tp order by id;
select id, ewmcov(x, y, 0.5, false) over (order by k) from tp order by id;
select id, ewmcorr(x, y, 0.5, false) over (order by k) from tp order by id;

-- unit_control_[0,2,6]_std: one within-instant scatter cluster
create table instant (id bigint primary key, k int, x double, y double);
insert into instant values (1, 1, 0, 0),(2, 1, 2, 2),(3, 1, 6, 6);
select id, ewmmean(x, 0.5, true) over (order by k) from instant order by id;
select id, ewmstd(x, 0.5, true) over (order by k) from instant order by id;
select id, ewmcorr(x, y, 0.5, true) over (order by k) from instant order by id;

-- clocks_[0,NULL,NULL,3]: all four adjust/ignoreNA stacks at the gap rows
create table clk (id bigint primary key, k bigint, x double, y double);
insert into clk values (1, 1, 0, 0),(2, 2, null, null),(3, 2, null, null),(4, 3, 3, 3);
select id, ewmmean(x, 0.5, true, true) over (order by k) from clk order by id;
select id, ewmvar(x, 0.5, true, true) over (order by k) from clk order by id;
select id, ewmcorr(x, y, 0.5, true, true) over (order by k) from clk order by id;
select id, ewmmean(x, 0.5, true, false) over (order by k) from clk order by id;
select id, ewmvar(x, 0.5, true, false) over (order by k) from clk order by id;
select id, ewmmean(x, 0.5, false, true) over (order by k) from clk order by id;
select id, ewmvar(x, 0.5, false, true) over (order by k) from clk order by id;
select id, ewmmean(x, 0.5, false, false) over (order by k) from clk order by id;
select id, ewmvar(x, 0.5, false, false) over (order by k) from clk order by id;
drop table clk;

-- p3_continuation: ignoreNA=false carries the gap-normalized state
create table p3 (id bigint primary key, x double);
insert into p3 values (1, 1),(2, null),(3, 3),(4, 4);
select id, ewmmean(x, 0.5, false, false) over (order by id) from p3 order by id;
select id, ewmvar(x, 0.5, false, false) over (order by id) from p3 order by id;
select id, ewmstd(x, 0.5, false, false) over (order by id) from p3 order by id;
select id, ewmmean(x, 0.5, true, false) over (order by id) from p3 order by id;
select id, ewmvar(x, 0.5, true, false) over (order by id) from p3 order by id;

-- all-NULL partition: NULL everywhere (all_null_3)
create table nul (id bigint primary key, x double);
insert into nul values (1, null),(2, null),(3, null);
select id, ewmmean(x, 0.5, false) over (order by id) from nul order by id;
select id, ewmvar(x, 0.5, false) over (order by id) from nul order by id;
select id, ewmstd(x, 0.5, false) over (order by id) from nul order by id;
drop table nul;

-- partitions reset the account state
create table parts (id bigint primary key, p int, x double);
insert into parts values (1, 1, 1),(2, 1, 2),(3, 2, 3),(4, 2, 4);
select id, p, ewmmean(x, 0.5, false) over (partition by p order by id) from parts order by id;
select id, p, ewmvar(x, 0.5, false) over (partition by p order by id) from parts order by id;

-- const_[10,NULL,10] family (§10 Invariant 4): the constant signal is
-- preserved through the gap under every adjust/ignoreNA stack and under
-- alpha=1 (values stay 10 at every row; the variance and std stay 0)
create table cst (id bigint primary key, x double);
insert into cst values (1, 10),(2, null),(3, 10);
select id, ewmmean(x, 0.5, true, false) over (order by id) from cst order by id;
select id, ewmmean(x, 0.5, true, true) over (order by id) from cst order by id;
select id, ewmmean(x, 0.5, false, false) over (order by id) from cst order by id;
select id, ewmmean(x, 0.5, false, true) over (order by id) from cst order by id;
select id, ewmvar(x, 0.5, true, false) over (order by id) from cst order by id;
select id, ewmstd(x, 0.5, false, false) over (order by id) from cst order by id;
select id, ewmmean(x, 1, true, false) over (order by id) from cst order by id;
select id, ewmmean(x, 1, false, false) over (order by id) from cst order by id;

-- const_ties_keys122(_adjT) and zero-variance corr NULL: [10,10,10] on the
-- tied keys [1,2,2]
create table cst2 (id bigint primary key, k bigint, x double, y double);
insert into cst2 values (1, 1, 10, 10),(2, 2, 10, 10),(3, 2, 10, 10);
select id, ewmmean(x, 0.5, false) over (order by k) from cst2 order by id;
select id, ewmvar(x, 0.5, false) over (order by k) from cst2 order by id;
select id, ewmstd(x, 0.5, false) over (order by k) from cst2 order by id;
select id, ewmcorr(x, y, 0.5, false) over (order by k) from cst2 order by id;
select id, ewmmean(x, 0.5, true) over (order by k) from cst2 order by id;
drop table cst;
drop table cst2;

-- prepared decay: the configuration is rebuilt per EXECUTE, so one
-- out-of-range execution must not poison the reusable plan
set @ewm_alpha = 0.5;
prepare mm from 'select id, ewmmean(x, ?, false) over (order by id) from ewm_stat_test.s order by id';
execute mm using @ewm_alpha;
set @ewm_alpha = 3.0;
execute mm using @ewm_alpha;
set @ewm_alpha = 0.5;
execute mm using @ewm_alpha;
deallocate prepare mm;

-- §10 prepared stability: the design's mixed decay form ABS(?) with
-- policy markers; α=-1.0 -> ABS=1.0 is a different valid α reusing the
-- same prepared statement with unchanged parameter types, α=3.0 ->
-- ABS=3.0 errors on that execution alone, and the final EXECUTE
-- recovers. The literal twin of the recovering configuration pins the
-- values (minPeriods=2 gates the single-observation prefix to NULL).
set @ewm_decay = 0.5;
set @ewm_adjust = true;
set @ewm_igna = false;
set @ewm_minp = 2;
prepare ma from 'select id, ewmmean(x, abs(?), ?, ?, ?) over (order by id) from ewm_stat_test.s order by id';
execute ma using @ewm_decay, @ewm_adjust, @ewm_igna, @ewm_minp;
set @ewm_decay = -1.0;
execute ma using @ewm_decay, @ewm_adjust, @ewm_igna, @ewm_minp;
set @ewm_decay = 3.0;
execute ma using @ewm_decay, @ewm_adjust, @ewm_igna, @ewm_minp;
set @ewm_decay = 0.5;
execute ma using @ewm_decay, @ewm_adjust, @ewm_igna, @ewm_minp;
select id, ewmmean(x, 0.5, true, false, 2) over (order by id) from ewm_stat_test.s order by id;
deallocate prepare ma;

-- §7/§10: a prepared half-life must be strictly positive; h=0 fails
-- that EXECUTE alone and the plan serves the valid EXECUTEs around it.
-- The literal twin of the valid execution pins the values.
set @ewm_h = 3600;
prepare mh from 'select id, ewmmean(x, ''halflife'', ?, false) over (order by id) from ewm_stat_test.s order by id';
execute mh using @ewm_h;
set @ewm_h = 0;
execute mh using @ewm_h;
set @ewm_h = 3600;
execute mh using @ewm_h;
select id, ewmmean(x, 'halflife', 3600, false) over (order by id) from ewm_stat_test.s order by id;
deallocate prepare mh;

-- alpha_quarter (second review round): the retention/arrival taxonomy is
-- retention 1-alpha with a fresh arrival share alpha — adjust=false
-- runs y_t = (1-alpha)y_(t-1) + alpha*x_t (1, 1.25, 1.6875, 2.265625)
-- and adjust=true mass-normalizes the (1-alpha)^(t-i) weights
-- (1, 11/7 = 1.5714285714285714, 81/37 = 2.1891891891891893,
--  499/175 = 2.8514285714285716)
select id, ewmmean(x, 0.25, false) over (order by id) from s order by id;
select id, ewmmean(x, 0.25, true) over (order by id) from s order by id;

-- multikey_full_tuple (second review round): ORDER BY a,b makes the peer
-- groups the full-tuple groups — (1,1) ties once, (2,1) is the later
-- group, and the values publish [22.5, 15, 15] in id order
-- (engine-verified [15, 15, 22.5] in group order)
create table mk (id bigint primary key, a int, b int, x double);
insert into mk values (1, 2, 1, 30),(2, 1, 1, 10),(3, 1, 1, 20);
select id, ewmmean(x, 0.5, true) over (order by a, b) from mk order by id;
drop table mk;

-- row-decay ORDER BY latitude (second review round): varchar keys order
-- and tie exactly like numeric keys in both directions, and NULL keys
-- order under the framework's nulls-first rule
create table lat (id bigint primary key, k varchar(8), x double);
insert into lat values (1, 'bee', 10),(2, 'ant', 20),(3, 'ant', 30);
select id, ewmmean(x, 0.5, true) over (order by k desc) from lat order by id;
select id, ewmmean(x, 0.5, true) over (order by k) from lat order by id;
drop table lat;
create table latn (id bigint primary key, k varchar(8), x double);
insert into latn values (1, null, 10),(2, 'x', 20),(3, 'x', 30);
select id, ewmmean(x, 0.5, true) over (order by k) from latn order by id;
drop table latn;

-- constant vectors (second review round): a constant sample expression
-- carries one physical element that every row reads (7,7,7,7), and a
-- constant ORDER BY key makes the whole partition one arrival batch
-- whose unadjusted publish is the plain batch mean (2.5 at every row)
select id, ewmmean(cast(7.0 as double), 0.5, false) over (order by id) from s order by id;
select id, ewmmean(x, 0.5, false) over (order by cast(9 as char)) from s order by id;

-- prepared marker decay latitude (second review round): an unresolved
-- decay marker defers its mode to the per-execution configuration
-- boundary, so PREPARE keeps the row-mode ORDER BY latitude — a DESC,
-- a multi-key and a single-key ORDER BY all serve valid EXECUTEs, and
-- an EXECUTE that resolves to the 'halflife' tag under DESC meets the
-- execution-boundary event-time ORDER BY rejection (the h=0 EXECUTE
-- shows the config validation running first)
set @ewm_a = 0.5;
prepare sd from 'select id, ewmmean(x, ?, false) over (order by id desc) from ewm_stat_test.s order by id';
execute sd using @ewm_a;
deallocate prepare sd;

prepare sm from 'select id, ewmmean(x, ?) over (order by id, x) from ewm_stat_test.s order by id';
execute sm using @ewm_a;
deallocate prepare sm;

set @ewm_tag = 'halflife';
set @ewm_hp = 10;
prepare sh from 'select id, ewmmean(x, ?, ?, false) over (order by id desc) from ewm_stat_test.s order by id';
execute sh using @ewm_tag, @ewm_hp;
set @ewm_hp = 0;
execute sh using @ewm_tag, @ewm_hp;
deallocate prepare sh;

drop table s;
drop table t122;
drop table tp;
drop table instant;
drop table p3;
drop table parts;
drop database ewm_stat_test;

-- event-time decay ('halflife'): one dedicated database
create database ewm_stat_time;
use ewm_stat_time;

-- evh_bigint_neighbors: h=1, BIGINT neighbor keys
create table ev (t bigint primary key, x double, y double);
insert into ev values (0, 0, 0),(1, 5, 5);
select t, ewmmean(x, 'halflife', 1, true) over (order by t) from ev order by t;
select t, ewmvar(x, 'halflife', 1, true) over (order by t) from ev order by t;
select t, ewmstd(x, 'halflife', 1, true) over (order by t) from ev order by t;

-- evh_us_subsecond: h=1000000 and a quarter-second int64 key gap
delete from ev;
insert into ev values (0, 1, 1),(250000, 2, 2);
select t, ewmmean(x, 'halflife', 1000000, true) over (order by t) from ev order by t;
select t, ewmvar(x, 'halflife', 1000000, true) over (order by t) from ev order by t;

-- evh_minint64_maxint64: the full BIGINT magnitude domain (adjust=false)
delete from ev;
insert into ev values (-9223372036854775808, 0, 0),(9223372036854775807, 10, 10);
select t, ewmmean(x, 'halflife', 1, false) over (order by t) from ev order by t;
select t, ewmcov(x, y, 'halflife', 1, false) over (order by t) from ev order by t;
select t, ewmcorr(x, y, 'halflife', 1, false) over (order by t) from ev order by t;

-- evh_underflow_carry: a 2000-unit gap decays the state fully (adjust=false)
delete from ev;
insert into ev values (0, 2, 2),(2000, 1, 1);
select t, ewmmean(x, 'halflife', 1, false) over (order by t) from ev order by t;
select t, ewmvar(x, 'halflife', 1, false) over (order by t) from ev order by t;

-- evh_decimal_keys: DECIMAL ORDER BY keys travel the exact rational domain
create table evd (t decimal(2,1), x double);
insert into evd values (1.5, 2),(3.5, 4);
select t, ewmmean(x, 'halflife', 1, true) over (order by t) from evd order by t;
select t, ewmvar(x, 'halflife', 1, true) over (order by t) from evd order by t;
select t, ewmstd(x, 'halflife', 1, true) over (order by t) from evd order by t;
drop table evd;

-- evh_float_extreme_key: finite DOUBLE keys one half-life apart (adjust=false);
-- y stays constant 0, so the correlation stays NULL (§5 zero-variance rule)
create table evf (t double, x double, y double);
insert into evf values (-1e308, 0, 0),(1e308, 10, 0);
select t, ewmmean(x, 'halflife', 1e308, false) over (order by t) from evf order by t;
select t, ewmvar(x, 'halflife', 1e308, false) over (order by t) from evf order by t;
select t, ewmcorr(x, y, 'halflife', 1e308, false) over (order by t) from evf order by t;
-- a constant signal stays constant across the same gap
delete from evf;
insert into evf values (-1e308, 7, 7),(1e308, 7, 7);
select t, ewmmean(x, 'halflife', 1e308, false) over (order by t) from evf order by t;
select t, ewmvar(x, 'halflife', 1e308, false) over (order by t) from evf order by t;

-- evh_zero_mass_gap: the pathological 1e-20 against h=1e308 gap
delete from evf;
insert into evf values (0, 7, 7),(1e-20, 9, -9);
select t, ewmmean(x, 'halflife', 1e308, true) over (order by t) from evf order by t;
select t, ewmvar(x, 'halflife', 1e308, true) over (order by t) from evf order by t;
select t, ewmcov(x, y, 'halflife', 1e308, true) over (order by t) from evf order by t;
select t, ewmcorr(x, y, 'halflife', 1e308, true) over (order by t) from evf order by t;

-- scaled_merge_[0,1074]: the below-lattice retained arrival share
delete from evf;
insert into evf values (0, 10000, 10000),(1074, 0, 0);
select t, ewmmean(x, 'halflife', 1, false) over (order by t) from evf order by t;
select t, ewmvar(x, 'halflife', 1, false) over (order by t) from evf order by t;
select t, ewmstd(x, 'halflife', 1, false) over (order by t) from evf order by t;
select t, ewmcov(x, y, 'halflife', 1, false) over (order by t) from evf order by t;
select t, ewmcorr(x, y, 'halflife', 1, false) over (order by t) from evf order by t;

-- ev_nulls: the event-time NULL-bit matrix (inaT vs inaF at the gap rows)
delete from ev;
insert into ev values (0, 1, 1),(1, null, null),(2, 3, 3),(3, 4, null),(4, 5, 6);
select t, x, y, ewmmean(x, 'halflife', 1, true, true) over (order by t) from ev order by t;
select t, ewmmean(x, 'halflife', 1, true, false) over (order by t) from ev order by t;
select t, ewmcorr(x, y, 'halflife', 1, true, true) over (order by t) from ev order by t;

-- const_event_inaT/inaF (§10 Invariant 4): the constant 7 signal keeps
-- its account unchanged across the tied event-time batch t=0,35,100,100
-- (a dedicated non-keyed table carries the tied keys)
delete from ev;
create table evc (t bigint, x double, y double);
insert into evc values (0, 7, 7),(35, 7, 7),(100, 7, 7),(100, 7, 7);
select t, ewmmean(x, 'halflife', 100, true, true) over (order by t) from evc order by t;
select t, ewmmean(x, 'halflife', 100, true, false) over (order by t) from evc order by t;
select t, ewmvar(x, 'halflife', 100, true, false) over (order by t) from evc order by t;
drop table evc;

-- unsigned_keys (second review round): BIGINT UNSIGNED event keys keep
-- the full [0, 2^64-1] axis — 2^63 and 2^63+1 are one half-life apart
-- under h=1 ([0, 5]), the domain top works the same, the full-magnitude
-- gap from 0 to 2^64-1 decays the prior account to numerically zero
-- ([0, 10]), and two tied keys at 2^63 are one instant whose unadjusted
-- batch publishes the plain mean (15, 15)
create table uk (t bigint unsigned, x double);
insert into uk values (9223372036854775808, 0),(9223372036854775809, 10);
select t, ewmmean(x, 'halflife', 1, false) over (order by t) from uk order by t;
delete from uk;
insert into uk values (18446744073709551614, 0),(18446744073709551615, 10);
select t, ewmmean(x, 'halflife', 1, false) over (order by t) from uk order by t;
delete from uk;
insert into uk values (0, 0),(18446744073709551615, 10);
select t, ewmmean(x, 'halflife', 1, false) over (order by t) from uk order by t;
delete from uk;
insert into uk values (9223372036854775808, 10),(9223372036854775808, 20);
select t, ewmmean(x, 'halflife', 1, false) over (order by t) from uk order by t;
drop table uk;

-- ev_nulls_unadjusted (fifth challenge round): a NULL-valued instant
-- between two observations decays the prior account once per instant
-- (section 5: NULL-valued groups always apply their instant's
-- transition; the per-group gaps telescope), so with t=[0,1,2],
-- x=[0,NULL,10] and h=1 the adjust=false account merges the arrival on
-- the twice-decayed prior mass — the mass-normalized state
-- (0.25*0+0.5*10)/0.75 = 6.666666666666667, not the one-decay
-- recurrence 5; ignoreNA swaps only the NULL-input row's value bit, and
-- adjust=true's arrival mass k=1 publishes (0.25*0+1*10)/1.25 = 8
create table evu (t bigint, x double);
insert into evu values (0, 0),(1, null),(2, 10);
select t, ewmmean(x, 'halflife', 1, false, false) over (order by t) from evu order by t;
select t, ewmmean(x, 'halflife', 1, false, true) over (order by t) from evu order by t;
select t, ewmmean(x, 'halflife', 1, true, false) over (order by t) from evu order by t;
select t, ewmvar(x, 'halflife', 1, true, false) over (order by t) from evu order by t;
drop table evu;

-- marker_event_default (fifth challenge round): the marker decay
-- resolving to the ('halflife', 1) tag with DEFAULT policies runs
-- event-time with adjust=true — keys [0,1], x=[0,10] publishes
-- (0.5*0+1*10)/1.5 = 6.666666666666667 — and the same positional
-- statement serves the row-decay EXECUTE (0.5, false) publishing [0,5];
-- the three-marker statement pins the tag-form adjust position at the
-- execution boundary: ('halflife', 1, false) publishes [0,5]
create table evt (t bigint, x double);
insert into evt values (0, 0),(1, 10);
prepare mr5a from 'select t, ewmmean(x, ?, ?) over (order by t) from evt order by t';
set @ev_decay = 'halflife';
set @ev_h = 1.0;
execute mr5a using @ev_decay, @ev_h;
set @ev_decay = 0.5;
set @ev_h = false;
execute mr5a using @ev_decay, @ev_h;
deallocate prepare mr5a;
prepare mr5b from 'select t, ewmmean(x, ?, ?, ?) over (order by t) from evt order by t';
set @ev_decay = 'halflife';
set @ev_h = 1.0;
set @ev_adjust = false;
execute mr5b using @ev_decay, @ev_h, @ev_adjust;
deallocate prepare mr5b;
drop table evt;

-- mediumint_boundary (fifth challenge round): MEDIUMINT is a base DDL
-- type (transported as INT32), so COLUMN samples are ordinary numeric
-- samples lowered to double ([1, 1.5, 2.25]); the CAST spelling
-- CAST(x AS MEDIUMINT) is the base's pre-existing bind-time rejection
-- (base_binder.go: MySQL accepts MEDIUMINT in DDL but not as a CAST
-- target) — rejected before ewm evaluation, and the session recovers
create table mev (i int, m mediumint);
insert into mev values (1, 1),(2, 2),(3, 3);
select i, m, ewmmean(m, 0.5, false) over (order by i) from mev order by i;
drop table mev;
select cast(1 as mediumint);
select 1;

-- nonfinite_policy (review round 7): adjust and ignoreNA are constant
-- boolean policies - a non-finite numeric constant is rejected with the
-- policy family instead of silently saturating; finite numeric
-- constants keep resolving 0/1 as false/true, and a prepared
-- statement's non-finite EXECUTE fails that execution only while the
-- same statement serves the next valid EXECUTE afterwards.
-- Since the rebase onto main: cast(<string literal> AS DOUBLE) reads an
-- ordinary SQL string with the MySQL conversion, so the historical
-- spellings cast('NaN' as double)/cast('-Infinity' as double) now carry
-- 0 and would repeat the finite accept-row instead of constructing a
-- non-finite value. Upstream constructs genuinely non-finite doubles
-- through the IEEE bit pattern (bit_cast(unhex(...)) as double, the
-- form of prepare/prepared_nonfinite_float_param.sql, upstream #31720),
-- and a FLOAT-kind prepared parameter now keeps its non-finite value
-- through EXECUTE ... USING. Those spellings keep this round's
-- invariant: non-finite is rejected, finite 0/1 still resolves.
create table npl (t bigint, x double);
insert into npl values (0, 0),(1, 10);
select t, ewmmean(x, 0.5, bit_cast(unhex('000000000000f87f') as double)) over (order by t) from npl order by t;
select t, ewmmean(x, 0.5, cast(1 as double)) over (order by t) from npl order by t;
select t, ewmmean(x, 0.5, false, bit_cast(unhex('000000000000f0ff') as double)) over (order by t) from npl order by t;
select t, ewmmean(x, 0.5, cast(0 as double), true) over (order by t) from npl order by t;
prepare npp from 'select t, ewmmean(x, 0.5, ?, true) over (order by t) from npl order by t';
set @np_adjust = bit_cast(unhex('000000000000f87f') as double);
execute npp using @np_adjust;
select 1;
set @np_adjust = cast(0 as double);
execute npp using @np_adjust;
deallocate prepare npp;
drop table npl;

-- domainless_null_samples (ninth review round): a bare NULL sample binds
-- as the domainless NULL literal type, and the shared checker gives it
-- the same double NULL transport as typed samples (§4 position 1), so
-- every statistic publishes the §5/§6 NULL-policy rows instead of the
-- dispatch transport error: an all-NULL sample column never observes a
-- sample and a pair with a NULL position never observes a valid pair
create table dn (t bigint, x double);
insert into dn values (0, 10),(1, 20);
select t, ewmmean(null, 0.5, false) over (order by t) from dn order by t;
select t, ewmvar(null, 0.5, false) over (order by t) from dn order by t;
select t, ewmstd(null, 0.5, false) over (order by t) from dn order by t;
select t, ewmcov(null, x, 0.5, false) over (order by t) from dn order by t;
select t, ewmcov(x, null, 0.5, false) over (order by t) from dn order by t;
select t, ewmcorr(null, x, 0.5, false) over (order by t) from dn order by t;
select t, ewmcorr(x, null, 0.5, false) over (order by t) from dn order by t;
drop table dn;

-- nullable_results (ninth review round): the ewm results synthesize
-- NULL from all-non-NULL arguments — the §5 C-gate (minPeriods) and the
-- zero-variance correlation rule — so no window output is inferred
-- NOT NULL from its inputs; the generated columns and their values pin
-- the publication rows through the CTAS-derived schema
create table m9 (id bigint primary key, x double);
insert into m9 values (1, 1),(2, 2),(3, 3),(4, 4);
create table ctas_m9 as select ewmmean(x, 0.5, true, false, 2) over (order by id) as c from m9;
show columns from ctas_m9;
select c from ctas_m9 order by c;
create table ctas_c9 as select ewmcorr(1e0, 1e0, 0.5, false) over (order by id) as c from m9;
show columns from ctas_c9;
select c from ctas_c9 order by c;
drop table ctas_m9;
drop table ctas_c9;
drop table m9;

-- decimal256_marker_halflife (ninth review round): the tag-form
-- half-life position behind an unresolved decay marker shares the
-- numeric classification of the literal tag form (§4 position h; §8
-- limitation 3 — decimal256 reaches the configuration evaluator
-- unchanged), so CAST(1 AS DECIMAL(65,0)) binds the complete marker
-- tag form at PREPARE for the mean and covariance families and executes
-- the same event-time account as its literal twins: adjust=false
-- publishes [0,5] (the evt three-marker twin); the adjust=true masses
-- (0.5, 1) publish cov = (0.5*(0-20/3)^2 + (10-20/3)^2)/1.5 = 200/9
-- with corr 1.0 (§5's plain mass normalization, the same rule that
-- pins the ev_nulls twins 20/3 and var 16)
create table d9 (t bigint, x double, y double);
insert into d9 values (0, 0, 0),(1, 10, 10);
set @ewm_tag9 = 'halflife';
prepare d9m from 'select t, ewmmean(x, ?, cast(1 as decimal(65,0)), false, false, 0) over (order by t) from d9 order by t';
execute d9m using @ewm_tag9;
prepare d9c from 'select t, ewmcov(x, y, ?, cast(1 as decimal(65,0)), true, false, 0) over (order by t) from d9 order by t';
execute d9c using @ewm_tag9;
prepare d9r from 'select t, ewmcorr(x, y, ?, cast(1 as decimal(65,0)), true, false, 0) over (order by t) from d9 order by t';
execute d9r using @ewm_tag9;
deallocate prepare d9m;
deallocate prepare d9c;
deallocate prepare d9r;
drop table d9;

-- execution_dependent_clock (tenth review round): the decay and policy
-- positions admit per-execution constants — a real-time expression like
-- SECOND(CURRENT_TIMESTAMP()) survives plan binding as a function tree
-- and the configuration boundary evaluates it with the EXECUTING
-- statement's clock, so neither prepared cache may freeze the PREPARE
-- evaluation. SET SESSION TIMESTAMP pins the statement clock
-- deterministically (the frontend timestamp override; zero restores the
-- wall clock): PREPARE and the first EXECUTE run at second 1 —
-- adjust=true → (1*10 + 0.5*0)/1.5 = 20/3 — and the second EXECUTE runs
-- at second 3: adjust=false → 0.5*0 + 0.5*10 = 5
create table rtt (i int, v double, primary key(i));
insert into rtt values (1, 0.0),(2, 10.0);
set session timestamp = 1793600401;
prepare rt1 from 'select ewmmean(v, 0.5, second(current_timestamp()) = 1) over (order by i) from rtt order by i';
execute rt1;
set session timestamp = 1793600403;
execute rt1;
set session timestamp = 0;
-- the literal-only twin keeps the prepared caches and publishes [0,5] at
-- any clock (the cacheable form the exclusion is not allowed to lose)
prepare rt2 from 'select ewmmean(v, 0.5, false) over (order by i) from rtt order by i';
execute rt2;
deallocate prepare rt1;
deallocate prepare rt2;
drop table rtt;

-- minperiods_boundary (eleventh review round): §4 declares minPeriods a
-- constant integer >= 0 with NO upper limit, judged in the constant's
-- original numeric domain. The int32 cap is gone: 2147483647 and
-- 2147483648 (and the exact int64 4611686018427387905 = 2^62+1, which is
-- NOT float64-exact) are ordinary thresholds whose C-gate publishes NULL
-- at every row of this four-observation fixture. A DECIMAL fraction
-- whose float64 view rounds to an integer is a fractional policy value:
-- 0.999999999999999999 at DECIMAL(18,18) (float64 view exactly 1.0),
-- 9.99999999999999999 at DECIMAL(18,17) (view 10.0) and their 38- and
-- 64-digit twins reject with the §7 minPeriods family instead of
-- silently encoding the rounded integer, while integral decimal
-- spellings (2.0) stay ordinary thresholds. The 64-digit fraction is
-- the widest fractional spelling MatrixOne's parser admits (decimal
-- precision caps at 65) and rides the DECIMAL256 carrier, so that row
-- rejects inside the 256-bit domain. The prepared marker
-- minPeriods re-uses the same per-execution boundary: the fractional
-- EXECUTE fails that execution only, select 1 recovers, and the next
-- EXECUTEs publish through their own thresholds
create table m11 (id bigint primary key, x double);
insert into m11 values (1, 1),(2, 2),(3, 3),(4, 4);
select id, ewmmean(x, 0.5, false, false, cast('2.0' as decimal(18,1))) over (order by id) from m11 order by id;
select id, ewmmean(x, 0.5, false, false, 2147483647) over (order by id) from m11 order by id;
select id, ewmmean(x, 0.5, false, false, 2147483648) over (order by id) from m11 order by id;
select id, ewmmean(x, 0.5, false, false, 4611686018427387905) over (order by id) from m11 order by id;
select id, ewmmean(x, 0.5, false, false, cast('0.999999999999999999' as decimal(18,18))) over (order by id) from m11 order by id;
select 1 as recover_a;
select id, ewmmean(x, 0.5, false, false, cast('9.99999999999999999' as decimal(18,17))) over (order by id) from m11 order by id;
select id, ewmmean(x, 0.5, false, false, cast('0.99999999999999999999999999999999999999' as decimal(38,38))) over (order by id) from m11 order by id;
select id, ewmmean(x, 0.5, false, false, cast('0.9999999999999999999999999999999999999999999999999999999999999999' as decimal(64,64))) over (order by id) from m11 order by id;
select 1 as recover_b;
set @mpb_frac = cast('0.999999999999999999' as decimal(18,18));
set @mpb_big = 2147483648;
set @mpb_two = cast('2.0' as decimal(18,1));
prepare mpb from 'select id, ewmmean(x, 0.5, false, false, ?) over (order by id) from m11 order by id';
execute mpb using @mpb_frac;
select 1 as recover_c;
execute mpb using @mpb_big;
select 1 as recover_d;
execute mpb using @mpb_two;
deallocate prepare mpb;
drop table m11;

drop table ev;
drop table evf;
drop database ewm_stat_time;
