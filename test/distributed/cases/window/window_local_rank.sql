-- Local sliding-window ranks (issue #29947): the frame-local competition
-- rank operators mrank(v, N), cumrank(v) and tmrank(v, INTERVAL n unit) —
-- 1-based ascending competition rank of the current non-NULL value, NULL when
-- the current value is NULL. Any error in this file is expected to be the
-- documented error surface of the operators, not a commented-out failure.
drop database if exists local_rank_bvt;
create database local_rank_bvt;
use local_rank_bvt;
set @local_rank_saved_tz = @@time_zone;
set time_zone = '+00:00';

-- The issue's six-row profile fixture.
drop table if exists local_rank_src;
create table local_rank_src (id int, gid int, tick int, v double, y double, w double, flag int);
insert into local_rank_src values
(1,1,0,cast(null as double),2.0,1.0,1),
(2,1,0,2.0,4.0,2.0,0),
(3,1,1,4.0,3.0,1.0,1),
(4,1,3,4.0,7.0,3.0,1),
(5,1,3,cast(null as double),5.0,1.0,0),
(6,1,8,-1.0,8.0,2.0,1);

-- The issue's declared profile: competition rank in the last three physical
-- rows. Expected: NULL,1,2,2,NULL,1.
select id, v, mrank(v,3) over (order by id) as r from local_rank_src order by id;

-- The issue's own independent SQL control reproduces the same values through
-- a completely different mechanism (self-join + aggregate).
select a.id, a.v,
       case when a.v is null then null
            else 1+sum(case when b.v<a.v then 1 else 0 end) end as control_rank
from local_rank_src a join local_rank_src b
     on b.id between a.id-2 and a.id
group by a.id, a.v
order by a.id;

-- Cumulative competition rank from the partition start through the current
-- row: each non-NULL row ranks every strictly smaller non-NULL row seen so
-- far, plus one. Expected: NULL,1,2,2,NULL,1 (the second 4 ties with the
-- first and they share rank 2).
select id, v, cumrank(v) over (order by id) as r from local_rank_src order by id;

-- A width wider than the partition reproduces cumrank exactly.
select id, mrank(v,10) over (order by id) as wide, cumrank(v) over (order by id) as cum
from local_rank_src order by id;

-- Width one: every non-NULL row ranks alone. Expected: NULL,1,1,1,NULL,1.
select id, mrank(v,1) over (order by id) as r from local_rank_src order by id;

-- Width two: the 4/4 tie shares rank 1. Expected: NULL,1,2,1,NULL,1.
select id, mrank(v,2) over (order by id) as r from local_rank_src order by id;

-- Partitions reset both frames.
insert into local_rank_src values
(7,2,0,9.0,1.0,1.0,1),
(8,2,1,8.0,2.0,2.0,0),
(9,2,5,7.5,3.0,1.0,1),
(10,2,6,-3.0,4.0,2.0,1);

select id, gid, mrank(v,3) over (partition by gid order by id) as r
from local_rank_src order by id;

select id, gid, cumrank(v) over (partition by gid order by id) as r
from local_rank_src order by id;

-- The time-bounded trailing rank over RANGE-interval membership.
drop table if exists tmrank_src;
create table tmrank_src (id int, ts datetime, v double);
insert into tmrank_src values
(1,'2026-01-01 00:00:00',5.0),
(2,'2026-01-02 00:00:00',4.0),
(3,'2026-01-02 06:00:00',9.0),
(4,'2026-01-03 12:00:00',1.0);

-- One DAY: expected 1,1,2,1.
select id, tmrank(v, interval 1 day) over (order by ts) as r from tmrank_src order by id;

-- 1.5 DAY decimal literal quantizes to a 36-hour (129600000000) MICROSECOND
-- magnitude and reaches back past the first row for row 3: expected 1,1,3,1.
select id, tmrank(v, interval 1.5 day) over (order by ts) as r from tmrank_src order by id;

-- The MySQL-string form of the same spelling keeps the integral DAY reading:
-- expected 1,1,2,1 (the rounding-profile counterexample against treating the
-- string and the decimal literal identically).
select id, tmrank(v, interval '1.5' day) over (order by ts) as r from tmrank_src order by id;

-- Calendar arithmetic for month units: one month back from March 31 lands on
-- the capped February 28, so the February row stays in the frame and row 2
-- ranks 2 (a flat 30-day subtraction would make it 1).
drop table if exists tmrank_month;
create table tmrank_month (id int, ts date, v double);
insert into tmrank_month values
(1,'2026-02-28',5.0),
(2,'2026-03-31',10.0);
select id, ts, tmrank(v, interval 1 month) over (order by ts) as r from tmrank_month order by id;

-- Equal-instant peers with a zero-hour window: the approved membership
-- definition (RANGE membership intersected with k <= j) lets the window scan
-- order two equal-instant peers freely, so this public section value-
-- equalizes the peers (3.0 and 3.0) - under every peer permutation the two
-- rows rank 1 because neither equal value is ever strictly less than the
-- other. The trailing-cutoff DISCRIMINATION itself (a first, higher-valued
-- peer whose rank must drop the later, lower-valued peer) is pinned by the
-- operator-level tests with a controlled physical order; here the
-- invariant public contract is that k<=j plus value equality holds the
-- ranks permutation-free.
drop table if exists tmrank_peers;
create table tmrank_peers (id int, ts datetime, v double);
insert into tmrank_peers values
(1,'2026-01-01 00:00:00',7.0),
(2,'2026-01-01 05:00:00',3.0),
(3,'2026-01-01 05:00:00',3.0);

select id, v, tmrank(v, interval 0 hour) over (order by ts) as r
from tmrank_peers order by id;

-- Parity demonstration: the ordinary RANGE CURRENT ROW peer group counts
-- both equal-instant rows (1,2,2) while tmrank returns 1 for both peers
-- under any scan permutation: the k<=j cut bounds the frame at the current
-- row and equal values never raise a strictly-smaller count.
select id,
       count(*) over (order by ts range current row) as peer_group,
       tmrank(v, interval 0 hour) over (order by ts) as r
from tmrank_peers order by id;

-- The remaining catalog-admitted value domains: YEAR (temporal, including
-- the 1901..2155 bounds) and DECIMAL256 magnitudes far beyond int64 compare
-- exactly like the other numeric and temporal members, on every operator.
drop table if exists local_rank_typed;
create table local_rank_typed (id int, ts datetime, y year, d decimal(65,0));
insert into local_rank_typed values
(1,'2026-01-01 00:00:00',2024,-100000000000000000000000000000),
(2,'2026-01-02 00:00:00',2024, 50000000000000000000000000),
(3,'2026-01-02 06:00:00',1999, 500000000000000000000000000),
(4,'2026-01-03 12:00:00',2155,1000000000000000000000000000000);

-- cumrank over YEAR: the 2024 tie shares rank 1; 1999 is a new low; the 2155
-- high ranks above everything before it. Expected 1,1,1,4.
select id, y, cumrank(y) over (order by id) as r from local_rank_typed order by id;

-- cumrank over DECIMAL256: strictly ascending magnitudes past int64 range.
-- Expected 1,2,3,4.
select id, d, cumrank(d) over (order by id) as r from local_rank_typed order by id;

-- mrank's trailing three-row frame over the same DECIMAL256 values: ties of
-- width frames still rank within the last three rows only. Expected 1,2,3,3.
select id, mrank(d, 3) over (order by id) as r from local_rank_typed order by id;

-- The 36-hour (1.5 DAY) window over DECIMAL256 values: row 2's border lands
-- on Dec 31 12:00 and reaches back to row 1, row 3's border lands exactly on
-- row 1's timestamp so rows 1-3 compete, and row 4's border lands exactly on
-- row 2's timestamp so rows 2-4 compete. Expected 1,2,3,3.
select id, tmrank(d, interval 1.5 day) over (order by ts) as r
from local_rank_typed order by id;

-- D21 key domain: YEAR columns are published with their own Oid and the
-- shared resolver has no YEAR order-key form, so a YEAR ORDER BY stays
-- refused exactly like the integer key above.
select id, tmrank(d, interval 1 day) over (order by y) from local_rank_typed order by id;

-- NULL current values rank NULL inside a time-bounded frame. The three rows
-- share one timestamp, so the two non-NULL peers are value-equalized (5.0
-- and 5.0): whichever way the scan orders them, neither is strictly less
-- than the other and both rank 1, keeping the public rows
-- permutation-free (the trailing-cut discrimination itself is pinned by the
-- operator-level tests).
drop table if exists tmrank_nullv;
create table tmrank_nullv (id int, ts datetime, v double);
insert into tmrank_nullv values
(1,'2026-01-01 00:00:00',cast(null as double)),
(2,'2026-01-01 00:00:00',5.0),
(3,'2026-01-01 00:00:00',5.0);
select id, v, tmrank(v, interval 1 day) over (order by ts) as r from tmrank_nullv order by id;

-- NULL order keys keep their ordinary NULL peer group (k <= j cut at the
-- current row), and the equal-instant and NULL-key peer groups are value-
-- equalized here so every recorded rank holds under ANY peer permutation
-- the window scan may choose (the public contract is permutation-free:
-- neither peer of a pair is ever strictly less than the other, so both
-- rank 1 whichever way the scan orders them). The trailing cutoff
-- discrimination - a first, higher-valued peer whose rank proves the later,
-- lower-valued peer was cut - is pinned by the operator-level tests with a
-- controlled physical order.
insert into tmrank_peers values
(4,'2026-01-02 00:00:00',6.0),
(5,null,2.0),
(6,null,2.0);
select id, count(*) over (order by ts range interval 1 day preceding) as range_cnt,
       tmrank(v, interval 1 day) over (order by ts) as r
from tmrank_peers order by id;

-- The compound string with a fractional second publishes the
-- NULL-arithmetic outcome: a null-marked count propagates NULL to every
-- key row of the window.
select id, tmrank(v, interval '00:01.5' minute_second) over (order by ts) as r
from tmrank_src order by id;

-- The foldable-ripple family: sin(1) day rounds to 1 day; an INT-typed
-- negative-then-rounded magnitude (CAST 0-sin(9) AS SIGNED rounds -0.41 to 0)
-- publishes the accepted zero-width control (each row ranks alone on
-- distinct keys). The bare 0-SIN(9) fold stays a negative float after
-- binding and is refused with the shared illegal-width error below.
select id, tmrank(v, interval sin(1) day) over (order by ts) as r from tmrank_src order by id;
select id, tmrank(v, interval cast(0-sin(9) as signed) day) over (order by ts) as r from tmrank_src order by id;

-- Width validation surfaces.
select id, mrank(v, 0) over (order by id) as r from local_rank_src order by id;
select id, mrank(v, -1) over (order by id) as r from local_rank_src order by id;
select id, mrank(v, cast(null as bigint)) over (order by id) as r from local_rank_src order by id;
-- Unsigned widths validate in their source integer domain: values above
-- int64 Max are valid positive counts that simply cover the partition
-- prefix, so each below reproduces cumrank (NULL,1,2,2,NULL,1).
select id, mrank(v, cast(9223372036854775808 as unsigned)) over (order by id) as r
from local_rank_src order by id;
select id, mrank(v, cast(18446744073709551615 as unsigned)) over (order by id) as r
from local_rank_src order by id;

-- Arity and domain gates at bind time.
select id, mrank(v) over (order by id) from local_rank_src order by id;
select id, mrank(v, '3') over (order by id) from local_rank_src order by id;
select id, mrank(cast('x' as varchar), 3) over (order by id) from local_rank_src order by id;

-- The internal three-argument spelling is unreachable from user SQL.
select id, tmrank(v, 1, 2) over (order by ts) from tmrank_src order by id;

-- tmrank's ORDER BY key domain: exactly one DATE/TIME/DATETIME/TIMESTAMP key.
-- An integer key is refused with the named diagnostic.
select id, tmrank(v, interval 1 day) over (order by id) from tmrank_src order by id;
-- A partition over any type stays legal; only the ORDER BY key domain is
-- restricted. Partitions keyed by the row id rank every row alone.
select id, tmrank(v, interval 1 day) over (partition by id order by ts) as r
from tmrank_src order by id;

-- A non-foldable (column-referenced) width is refused before evaluation.
select id, tmrank(v, interval id day) over (order by ts) from tmrank_src order by id;

-- Negative and NULL interval magnitudes share the frame-bound illegal-width
-- error, including the reviewer-approved 0-SIN(1) negative witness: the
-- raw float fold is already negative and the shared int64 CAST would keep
-- it negative (-1), so the refusal fires on every path.
select id, tmrank(v, interval -1 day) over (order by ts) as r from tmrank_src order by id;
select id, tmrank(v, interval null day) over (order by ts) as r from tmrank_src order by id;
select id, tmrank(v, interval (0-sin(1)) day) over (order by ts) as r from tmrank_src order by id;

-- Overflowing interval magnitudes share the frame-bound judge row-
-- independently: the compile-time owner rejects the width on populated
-- input, on EMPTY INPUT and under LIMIT 0 with the same surface, before any
-- row is produced, and a repeated identical statement observes the same
-- outcome (the validate-discard pass leaves the reusable plan untouched).
select id, tmrank(v, interval 9223372036854775807 year) over (order by ts) as r
from tmrank_src order by id;
select id, tmrank(v, interval 9223372036854775807 year) over (order by ts) as r
from tmrank_src where id < 0;
select id, tmrank(v, interval 9223372036854775807 year) over (order by ts) as r
from tmrank_src limit 0;
select id, tmrank(v, interval 9223372036854775807 year) over (order by ts) as r
from tmrank_src order by id;

-- The row-independent mirror for VALID widths: identical repeated executions
-- succeed and stay repeatable over empty input and under LIMIT 0.
select id, tmrank(v, interval 3 day) over (order by ts) as r
from tmrank_src where id < 0;
select id, tmrank(v, interval 3 day) over (order by ts) as r
from tmrank_src limit 0;
select id, tmrank(v, interval 3 day) over (order by ts) as r
from tmrank_src where id < 0;

-- The D22 admission boundary: a control function in tmrank's width is a
-- bind-time refusal with the frame-bound gate's exact message for every
-- account (the refusal runs before any evaluation and before any
-- account-dependent gate).
select id, tmrank(v, interval mo_ctl('cn','Task','health:time') day) over (order by ts)
from tmrank_src order by id;
select id, tmrank(v, interval (3 + mo_ctl('cn','Task','health:time')) day) over (order by ts)
from tmrank_src order by id;
select sum(v) over (order by ts range interval mo_ctl('cn','Task','health:time') day preceding)
from tmrank_src;

-- Explicit frames are rejected: the operators derive their frames from their
-- own arguments.
select id, mrank(v,3) over (order by id rows 2 preceding) from local_rank_src order by id;
select id, cumrank(v) over (order by id range between unbounded preceding and current row)
from local_rank_src order by id;
select id, tmrank(v, interval 1 day) over (order by ts rows unbounded preceding)
from tmrank_src order by id;

-- Prepared width: the direct marker is retained raw at PREPARE and each
-- EXECUTE parameter re-materializes through the same normalization (user
-- variables are the parameter carrier EXECUTE USING accepts; 3 and 30 DAYS
-- reach back past the first row for the 9.0; the zero parameter publishes
-- the zero-width frame where every row ranks alone). The parameter's
-- runtime domain survives the round trip: a NUMERIC 1.5 transports the raw
-- pipeline's MySQL nearest-integer reading - two DAYS - while the
-- STRING-spelled parameter transports the string grammar's integral-prefix
-- reading (one day), and an overflowing EXECUTE parameter rejects at
-- execution through the same compile-time owner as the constant form
-- above. On THIS fixture [1,1,3,1] the two-day and one-day numeric
-- readings agree; the 40-hour witness below separates them, and the
-- bind-suite UT pins that the CONSTANT decimal literal takes the 36-hour
-- microsecond quantization instead (the two owners intentionally use
-- different conversions).
prepare stmt_tmr from select id, tmrank(v, interval ? day) over (order by ts) as r
from tmrank_src order by id;
set @tmrank_w = 3;
execute stmt_tmr using @tmrank_w;
set @tmrank_w = 0;
execute stmt_tmr using @tmrank_w;
set @tmrank_w = 30;
execute stmt_tmr using @tmrank_w;
set @tmrank_w = 1.5;
execute stmt_tmr using @tmrank_w;
set @tmrank_w = '1.5';
execute stmt_tmr using @tmrank_w;
set @tmrank_w = 1.5;
execute stmt_tmr using @tmrank_w;
set @tmrank_w = 9223372036854775807;
execute stmt_tmr using @tmrank_w;
deallocate prepare stmt_tmr;

-- The 40-hour witness: the rows sit 40 hours apart, inside two DAYS but
-- outside the 36-hour constant. So the CONSTANT decimal 1.5-DAY literal
-- (36 hours through the bind owner) leaves the later row ranking 1, the
-- two-DAY constant bracket ranks it 2, and the PREPARED numeric 1.5 must
-- publish the raw pipeline's nearest-integer reading (2 DAYS -> rank 2)
-- while the STRING parameter keeps the integral prefix (1 DAY -> rank 1).
-- Expected rows: constant 1.5 DAY -> 1,1; constant 2 DAY -> 1,2; prepared
-- numeric 1.5 -> 1,2; prepared string '1.5' -> 1,1.
drop table if exists tmrank_w40;
create table tmrank_w40 (id int, ts datetime, v double);
insert into tmrank_w40 values
(1,'2026-01-01 00:00:00',1.0),
(2,'2026-01-02 16:00:00',5.0);
select id, tmrank(v, interval 1.5 day) over (order by ts) as r from tmrank_w40 order by id;
select id, tmrank(v, interval 2 day) over (order by ts) as r from tmrank_w40 order by id;
prepare stmt_w40 from select id, tmrank(v, interval ? day) over (order by ts) as r
from tmrank_w40 order by id;
set @tmrank_w = 1.5;
execute stmt_w40 using @tmrank_w;
set @tmrank_w = '1.5';
execute stmt_w40 using @tmrank_w;
deallocate prepare stmt_w40;
drop table tmrank_w40;

-- Prepared widths under empty input and LIMIT 0 with recovery on the same
-- handle: the row-independent owner refuses an overflowing EXECUTE
-- parameter even when no row will be produced (empty WHERE input and
-- LIMIT 0), and the SAME prepared handle stays usable afterwards - a
-- clean empty result on a valid width, then the overflow refusal again.
prepare stmt_tmr_empty from select id, tmrank(v, interval ? day) over (order by ts) as r
from tmrank_src where id < 0;
set @tmrank_w = 9223372036854775807;
execute stmt_tmr_empty using @tmrank_w;
set @tmrank_w = 3;
execute stmt_tmr_empty using @tmrank_w;
set @tmrank_w = 0;
execute stmt_tmr_empty using @tmrank_w;
set @tmrank_w = 9223372036854775807;
execute stmt_tmr_empty using @tmrank_w;
deallocate prepare stmt_tmr_empty;
prepare stmt_tmr_l0 from select id, tmrank(v, interval ? day) over (order by ts) as r
from tmrank_src limit 0;
set @tmrank_w = 9223372036854775807;
execute stmt_tmr_l0 using @tmrank_w;
set @tmrank_w = 1.5;
execute stmt_tmr_l0 using @tmrank_w;
deallocate prepare stmt_tmr_l0;

-- Prepared MRANK widths (NTILE parity): the bare width marker binds in the
-- INTEGER domain at PREPARE, and every EXECUTE is re-resolved against the
-- fill's own domain: integer fills rank (an oversized width clamps at the
-- partition start), integer-domain values below 1 (0, negative) refuse with
-- the operator's named row-count execution error, and non-integer-domain
-- fills (NULL, decimal 1.5, string '3') refuse with the function's
-- overload-class argument error - the same rejection class as ordinary
-- non-integer widths. The SAME handle recovers for a valid width after each
-- refusal.
prepare stmt_mrank from select id, mrank(v, ?) over (order by id) as r
from local_rank_src order by id;
set @mrank_w = 3;
execute stmt_mrank using @mrank_w;
set @mrank_w = 1;
execute stmt_mrank using @mrank_w;
set @mrank_w = 2;
execute stmt_mrank using @mrank_w;
set @mrank_w = 0;
execute stmt_mrank using @mrank_w;
set @mrank_w = -1;
execute stmt_mrank using @mrank_w;
set @mrank_w = null;
execute stmt_mrank using @mrank_w;
set @mrank_w = 1.5;
execute stmt_mrank using @mrank_w;
set @mrank_w = '3';
execute stmt_mrank using @mrank_w;
set @mrank_w = 3;
execute stmt_mrank using @mrank_w;
deallocate prepare stmt_mrank;

-- The explicit CAST(? AS UNSIGNED) prepared route keeps its own unsigned
-- domain: MaxUint64 covers the partition prefix (the cumrank profile) and a
-- small width ranks narrowly on the same handle - the unsigned boundary
-- transport documented for ordinary spelling survives preparation.
prepare stmt_mrank_u from select id, mrank(v, cast(? as unsigned)) over (order by id) as r
from local_rank_src order by id;
set @mrank_w = 18446744073709551615;
execute stmt_mrank_u using @mrank_w;
set @mrank_w = 3;
execute stmt_mrank_u using @mrank_w;
deallocate prepare stmt_mrank_u;

-- mrank's invalid-width refusal is ROW-DEPENDENT (each row's width is read
-- during the scan), so an invalid EXECUTE over EMPTY input returns an empty
-- result rather than tmrank's row-independent overflow refusal, and the same
-- handle keeps working for valid widths afterwards.
prepare stmt_mrank_e from select id, mrank(v, ?) over (order by id) as r
from local_rank_src where id < 0 order by id;
set @mrank_w = 0;
execute stmt_mrank_e using @mrank_w;
set @mrank_w = 3;
execute stmt_mrank_e using @mrank_w;
set @mrank_w = 0;
execute stmt_mrank_e using @mrank_w;
deallocate prepare stmt_mrank_e;

-- The OVER-less probe stays a parse error, the same surface rank() produces.
select mrank(v,3) from local_rank_src;
select cumrank(v) from local_rank_src;
select tmrank(v, interval 3 day) from tmrank_src;
select rank() from local_rank_src;

-- Identifier compatibility: the three names stay usable as plain identifiers.
drop table if exists mrank;
create table mrank (id int);
insert into mrank values (1);
select mrank.id from mrank;
drop table if exists cumrank;
create table cumrank (tmrank int, mrank int);
insert into cumrank values (2, 3);
select tmrank, mrank from cumrank;
select cumrank.mrank from cumrank;
drop table mrank;
drop table cumrank;

-- Restore the session timezone before cleanup.
set time_zone = @local_rank_saved_tz;

-- Clean up fixtures.
drop table tmrank_month;
drop table tmrank_peers;
drop table tmrank_nullv;
drop table tmrank_src;
drop table local_rank_typed;
drop table local_rank_src;
drop database local_rank_bvt;
