-- Prepared distance threshold on an IVFFLAT index: `distfn(v, lit) <op> ?`.
--
-- getDistRangeFromFilters peels a distance predicate off the filter list and pushes it
-- into the index reader, so the whole query collapses to one Vector Index Scan. It used
-- to require a LITERAL bound, so a prepared '?' lost the pushdown and the plan fell back
-- to a base-table scan joined to the index stream -- same answers, much more work.
--
-- The bound may now be an expression that is CONSTANT for the execution (a '?'), which
-- vectorscan constant-folds before the scan. It must still NOT accept a per-row
-- expression such as a column reference: that has no single value to fold and has to
-- stay a residual filter (#25639).
drop database if exists ivf_prep_range;
create database ivf_prep_range;
use ivf_prep_range;
set experimental_ivf_index = 1;

create table t(id int primary key, v vecf32(3));
insert into t values (1,'[1,1,1]'),(2,'[2,2,2]'),(3,'[9,9,9]'),(4,'[10,10,10]');
create index idx using ivfflat on t(v) lists=1 op_type 'vector_l2_ops';
-- distances from [1,1,1]: id1=0, id2=1.73, id3=13.86, id4=15.59

-- ============ literal controls ============
select id from t where l2_distance(v,'[1,1,1]') < 5 order by l2_distance(v,'[1,1,1]') limit 2;
select id from t where l2_distance(v,'[1,1,1]') > 5 order by l2_distance(v,'[1,1,1]') limit 2;
select id from t where l2_distance(v,'[1,1,1]') <= 1.7320508 order by l2_distance(v,'[1,1,1]') limit 4;

create table u(id int primary key, v vecf32(3), lim double);
insert into u values (1,'[1,1,1]',5),(2,'[2,2,2]',5),(3,'[9,9,9]',0.5),(4,'[10,10,10]',100);
create index uidx using ivfflat on u(v) lists=1 op_type 'vector_l2_ops';

-- ============ the plan, not just the answer ============
-- The pushdown is invisible in the results -- an unpushed bound gives the same rows
-- from a base-table scan joined to the index stream. Assert the SHAPE, or this case
-- passes just as happily with the pushdown gone.
-- @regex("Vector Index Scan",true)
-- @regex("Table Scan",false)
explain select id from t where l2_distance(v,'[1,1,1]') < 5 order by l2_distance(v,'[1,1,1]') limit 2;

-- The same query with a prepared bound must produce the same shape: one index scan,
-- no base-table scan, no join.
prepare plan_stmt from 'explain select id from t where l2_distance(v,''[1,1,1]'') < ? order by l2_distance(v,''[1,1,1]'') limit 2';
set @d=5;
-- @regex("Vector Index Scan",true)
-- @regex("Table Scan",false)
-- @regex("Join",false)
execute plan_stmt using @d;
deallocate prepare plan_stmt;

-- A per-row bound must NOT be pushed: the predicate stays on the scan.
-- @regex("Table Scan",true)
explain select id from u where l2_distance(v,'[1,1,1]') < lim order by id;

-- ============ prepared upper bound ============
prepare lt_stmt from 'select id from t where l2_distance(v,''[1,1,1]'') < ? order by l2_distance(v,''[1,1,1]'') limit 2';
set @d=5;
execute lt_stmt using @d;
-- repeated execution reuses the plan and rebinds the bound
execute lt_stmt using @d;
-- a different bound on the same prepared statement must change the answer
set @d2=1;
execute lt_stmt using @d2;
deallocate prepare lt_stmt;

-- ============ prepared lower bound ============
-- the direction where the filter and the ORDER BY disagree: the nearest rows are the
-- ones the predicate removes, so an unpushed bound would under-return
prepare gt_stmt from 'select id from t where l2_distance(v,''[1,1,1]'') > ? order by l2_distance(v,''[1,1,1]'') limit 2';
set @d=5;
execute gt_stmt using @d;
deallocate prepare gt_stmt;

-- ============ prepared bound on both sides ============
prepare band_stmt from 'select id from t where l2_distance(v,''[1,1,1]'') > ? and l2_distance(v,''[1,1,1]'') < ? order by l2_distance(v,''[1,1,1]'') limit 4';
set @lo=1;
set @hi=14;
execute band_stmt using @lo,@hi;
deallocate prepare band_stmt;

-- ============ a per-row bound stays a residual filter ============
-- id is a column, not a constant: it must not be pushed as a range, and the answer must
-- still be right
select id from u where l2_distance(v,'[1,1,1]') < lim order by id;

drop database ivf_prep_range;
