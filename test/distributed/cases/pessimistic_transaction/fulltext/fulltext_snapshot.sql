-- Regression for #27941 (classic fulltext): a MATCH on a named snapshot must read
-- the HISTORICAL index, not the current one. Before the fix, the fulltext TVF loaded
-- the current index via nested SQL at the current txn, so a `{snapshot=...} MATCH`
-- returned current-index results. The fix threads the snapshot TS into the TVF and
-- clones the read txn at that TS. fulltext is async, so readiness is gated on the
-- MATCH row count with a generous wait before divergence-dependent assertions.
drop database if exists ft_snap_case;
create database ft_snap_case;
use ft_snap_case;

create table t(id bigint primary key, body text);
insert into t values (1,'historic alpha'),(2,'shared token');
create fulltext index idx on t(body);

-- Wait until the base index has synced the historic rows BEFORE taking the snapshot,
-- so the snapshot captures a populated (not lagging) index. 'alpha' => 1 row (id1).
-- @wait_expect(1, 120)
select id from t where match(body) against('+alpha' in boolean mode);

create snapshot ft_snap_case_sp for account;

-- Diverge the current index from the snapshot: id1 loses 'alpha', gains 'beta'; add id3.
update t set body='current beta' where id=1;
insert into t values (3,'current beta');

-- Wait until the current index reflects the update (id1,id3 match 'beta' => 2 rows).
-- @wait_expect(2, 120)
select id from t where match(body) against('+beta' in boolean mode) order by id;

-- Current index (control): 'alpha' now gone, 'beta' => id1,id3.
select id from t where match(body) against('+alpha' in boolean mode);
select id from t where match(body) against('+beta' in boolean mode) order by id;

-- Snapshot index (the fix): historical state -- 'alpha' => id1 'historic alpha',
-- 'beta' => empty (id1 had not changed, id3 did not exist at snapshot time).
select id, body from t {snapshot='ft_snap_case_sp'} where match(body) against('+alpha' in boolean mode);
select id from t {snapshot='ft_snap_case_sp'} where match(body) against('+beta' in boolean mode);

drop snapshot ft_snap_case_sp;
drop database ft_snap_case;
