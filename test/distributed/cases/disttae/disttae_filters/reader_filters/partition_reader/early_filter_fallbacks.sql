drop database if exists early_filter_fallbacks;
create database early_filter_fallbacks;
use early_filter_fallbacks;

-- Committed small batches remain in partition state. Exercise OR union and
-- overlapping branches without relying on persisted block filtering.
create table committed_spk(pk int primary key, v int);
insert into committed_spk values
    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
    (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
select pk from committed_spk
where pk = 1 or pk between 3 and 5 or pk in (8, 9)
order by pk;
select pk from committed_spk
where pk between 2 and 6 or pk between 4 and 8
order by pk;

-- Once persisted, an unsupported conjunct must not discard the supported PK
-- range from early object/block filtering. The residual evaluates both terms.
-- @ignore:0
select mo_ctl('dn', 'flush', 'early_filter_fallbacks.committed_spk');
select sleep(1);
select pk from committed_spk
where pk between 2 and 8 and abs(v - 5) >= 2
order by pk;

-- More than four compound-PK prefixes used to make MemPKFilter fail open.
create table committed_cpk(a varchar(20), b int, v int, primary key(a, b));
insert into committed_cpk values
    ('p1', 1, 1), ('p2', 2, 2), ('p3', 3, 3), ('p4', 4, 4),
    ('p5', 5, 5), ('p6', 6, 6), ('p7', 7, 7), ('p8', 8, 8);
select a, b from committed_cpk
where a in ('p1', 'p2', 'p3', 'p4', 'p5', 'p6')
order by a, b;
select a, b from committed_cpk
where a in ('p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p6', null)
order by a, b;
select a, b from committed_cpk
where a = 'p1' or a between 'p3' and 'p5' or a in ('p7', 'p8')
order by a, b;

-- The same predicates must filter uncommitted workspace batches without a
-- mutable lazy cache or a threshold-based full pass-through.
create table workspace_cpk(a varchar(20), b int, v int, primary key(a, b));
begin;
insert into workspace_cpk values
    ('p1', 1, 1), ('p2', 2, 2), ('p3', 3, 3), ('p4', 4, 4),
    ('p5', 5, 5), ('p6', 6, 6), ('p7', 7, 7), ('p8', 8, 8);
select a, b from workspace_cpk
where a in ('p1', 'p2', 'p3', 'p4', 'p5', 'p6')
order by a, b;
select a, b from workspace_cpk
where a = 'p1' or a between 'p3' and 'p5' or a in ('p7', 'p8')
order by a, b;
rollback;
select count(*) from workspace_cpk;

drop database early_filter_fallbacks;
