drop database if exists unique_index_extra_filter;
create database unique_index_extra_filter;
use unique_index_extra_filter;

create table t (
    id int primary key,
    a varchar(32),
    payload varchar(32),
    unique index uq_a(a)
);
insert into t values
    (1, 'abcdx', 'p1'),
    (2, 'abce0', 'p2');

-- A one-part unique index stores its key directly, so an extra predicate on
-- that part must not try to decode the raw key with serial_extract.
select id, a, payload from t where a = 'abcdx' and a like 'abcdx%';
select id, a, payload from t force index(uq_a) where a = 'abcdx' and a like 'abcdx%';
select id, a, payload from t ignore index(uq_a) where a = 'abcdx' and a like 'abcdx%';
select id, a, payload from t force index(uq_a) where a = 'abcdx' and a > 'abcdw';
select id, a, payload from t force index(uq_a) where 'abcdw' < a and a = 'abcdx';

-- Predicates on columns not represented by the hidden index stay on the base
-- table and remain a control for the backfill path.
select id, a, payload from t force index(uq_a) where a = 'abcdx' and payload = 'p1';

select mo_ctl('dn', 'flush', 'unique_index_extra_filter.t');
select id, a, payload from t where a = 'abcdx' and a like 'abcdx%';
select id, a, payload from t force index(uq_a) where a = 'abcdx' and a like 'abcdx%';
select id, a, payload from t ignore index(uq_a) where a = 'abcdx' and a like 'abcdx%';

-- Serialized composite keys must continue to extract individual parts before
-- pushing additional predicates.
create table composite_control (
    id int primary key,
    a varchar(32),
    b int,
    payload varchar(32),
    unique index uq_ab(a, b)
);
insert into composite_control values
    (1, 'abcdx', 10, 'p1'),
    (2, 'abce0', 20, 'p2');
select id, a, b, payload
from composite_control force index(uq_ab)
where a = 'abcdx' and b = 10 and a like 'abcdx%';

-- A lossy prefix part cannot evaluate a full-value extra predicate on the
-- hidden index. It remains a base-table residual after candidate lookup.
create table prefix_control (
    id int primary key,
    a varchar(32),
    payload varchar(32),
    unique index uq_a(a(4))
);
insert into prefix_control values
    (1, 'abcdx', 'p1'),
    (2, 'abce0', 'p2');
select id, a, payload
from prefix_control force index(uq_a)
where a = 'abcdx' and a like 'abcdx%';
select mo_ctl('dn', 'flush', 'unique_index_extra_filter.prefix_control');
select id, a, payload
from prefix_control force index(uq_a)
where a = 'abcdx' and a like 'abcdx%';

drop database unique_index_extra_filter;
