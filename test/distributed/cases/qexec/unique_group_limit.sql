drop database if exists unique_group_limit_bvt;
create database unique_group_limit_bvt;
use unique_group_limit_bvt;

-- A complete declared-NOT-NULL UNIQUE key has the same singleton-group law as
-- a primary key. Nullable UNIQUE remains a counterexample because SQL permits
-- duplicate NULL keys.
create table unique_values (
    id int,
    a bigint not null,
    b bigint not null,
    v decimal(10, 2),
    unique key uk_ab(a, b)
);
insert into unique_values values
    (1, 10, 100, 1.25),
    (2, 20, 200, null),
    (3, 30, 300, 3.50);
select a, b, count(*), count(v), sum(v), avg(v), min(v), max(v)
from unique_values
group by a, b
order by a, b
limit 100;

create table nullable_unique_values (
    a bigint,
    b bigint,
    unique key uk_ab(a, b)
);
insert into nullable_unique_values values (null, 1), (null, 1), (2, 2);
select a, b, count(*)
from nullable_unique_values
group by a, b
order by a, b
limit 100;

-- Prepared execution must observe UNIQUE DDL changes rather than retaining a
-- stale multiplicity proof.
prepare unique_group from
    'select a, b, count(*) c from unique_values group by a, b order by a, b limit 100';
execute unique_group;
alter table unique_values drop index uk_ab;
insert into unique_values values (4, 10, 100, 9.00);
execute unique_group;
delete from unique_values where id = 4;
alter table unique_values add unique key uk_ab(a, b);
execute unique_group;
drop table unique_values;
create table unique_values (
    id int,
    a bigint not null,
    b bigint not null,
    v decimal(10, 2),
    unique key uk_ab(a, b)
);
insert into unique_values values (5, 50, 500, 5.00);
execute unique_group;
deallocate prepare unique_group;

drop database unique_group_limit_bvt;
