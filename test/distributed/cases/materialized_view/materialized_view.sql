drop database if exists mv_e2e;
create database mv_e2e;
use mv_e2e;

-- FAST uses the real ISCP snapshot and tail path.  Every readiness query
-- compares the persisted MV rows with an independent source-table aggregate.
create table events (
    id bigint primary key,
    service varchar(20),
    duration int,
    trace_id varchar(20)
);

insert into events values
    (1, null, 10, 'a'),
    (2, null, null, 'a'),
    (3, 'api', 5, 'x'),
    (4, 'api', 20, 'y'),
    (5, 'web', null, null);

create materialized view mv_fast
refresh fast on change as
select service,
       count(*) as row_count,
       count(duration) as duration_count,
       sum(duration) as duration_sum,
       avg(duration) as duration_avg,
       min(duration) as duration_min,
       max(duration) as duration_max,
       count(distinct trace_id) as trace_count
from events
group by service;

-- Initial snapshot, including a NULL group and NULL aggregate inputs.
-- @wait_expect(2, 30)
select count(*) = 0 as mv_matches
from (
    select 1
    from mv_fast m
    full outer join (
        select service,
               count(*) as row_count,
               count(duration) as duration_count,
               sum(duration) as duration_sum,
               avg(duration) as duration_avg,
               min(duration) as duration_min,
               max(duration) as duration_max,
               count(distinct trace_id) as trace_count
        from events
        group by service
    ) e on m.service <=> e.service
    where m.row_count is null or e.row_count is null
       or not (m.row_count <=> e.row_count
           and m.duration_count <=> e.duration_count
           and m.duration_sum <=> e.duration_sum
           and m.duration_avg <=> e.duration_avg
           and m.duration_min <=> e.duration_min
           and m.duration_max <=> e.duration_max
           and m.trace_count <=> e.trace_count)
) mismatches;

insert into events values
    (6, 'api', 1, 'x'),
    (7, 'db', 30, 'z');

-- Append tail: a duplicate distinct value must not increase trace_count.
-- @wait_expect(2, 30)
select count(*) = 0 as mv_matches
from (
    select 1
    from mv_fast m
    full outer join (
        select service, count(*) as row_count, count(duration) as duration_count,
               sum(duration) as duration_sum, avg(duration) as duration_avg,
               min(duration) as duration_min, max(duration) as duration_max,
               count(distinct trace_id) as trace_count
        from events group by service
    ) e on m.service <=> e.service
    where m.row_count is null or e.row_count is null
       or not (m.row_count <=> e.row_count
           and m.duration_count <=> e.duration_count
           and m.duration_sum <=> e.duration_sum
           and m.duration_avg <=> e.duration_avg
           and m.duration_min <=> e.duration_min
           and m.duration_max <=> e.duration_max
           and m.trace_count <=> e.trace_count)
) mismatches;

delete from events where id = 4;

-- Delete tail: removing the previous maximum forces affected-group MIN/MAX
-- recomputation while preserving the remaining distinct multiplicity.
-- @wait_expect(2, 30)
select count(*) = 0 as mv_matches
from (
    select 1
    from mv_fast m
    full outer join (
        select service, count(*) as row_count, count(duration) as duration_count,
               sum(duration) as duration_sum, avg(duration) as duration_avg,
               min(duration) as duration_min, max(duration) as duration_max,
               count(distinct trace_id) as trace_count
        from events group by service
    ) e on m.service <=> e.service
    where m.row_count is null or e.row_count is null
       or not (m.row_count <=> e.row_count
           and m.duration_count <=> e.duration_count
           and m.duration_sum <=> e.duration_sum
           and m.duration_avg <=> e.duration_avg
           and m.duration_min <=> e.duration_min
           and m.duration_max <=> e.duration_max
           and m.trace_count <=> e.trace_count)
) mismatches;

update events
set service = 'web', duration = 40, trace_id = 'b'
where id = 1;

-- Update tail is one old-row delete plus one new-row insert and moves a row
-- out of the NULL group into another group.
-- @wait_expect(2, 30)
select count(*) = 0 as mv_matches
from (
    select 1
    from mv_fast m
    full outer join (
        select service, count(*) as row_count, count(duration) as duration_count,
               sum(duration) as duration_sum, avg(duration) as duration_avg,
               min(duration) as duration_min, max(duration) as duration_max,
               count(distinct trace_id) as trace_count
        from events group by service
    ) e on m.service <=> e.service
    where m.row_count is null or e.row_count is null
       or not (m.row_count <=> e.row_count
           and m.duration_count <=> e.duration_count
           and m.duration_sum <=> e.duration_sum
           and m.duration_avg <=> e.duration_avg
           and m.duration_min <=> e.duration_min
           and m.duration_max <=> e.duration_max
           and m.trace_count <=> e.trace_count)
) mismatches;

select service, row_count, duration_count, duration_sum, duration_avg,
       duration_min, duration_max, trace_count
from mv_fast
order by service;

-- UNION ALL keeps branch identity in the hidden key. Equal visible groups from
-- different sources remain duplicate output rows and each source batch is
-- routed only to its own incremental branch.
create table union_events (
    id bigint primary key,
    service varchar(20),
    duration int,
    trace_id varchar(20)
);
create table union_archive (
    id bigint primary key,
    service varchar(20),
    duration int,
    trace_id varchar(20)
);
insert into union_events values (1, 'api', 10, 't1'), (2, 'web', 8, 't2');
insert into union_archive values (10, 'api', 20, 'u1'), (11, 'db', 30, 'u2');

create materialized view mv_union_all
refresh fast on change as
select service, count(*) as row_count, sum(duration) as duration_sum,
       min(duration) as duration_min, max(duration) as duration_max,
       count(distinct trace_id) as trace_count
from union_events group by service
union all
select service, count(*) as rows_seen, sum(duration) as duration_total,
       min(duration) as min_seen, max(duration) as max_seen,
       count(distinct trace_id) as traces_seen
from union_archive group by service;

-- @wait_expect(2, 30)
select count(*) = 4 and sum(row_count) = 4 as union_ready from mv_union_all;

insert into union_events values (3, 'api', 5, 't1');

-- Pure append must update only the matching branch even when another branch
-- has the same visible group key. This check runs before any affected-group
-- recomputation from a delete can mask cross-branch contamination.
-- @wait_expect(2, 30)
select count(*) = 2 and sum(row_count) = 3 and sum(duration_sum) = 35
       and min(duration_min) = 5 and max(duration_max) = 20
       and sum(trace_count) = 2 as union_append_ready
from mv_union_all where service = 'api';

delete from union_archive where id = 10;
update union_events set service = 'api', duration = 7, trace_id = 't3' where id = 2;
update union_archive set service = 'api', duration = 40, trace_id = 'u3' where id = 11;

-- One branch now contributes three api rows while the other contributes one;
-- UNION ALL must expose two api rows rather than merge them.
-- @wait_expect(2, 30)
select count(*) = 2 and sum(row_count) = 4 and sum(duration_sum) = 62 as union_ready
from mv_union_all;

select service, row_count, duration_sum, duration_min, duration_max, trace_count
from mv_union_all
order by duration_sum;

-- COMPLETE and FORCE exercise boundary-consistent full replacement over two
-- direct sources.  FORCE must select full refresh because JOIN is outside the
-- incremental subset.
create table dimensions (
    id int primary key,
    label varchar(20) not null
);
create table facts (
    id bigint primary key,
    dimension_id int not null,
    amount int
);
insert into dimensions values (1, 'api'), (2, 'edge');
insert into facts values (1, 1, 10), (2, 1, 20), (3, 2, 30);

create materialized view mv_complete
refresh complete on change as
select d.label, count(*) as row_count, sum(f.amount) as amount_sum
from facts f join dimensions d on f.dimension_id = d.id
group by d.label;

create materialized view mv_force
refresh force on change as
select d.label, count(*) as row_count, sum(f.amount) as amount_sum
from facts f join dimensions d on f.dimension_id = d.id
group by d.label;

-- @wait_expect(2, 30)
select refresh_method, mv_matches
from (
    select 'complete' as refresh_method, count(*) = 0 as mv_matches
    from (
        select 1
        from mv_complete m
        full outer join (
            select d.label, count(*) as row_count, sum(f.amount) as amount_sum
            from facts f join dimensions d on f.dimension_id = d.id
            group by d.label
        ) e on m.label <=> e.label
        where m.row_count is null or e.row_count is null
           or not (m.row_count <=> e.row_count and m.amount_sum <=> e.amount_sum)
    ) complete_mismatches
    union all
    select 'force' as refresh_method, count(*) = 0 as mv_matches
    from (
        select 1
        from mv_force m
        full outer join (
            select d.label, count(*) as row_count, sum(f.amount) as amount_sum
            from facts f join dimensions d on f.dimension_id = d.id
            group by d.label
        ) e on m.label <=> e.label
        where m.row_count is null or e.row_count is null
           or not (m.row_count <=> e.row_count and m.amount_sum <=> e.amount_sum)
    ) force_mismatches
) readiness
order by refresh_method;

insert into facts values (4, 2, 5);
update facts set amount = 15 where id = 1;
delete from facts where id = 3;
update dimensions set label = 'core' where id = 1;

-- Changes on both direct sources must converge to one common source boundary.
-- @wait_expect(2, 30)
select refresh_method, mv_matches
from (
    select 'complete' as refresh_method, count(*) = 0 as mv_matches
    from (
        select 1
        from mv_complete m
        full outer join (
            select d.label, count(*) as row_count, sum(f.amount) as amount_sum
            from facts f join dimensions d on f.dimension_id = d.id
            group by d.label
        ) e on m.label <=> e.label
        where m.row_count is null or e.row_count is null
           or not (m.row_count <=> e.row_count and m.amount_sum <=> e.amount_sum)
    ) complete_mismatches
    union all
    select 'force' as refresh_method, count(*) = 0 as mv_matches
    from (
        select 1
        from mv_force m
        full outer join (
            select d.label, count(*) as row_count, sum(f.amount) as amount_sum
            from facts f join dimensions d on f.dimension_id = d.id
            group by d.label
        ) e on m.label <=> e.label
        where m.row_count is null or e.row_count is null
           or not (m.row_count <=> e.row_count and m.amount_sum <=> e.amount_sum)
    ) force_mismatches
) readiness
order by refresh_method;

select label, row_count, amount_sum from mv_complete order by label;
select label, row_count, amount_sum from mv_force order by label;

drop materialized view mv_fast;
drop materialized view mv_union_all;
drop materialized view mv_complete;
drop materialized view mv_force;
drop database mv_e2e;
