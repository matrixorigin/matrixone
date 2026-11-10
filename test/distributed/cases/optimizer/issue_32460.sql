drop database if exists issue_32460;
create database issue_32460;
use issue_32460;

create table event_index_repro (
  id bigint primary key,
  user_id varchar(128) not null,
  run_id varchar(128) not null,
  event_type varchar(32) not null,
  event_idx bigint not null,
  index idx_control (user_id, run_id, event_type, event_idx)
);

-- @separator:table
-- @regex("Index Table Scan.*idx_control",true)
-- @regex("Runtime Filter Probe: __mo_index_idx_col Match Prefix",true)
explain
select r.ordinal, max(e.event_idx)
from (values row(0, 'owner_a', 'run_a', 1, 10),
             row(1, 'owner_b', 'run_b', 2, 10)) r(ordinal, uid, rid, lo, hi)
join event_index_repro e force index (idx_control)
  on e.user_id = r.uid and e.run_id = r.rid
 and e.event_type = 'user_intent'
 and e.event_idx > r.lo and e.event_idx <= r.hi
where e.run_id in ('run_a', 'run_b')
group by r.ordinal;

-- The scalar control already has a bounded composite-index lookup.
-- @separator:table
-- @regex("Index Table Scan.*idx_control",true)
explain
select max(event_idx) from event_index_repro force index (idx_control)
where user_id = 'owner_a' and run_id = 'run_a'
  and event_type = 'user_intent' and event_idx > 1 and event_idx <= 10;

insert into event_index_repro values
(1, 'owner_a', 'run_a', 'user_intent', 1),
(2, 'owner_a', 'run_a', 'user_intent', 2),
(3, 'owner_a', 'run_a', 'user_intent', 10),
(4, 'owner_a', 'run_a', 'user_intent', 11),
(5, 'owner_a', 'run_a', 'other', 99),
(6, 'owner_b', 'run_b', 'user_intent', 2),
(7, 'owner_b', 'run_b', 'user_intent', 3),
(8, 'owner_b', 'run_b', 'user_intent', 10),
(9, 'owner_b', 'run_b', 'user_intent', 11),
(10, 'owner_b', 'run_b', 'other', 99),
(11, 'owner_c', 'run_c', 'user_intent', 7),
(12, 'owner_a', 'run_b', 'user_intent', 99);

select r.ordinal, max(e.event_idx) as max_event_idx
from (values row(0, 'owner_a', 'run_a', 1, 10),
             row(1, 'owner_b', 'run_b', 2, 10),
             row(2, 'owner_a', 'run_a', 0, 2),
             row(3, 'owner_z', 'run_a', 0, 10),
             row(4, cast(null as varchar(128)), 'run_a', 0, 10)) r(ordinal, uid, rid, lo, hi)
join event_index_repro e force index (idx_control)
  on e.user_id = r.uid and e.run_id = r.rid
 and e.event_type = 'user_intent'
 and e.event_idx > r.lo and e.event_idx <= r.hi
where e.run_id in ('run_a', 'run_b')
group by r.ordinal
order by r.ordinal;

select 0 as ordinal, max(e.event_idx) as max_event_idx
from event_index_repro e force index (idx_control)
where e.user_id = 'owner_a' and e.run_id = 'run_a'
  and e.event_type = 'user_intent' and e.event_idx > 1 and e.event_idx <= 10
union all
select 1, max(e.event_idx)
from event_index_repro e force index (idx_control)
where e.user_id = 'owner_b' and e.run_id = 'run_b'
  and e.event_type = 'user_intent' and e.event_idx > 2 and e.event_idx <= 10
order by ordinal;

-- @separator:table
-- @regex("Index Table Scan.*idx_control",false)
explain
select r.ordinal, max(e.event_idx)
from (values row(0, 'owner_a', 'run_a', 1, 10),
             row(1, 'owner_b', 'run_b', 2, 10)) r(ordinal, uid, rid, lo, hi)
join event_index_repro e ignore index (idx_control)
  on e.user_id = r.uid and e.run_id = r.rid
 and e.event_type = 'user_intent'
 and e.event_idx > r.lo and e.event_idx <= r.hi
where e.run_id in ('run_a', 'run_b')
group by r.ordinal;

drop database issue_32460;
