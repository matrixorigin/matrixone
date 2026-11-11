-- Issue #32410: fixed leading unique-key parts plus a trailing IN must use bounded access.
drop database if exists composite_unique_in_index;
create database composite_unique_in_index;
use composite_unique_in_index;
create table digits (n int primary key);
insert into digits values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
create table events (
  id varchar(64) not null, user_id varchar(128) not null,
  run_id varchar(64) not null, session_id varchar(64) not null,
  event_idx bigint not null, event_type varchar(64) not null,
  idempotency_key varchar(128), payload_json text not null,
  primary key (user_id,id),
  unique key uq_user_run_key (user_id,run_id,idempotency_key),
  key idx_session (user_id,session_id,run_id,event_idx),
  key idx_type (user_id,run_id,event_type,event_idx)
);
insert into events
select concat('id',a.n+b.n*10+c.n*100+d.n*1000),
       'target','long-run','session',a.n+b.n*10+c.n*100+d.n*1000,'tool_result',
       case a.n+b.n*10+c.n*100+d.n*1000
         when 1 then 'user_intent:x'
         when 2 then 'user_intent_applied:x'
         when 3 then 'user_intent_returned:x'
         else concat('event:',a.n+b.n*10+c.n*100+d.n*1000) end,
       '{}'
from digits a cross join digits b cross join digits c cross join digits d
where a.n+b.n*10+c.n*100+d.n*1000 < 5000;
select count(*) from events;

-- Equality remains the singleton control.
-- @regex("Index Table Scan on events[.]uq_user_run_key",true)
-- @regex("Table Scan on composite_unique_in_index[.]events[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=1\s+outputRows=1\b",true)
explain analyze select payload_json,event_idx,session_id from events
where user_id='target' and run_id='long-run'
  and idempotency_key='user_intent:x';

-- The session residual and non-covered projection require base-table backfill.
-- @regex("Index Table Scan on events[.]uq_user_run_key",true)
-- @regex("Table Scan on composite_unique_in_index[.]events[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=3\s+outputRows=3\b",true)
explain analyze select payload_json,event_idx,session_id from events
where user_id='target' and run_id='long-run' and session_id='session'
  and idempotency_key in ('user_intent:x','user_intent_applied:x','user_intent_returned:x');
-- @separator:space
select event_idx,session_id from events
where user_id='target' and run_id='long-run' and session_id='session'
  and idempotency_key in ('user_intent:x','user_intent_applied:x','user_intent_returned:x')
order by event_idx;

-- A hint selects the same bounded access path when ORDER BY needs another key.
-- @regex("Index Table Scan on events[.]uq_user_run_key",true)
-- @regex("Table Scan on composite_unique_in_index[.]events[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=3\s+outputRows=3\b",true)
explain analyze select event_idx,session_id,payload_json from events force index(uq_user_run_key)
where user_id='target' and run_id='long-run' and session_id='session'
  and idempotency_key in ('user_intent:x','user_intent_applied:x','user_intent_returned:x')
order by event_idx;
-- @separator:space
select event_idx,session_id from events force index(uq_user_run_key)
where user_id='target' and run_id='long-run' and session_id='session'
  and idempotency_key in ('user_intent:x','user_intent_applied:x','user_intent_returned:x')
order by event_idx;

-- IN has set semantics; duplicate and absent keys do not create rows.
select event_idx from events
where user_id='target' and run_id='long-run'
  and idempotency_key in ('user_intent:x','user_intent:x','absent')
order by event_idx;

-- Prepared values remain runtime constants, including NULL on reuse.
prepare in_stmt from 'select event_idx from events where user_id=? and run_id=? and idempotency_key in (?,?,?) order by event_idx';
set @u='target', @r='long-run', @k1='user_intent:x', @k2='user_intent_applied:x', @k3='user_intent_returned:x';
execute in_stmt using @u,@r,@k1,@k2,@k3;
set @k3=null;
execute in_stmt using @u,@r,@k1,@k2,@k3;
set @k1='absent', @k2='absent', @k3='absent';
execute in_stmt using @u,@r,@k1,@k2,@k3;
deallocate prepare in_stmt;

-- NULL unique-key rows and NULL list items retain SQL three-valued semantics.
insert into events values ('null-key','target','long-run','session',5000,'tool_result',null,'{"null":true}');
select event_idx from events
where user_id='target' and run_id='long-run'
  and idempotency_key in ('user_intent:x',null,'user_intent_applied:x')
order by event_idx;

-- Incomplete prefixes and IN on a nontrailing part keep their fallback semantics.
select event_idx from events
where user_id='target'
  and idempotency_key in ('user_intent:x','user_intent_applied:x','user_intent_returned:x')
order by event_idx;
select event_idx from events
where user_id in ('absent-user','target') and run_id='long-run'
  and idempotency_key='user_intent_returned:x'
order by event_idx;

-- Declared prefixes store truncated key parts. Exact full-value IN probes must
-- retain the base predicates and results instead of probing untruncated keys.
create table prefix_trailing (
  id int primary key, a int, b varchar(20), payload int,
  unique key uk_prefix_trailing (a,b(3))
);
insert into prefix_trailing values (1,1,'abcdef',7);
select payload from prefix_trailing force index(uk_prefix_trailing)
where a=1 and b in ('abcdef','ghijkl');
-- Unmatched ORDER BY must not expose full-value equality probes on prefix keys.
select payload from prefix_trailing force index(uk_prefix_trailing)
where a=1 and b='abcdef' order by payload;
select payload from prefix_trailing force index(uk_prefix_trailing)
where a=1 and b in ('abcdef') order by payload;

create table prefix_leading (
  id int primary key, a varchar(20), b int, payload int,
  unique key uk_prefix_leading (a(3),b)
);
insert into prefix_leading values (1,'abcdef',1,7);
select payload from prefix_leading force index(uk_prefix_leading)
where a='abcdef' and b in (1,2);
select payload from prefix_leading force index(uk_prefix_leading)
where a='abcdef' and b=1 order by payload;
select payload from prefix_leading force index(uk_prefix_leading)
where a='abcdef' and b in (1) order by payload;

-- One eligible full-value index must not expose a competing prefix index when
-- unmatched ORDER BY opens the shared filter phase. Composite-PK rows check results.
create table competing_prefix_first (
  tenant_id int not null, id int not null,
  a int, b varchar(20), c int, payload int,
  primary key (tenant_id,id),
  unique key uk_prefix (a,b(3)),
  unique key uk_full (a,c)
);
insert into competing_prefix_first values
  (1,1,1,'abcdef',3,7),
  (1,2,2,'abcdef',4,8);
select payload from competing_prefix_first force index(uk_prefix,uk_full)
where a=1 and b='abcdef' and c in (3,4) order by payload;
analyze table competing_prefix_first;
select payload from competing_prefix_first force index(uk_prefix,uk_full)
where a=1 and b='abcdef' and c in (3,4) order by payload;

create table competing_full_first (
  tenant_id int not null, id int not null,
  a int, b varchar(20), c int, payload int,
  primary key (tenant_id,id),
  unique key uk_full (a,c),
  unique key uk_prefix (a,b(3))
);
insert into competing_full_first values
  (1,1,1,'abcdef',3,7),
  (1,2,2,'abcdef',4,8);
select payload from competing_full_first force index(uk_prefix,uk_full)
where a=1 and b='abcdef' and c in (3,4) order by payload;
analyze table competing_full_first;
select payload from competing_full_first force index(uk_prefix,uk_full)
where a=1 and b='abcdef' and c in (3,4) order by payload;

-- Missing statistics must not let a multi-key trailing IN outrank a complete
-- unique equality. The uncovered payload requires base-row backfill.
create table ranking_many_first (
  id int primary key, a int not null, b int not null, c int not null, payload int,
  unique key uk_many (a,b),
  unique key uk_one (a,c)
);
insert into ranking_many_first
select a.n+b.n*10+c.n*100+d.n*1000,1,
       a.n+b.n*10+c.n*100+d.n*1000,
       a.n+b.n*10+c.n*100+d.n*1000,
       7000+a.n+b.n*10+c.n*100+d.n*1000
from digits a cross join digits b cross join digits c cross join digits d
where a.n+b.n*10+c.n*100+d.n*1000 < 5000;
-- @regex("Index Table Scan on ranking_many_first[.]uk_one",true)
-- @regex("Index Table Scan on ranking_many_first[.]uk_one[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=1\s+outputRows=1\b",true)
-- @regex("Table Scan on composite_unique_in_index[.]ranking_many_first[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=1\s+outputRows=1\b",true)
explain analyze select payload from ranking_many_first
where a=1 and b in (1,2,3) and c=1;
select payload from ranking_many_first where a=1 and b in (1,2,3) and c=1;

create table ranking_one_first (
  id int primary key, a int not null, b int not null, c int not null, payload int,
  unique key uk_one (a,c),
  unique key uk_many (a,b)
);
insert into ranking_one_first
select a.n+b.n*10+c.n*100+d.n*1000,1,
       a.n+b.n*10+c.n*100+d.n*1000,
       a.n+b.n*10+c.n*100+d.n*1000,
       7000+a.n+b.n*10+c.n*100+d.n*1000
from digits a cross join digits b cross join digits c cross join digits d
where a.n+b.n*10+c.n*100+d.n*1000 < 5000;
-- @regex("Index Table Scan on ranking_one_first[.]uk_one",true)
-- @regex("Index Table Scan on ranking_one_first[.]uk_one[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=1\s+outputRows=1\b",true)
-- @regex("Table Scan on composite_unique_in_index[.]ranking_one_first[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=1\s+outputRows=1\b",true)
explain analyze select payload from ranking_one_first
where a=1 and b in (1,2,3) and c=1;
select payload from ranking_one_first where a=1 and b in (1,2,3) and c=1;
drop database composite_unique_in_index;
