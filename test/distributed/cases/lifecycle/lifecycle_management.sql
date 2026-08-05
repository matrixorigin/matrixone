-- Lifecycle management-plane BVT.
-- Object retirement and Archive I/O remain in the asynchronous E2E/soak
-- suite; this case deliberately has no cron, checkpoint, or Provider wait.

drop account if exists lifecycle_bvt_acc;
create account lifecycle_bvt_acc admin_name = 'admin' identified by '111';

-- The cluster release gate must reject new bindings while disabled.
-- @ignore:0
select mo_feature_registry_upsert(
  'lifecycle',
  'TAE object Lifecycle',
  '{"archive_stages":[]}',
  false
);

-- @session:id=1&user=lifecycle_bvt_acc:admin&password=111
drop database if exists lifecycle_bvt;
create database lifecycle_bvt;
use lifecycle_bvt;
create table events(
  id bigint primary key,
  created_at timestamp not null,
  payload varchar(32)
);

alter table events set lifecycle (
  column created_at,
  expire after interval 7 day,
  action delete
);

-- @session
-- @ignore:0
select mo_feature_registry_upsert(
  'lifecycle',
  'TAE object Lifecycle',
  '{"archive_stages":[]}',
  true
);

-- @session:id=1&user=lifecycle_bvt_acc:admin&password=111
alter table events set lifecycle (
  column created_at,
  expire after interval 7 day,
  action delete
);

-- updated_at is intentionally ignored; every other public field is stable.
-- @ignore:6
show lifecycle for table events;

alter table events pause lifecycle;
-- @ignore:6
show lifecycle for table events;

alter table events resume lifecycle;
-- @ignore:6
show lifecycle for table events;

-- A bound base table must also reject indexes added later: Lifecycle retires
-- only the base Object and Phase 1 deliberately does not maintain index-child
-- Objects.
create index idx_events_created_at on events(created_at);

-- Empty result paths still exercise the public, bounded SHOW endpoints.
show lifecycle datasets for table events limit 1;
show lifecycle jobs limit 1;

-- Phase 1 rejects secondary indexes instead of leaving a binding that the
-- Object retire entry cannot keep consistent with an index child table.
create table indexed_events(
  id bigint primary key,
  created_at timestamp not null,
  key idx_created_at(created_at)
);
alter table indexed_events set lifecycle (
  column created_at,
  expire after interval 7 day,
  action delete
);

-- PAUSE and UNSET remain available after an emergency gate disable; RESUME
-- must not start new work while the gate is closed.
-- @session
-- @ignore:0
select mo_feature_registry_upsert(
  'lifecycle',
  'TAE object Lifecycle',
  '{"archive_stages":[]}',
  false
);

-- @session:id=1&user=lifecycle_bvt_acc:admin&password=111
alter table events pause lifecycle;
alter table events resume lifecycle;
alter table events unset lifecycle;
show lifecycle for table events;

drop table indexed_events;
drop table events;
drop database lifecycle_bvt;

-- @session
drop account lifecycle_bvt_acc;
