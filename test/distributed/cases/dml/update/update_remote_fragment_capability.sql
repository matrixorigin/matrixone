-- issue #25590: the same-version remote write persistence contract.
-- Topology: a multi-CN cluster (CI's distributed runner runs 3 CNs; the
-- handler's local validation runs 2 CNs). This session connects to one CN,
-- the coordinator of every statement below.
--
-- The whole cluster runs one build of this tree; every peer is the same
-- version, so nothing pins a protocol advertisement or refuses a build (the
-- round-51/52 reviews removed all per-feature protocol machinery - the
-- ownership contract is same-version scope, and its guarantee holds in
-- every stream mode through the run-scoped barrier). The leg proves the
-- mass remote metadata-producing DML COMMITS and reads back exactly in a
-- fresh session, with the receiving CN's pipeline consulted for the write
-- fragments.
-- The mo_ctl-free body needs no @ignore masks beyond the fault-injection
-- rows and the ANALYZE statistics sampler: the inject_send_pipeline fault
-- point is armed with the 'rfc_t' substring so the receiving CN's pipeline
-- log records every fragment of this case's DML (the reach witness), then
-- removed during teardown.
--
-- Seed calibration (kept from the capability corpus): the mass DML must
-- route as ExecTypeAP_MULTICN (scan stats BlockNum >
-- BlockThresholdForOneCN = 512 blocks at objectio.BlockMaxRows = 8192
-- rows/block, and Cost = rowcount > costThresholdForOneCN = 160000,
-- pkg/sql/plan/stats.go:46-49) AND must cross the workspace-spill line
-- (outcnt * SingleLineSizeEstimate of 300B > DistributedThreshold of 10MB,
-- pkg/sql/compile/compile.go:120-121) so the write fragments carry S3
-- producer metadata. The seed crosses both with margin: 65536 inner ids
-- (h16 x h16 x h16 x h16) times 68 outer values (h68) = 4,456,448 rows =
-- 545 estimated blocks; analyze table publishes the statistics so the plan
-- classifies deterministically.

-- @ignore:0
select enable_fault_injection();
-- @ignore:0
select add_fault_point('inject_send_pipeline', ':::', 'return', 0, 'rfc_t');

drop database if exists issue_25590_remote_capability;
create database issue_25590_remote_capability;
use issue_25590_remote_capability;
create table rfc_t (id bigint primary key, v bigint, payload varchar(128));
create table rfc_sentinel (k int primary key, note varchar(32));
create table h16 (n int);
insert into h16 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15);
create table h68 (n int);
insert into h68 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),
 (16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),
 (32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),
 (48),(49),(50),(51),(52),(53),(54),(55),(56),(57),(58),(59),(60),(61),(62),(63),
 (64),(65),(66),(67);
insert into rfc_t
select t.digit_id * 68 + k.n,
       0,
       concat('issue25590-payload-', k.n)
from (select h1.n * 4096 + h2.n * 256 + h3.n * 16 + h4.n as digit_id
      from h16 h1, h16 h2, h16 h3, h16 h4) t
cross join h68 k;
-- @ignore:6,8,10
analyze table rfc_t;
select count(*) from rfc_t;
select count(*) from rfc_t where payload like 'issue25590-payload-%';

-- leg P1 (same-version): the mass remote metadata-producing DML commits
-- and reads back exactly in a fresh session.
begin;
update rfc_t set v = v + 1;
commit;
select count(*) from rfc_t where v = 1;
-- @session:id=2
use issue_25590_remote_capability;
select count(*) from rfc_t where v = 1;
select count(*) from rfc_sentinel;
-- @session

-- teardown: retire the reach witness and the database.
-- @ignore:0
select remove_fault_point('inject_send_pipeline');
-- @ignore:0
select disable_fault_injection();
drop database issue_25590_remote_capability;
