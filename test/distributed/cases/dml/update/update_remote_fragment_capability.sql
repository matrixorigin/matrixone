-- issue #25590 design r11 D4'': the remote-write capability contract legs.
-- Topology: a multi-CN cluster (CI's distributed runner runs 3 CNs; the
-- handler's local validation runs 2 CNs). This session connects to one CN,
-- the coordinator of every statement below.
--
-- leg P1: with every CN at its fresh build (advertising the first adoption
--   build) the mass remote metadata-producing DML COMMITS and reads back
--   exactly in a fresh session.
-- leg P2a: after pinning the session's COORDINATOR CN (the CN this
--   session is connected to, @@server_id) to the pre-merge advertisement
--   (129, mo_ctl 'SetProtocolVersion') the same mass UPDATE is REFUSED with
--   the definite not-supported error, and under the DEFAULT policy the
--   transaction stays open: the earlier sentinel write is STILL VISIBLE in
--   the same transaction after the refusal, COMMIT succeeds, and the fresh
--   session sees the sentinel committed and NO effect of the refused
--   statement (statement-only rollback by ErrNotSupported, outside moerr's
--   whole-transaction list).
-- leg P2b: the separate full-ROLLBACK leg - the same refusal, an explicit
--   ROLLBACK, and the fresh readback shows nothing changed.
-- positive control: after restoring the coordinator through the same knob
--   the identical sequence succeeds and commits - only the advertised version
--   input flipped, which separates the capability line from volume noise.
-- The mo_ctl rows are masked with @ignore:0 (dynamic CN-UUID output); the
-- pin's real effect is proven behaviorally by the refused/succeed flip.
-- The inject_send_pipeline fault point is armed with the 'rfc_t' substring
-- so the receiving CN's pipeline log records every fragment of this case's
-- DML (the reach witness), then removed during teardown.
--
-- Coordinator pin note (round-31 pre-PR review, finding 1): the pin
-- targets the session's ACTUAL coordinator CN - the CN this session
-- connects to, read directly from the session variable @@server_id
-- (pkg/frontend/variables.go resolveServerID returns the CN base
-- service UUID, the same identity GetProtocolVersion lists). The
-- refusal is issued by the coordinator itself against its own
-- advertised version, so pinning that one identity makes every
-- refusal leg deterministic on every topology and every endpoint:
-- whichever CN the runner routes this session to IS the pinned
-- coordinator. The identical case file and result file are validated
-- through EVERY CN endpoint on two-CN and three-CN clusters. The
-- round-29 inventory-order defect (a session landing on an unpinned
-- third CN silently succeeding the refusal legs, archived 55/65) is
-- eliminated at the root: no positional occurrence of an
-- order-sensitive enumeration is trusted anywhere in this case.
--
-- Seed calibration (recorded per design r11 D4''): the mass DML must route
-- as ExecTypeAP_MULTICN (scan stats BlockNum > BlockThresholdForOneCN = 512
-- blocks at objectio.BlockMaxRows = 8192 rows/block, and Cost =
-- rowcount > costThresholdForOneCN = 160000, pkg/sql/plan/stats.go:46-49)
-- AND must cross the workspace-spill line (outcnt * SingleLineSizeEstimate
-- of 300B > DistributedThreshold of 10MB, pkg/sql/compile/compile.go:120-121)
-- so the write fragments carry S3 producer metadata. The seed crosses both
-- with margin: 65536 inner ids (h16 x h16 x h16 x h16) times 68 outer
-- values (h68) = 4,456,448 rows = 545 estimated blocks; analyze table
-- publishes the statistics so the plan classifies deterministically.

-- @ignore:0
select enable_fault_injection();
-- @ignore:0
select add_fault_point('inject_send_pipeline', ':::', 'return', 0, 'rfc_t');

drop database if exists issue_25590_remote_capability;
create database issue_25590_remote_capability;
use issue_25590_remote_capability;
create table rfc_t (id bigint primary key, v bigint, payload varchar(128));
create table rfc_sentinel (k int primary key, note varchar(32));
-- Coordinator capture (round-31 pre-PR review, finding 1): the CN
-- inventory is snapshotted once, and the COORDINATOR row of that
-- snapshot is selected by the session's own @@server_id - the actual
-- CN this session connects to, which plans and refuses every
-- statement below - so no positional occurrence of an order-sensitive
-- enumeration is ever trusted. coord_v records the coordinator's
-- advertised version at capture time (the fresh build's version), so
-- the positive control restores the SAME endpoint. The verification
-- rows after the pin and after the restore are deterministic locate
-- probes over this captured pair, topology- and endpoint-stable.
create table rfc_cn_inventory as
select mo_ctl('cn', 'GetProtocolVersion', '') as inv;
create table rfc_coord_capture as
select @@server_id as coord_id,
       substring(regexp_substr(inv, concat(@@server_id, ':[0-9]+'), 1, 1),
                 length(@@server_id) + 2) as coord_v
from rfc_cn_inventory;
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

-- leg P1: the capability-clean sibling.
begin;
update rfc_t set v = v + 1;
commit;
select count(*) from rfc_t where v = 1;
-- @session:id=2
use issue_25590_remote_capability;
select count(*) from rfc_t where v = 1;
select count(*) from rfc_sentinel;
-- @session

-- leg P2a: the default-policy refusal oracle. The pinned identity IS
-- the session's coordinator (@@server_id, captured above) - the CN
-- that plans and refuses the mass UPDATE below; the unmasked
-- verification row proves the coordinator now advertises the pinned
-- version.
-- @ignore:0
select mo_ctl('cn', 'SetProtocolVersion', concat((select coord_id from rfc_coord_capture), ':129'));
select (locate(concat((select coord_id from rfc_coord_capture), ':129'),
               mo_ctl('cn', 'GetProtocolVersion', '')) > 0)
  as coordinator_pinned_129;
begin;
insert into rfc_sentinel values (1, 'sentinel-p2a');
update rfc_t set v = v + 1;
select note from rfc_sentinel where k = 1;
select count(*), min(v), max(v) from rfc_t;
select count(*) from rfc_sentinel;
commit;
select count(*) from rfc_t where v = 2;
select count(*) from rfc_sentinel;
-- @session:id=2
use issue_25590_remote_capability;
select note from rfc_sentinel;
select count(*) from rfc_sentinel;
select count(*) from rfc_t where v = 2;
select count(*) from rfc_t where v = 1;
-- @session

-- leg P2b: the separate full-ROLLBACK leg.
begin;
insert into rfc_sentinel values (2, 'sentinel-p2b');
update rfc_t set v = v + 1;
rollback;
select count(*) from rfc_sentinel;
select count(*) from rfc_t where v = 2;
-- @session:id=2
use issue_25590_remote_capability;
select count(*) from rfc_sentinel;
select count(*) from rfc_t where v = 2;
-- @session

-- positive control: restore the SAME captured coordinator endpoint to
-- its captured version (the fresh build advertised it at capture time)
-- and rerun the identical sequence - it succeeds and commits.
-- @ignore:0
select mo_ctl('cn', 'SetProtocolVersion',
  concat((select coord_id from rfc_coord_capture), ':', (select coord_v from rfc_coord_capture)));
select (locate(concat((select coord_id from rfc_coord_capture), ':', (select coord_v from rfc_coord_capture)),
               mo_ctl('cn', 'GetProtocolVersion', '')) > 0)
  as coordinator_restored;
begin;
insert into rfc_sentinel values (3, 'sentinel-control');
update rfc_t set v = v + 1;
commit;
select count(*) from rfc_t where v = 2;
select count(*) from rfc_sentinel;
-- @session:id=2
use issue_25590_remote_capability;
select count(*) from rfc_t where v = 2;
select count(*) from rfc_sentinel;
-- @session

-- teardown: retire the reach witness and the database.
-- @ignore:0
select remove_fault_point('inject_send_pipeline');
-- @ignore:0
select disable_fault_injection();
drop database issue_25590_remote_capability;
