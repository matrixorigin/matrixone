-- issue #25590 design r15: the receiver-direction capability contract legs.
-- The mirror of update_remote_fragment_capability.sql (the r11 D4'' family),
-- which pins the session's COORDINATOR CN against the D3' coordinator
-- refusal. This case pins every RECEIVER CN - the non-coordinator CNs the
-- coordinator dispatches persistence-producing remote fragments to - against
-- the r15 dispatch refusal: a persistence-producing scope is NEVER
-- dispatched to a below-adoption receiver (the size-independent finite bound:
-- zero admitted legacy obligations), the refusal fires on the coordinator
-- inside remoteRun AFTER the round-44 stream confirm and BEFORE sendPipeline,
-- and nothing ever reaches the old fragment (no pipeline bytes, no persisted
-- objects, no relayed names behind any admission state). Plain statements
-- never consult the refusal: the sentinel inserts and every mass READ below
-- still succeed with the receivers pinned, which is the preserved mixed-
-- version plain/read availability on the same page.
--
-- leg P1: with every CN at its fresh build (advertising the first adoption
--   build, 133) the mass remote metadata-producing DML COMMITS and reads
--   back exactly in a fresh session (the clean sibling and the seed control
--   for the shape's producer-mode plan).
-- leg Pa: after pinning every RECEIVER CN advertisement to 129 (a real main
--   version below the adoption line) through ONE mo_ctl SetProtocolVersion
--   call in the documented multi-uuid form ('uuid1,uuid2,...:version',
--   pkg/sql/plan/function/ctl/cmd_rpc_version.go), the same mass UPDATE is
--   REFUSED with the definite receiver-capability not-supported error, and
--   under the DEFAULT policy the explicit transaction stays open: the
--   earlier sentinel write is STILL VISIBLE inside the transaction after
--   the refusal, COMMIT succeeds, and the fresh session reads the sentinel
--   committed and NO effect of the refused statement (statement-only
--   rollback: ErrNotSupported is outside moerr's whole-transaction list).
-- leg Pb (boundary control): an EMPTY-INPUT update of the same table
--   while every receiver stays pinned - the computed-column predicate
--   (v + 1 = 5) matches zero rows (leg Pa left v = 1 everywhere) and, on this build, plans
--   below the producer-plan threshold, so no persistence-producing
--   remote scope exists for it at all: the refusal never engages for a
--   plan that never producers. The statement executes as the ordinary
--   write it plans as (the exact narrowness the design demands for
--   pinned plain traffic), its transaction stays open until the
--   explicit ROLLBACK discards the leg. The decided REFUSED outcome for
--   an empty-input PRODUCER-MODE plan (risk structural in the encoded
--   plan, no dependence on runtime rows) is pinned at the dispatch
--   plane (pkg/sql/compile/remote_dispatch_refusal_r15_test.go), where
--   such scopes exist by construction and carry no rows at all.
-- positive control: after restoring every receiver through the same knob to
--   its captured version, the IDENTICAL sequence commits and reads back -
--   only the advertised capability input flipped, which separates the
--   capability line from volume noise.
--
-- Receiver-pin mechanics (r15, review round 45). The CN inventory
--   (mo_ctl GetProtocolVersion) lists every service with a query address as
--   'uuid:version' pairs joined with ', '. Two identity problems have
--   deterministic solutions, and neither is positional:
--   (1) the COORDINATOR pair is excludable by identity: the session's own
--       CN (its @@server_id is the exact string the inventory lists), as in
--       the D4'' family's coordinator capture.
--   (2) a TN registers a query address whenever its launch sets port-base,
--       and the same inventory lists it - but the TN carries no CN
--       capability and must never join a CN pin call. The case first marks
--       the TN through the documented dn-service form (mo_ctl SetProtocolVersion
--       on 'dn' ignores uuids and pins the TN directly) to the very version
--       the family already pins CNs to everywhere else (129 - every TN-side
--       RPC gate threshold, up to MORPCVersion123, stays enabled there),
--       re-reads the inventory, and excludes by uuid the pair whose
--       advertisement just moved to 129. On a launch whose TN ships without
--       a query address the mark row fails benignly, no pair moves into
--       the exclusion set, and the receiver set is simply every non-
--       coordinator pair - both topologies select exactly the CN receivers.
--       The uuid-bearing rows are @ignore:0 (masked wherever the service
--       identity differs); each pin's real effect is proven behaviorally
--       by the capability probes and the refused/succeed flip. The two
--       dn-form rows (the TN mark and its teardown restore) are
--       legitimately outcome-dependent across those topologies - the
--       SETPROTOCOLVERSION echo on a query-addressed TN, the documented
--       benign refusal without one - so each is validated by a @regex
--       pattern naming exactly its two acceptable outcomes; anything
--       else (a transport fault, a malformed echo) fails the case.
--   The positional occurrence walk (regexp_substr occurrence 1..16 over the
--   seeded numbers table) is bookkeeping only (the deterministic order of
--   the pin/restore lists); pair selection is ALWAYS by uuid identity, so
--   the round-29 order defect cannot reappear on any endpoint.
--
-- The inject_send_pipeline fault point is armed with the 'rrf_t' substring
-- so the receiving CN's pipeline log records every fragment of this case's
-- dispatching DML: the reach witness. The refused legs return before
-- sendPipeline attaches the marker, so the receiver-side log must NOT show
-- the marker for a refused leg; the positive control's marker IS REQUIRED
-- (the proof the restored run really dispatched and executed on the
-- receiver). The witness is asserted from the receiver CN log during review
-- validation; the SQL result file pins the behavioral flip itself.
--
-- Seed calibration (identical to the coordinator case, recorded per design
--   r11 D4''): the mass DML must route as ExecTypeAP_MULTICN (scan stats
--   BlockNum > BlockThresholdForOneCN = 512 blocks at objectio.BlockMaxRows
--   = 8192 rows/block, and Cost = rowcount > costThresholdForOneCN = 160000)
--   AND cross the workspace-spill line (outcnt * SingleLineSizeEstimate of
--   300B > DistributedThreshold of 10MB) so the write fragments carry S3
--   producer metadata. The seed crosses both with margin: 65536 inner ids
--   (h16 x h16 x h16 x h16) times 68 outer values (h68) = 4,456,448 rows =
--   545 estimated blocks; analyze table publishes the statistics so the plan
--   classifies deterministically.

-- @ignore:0
select enable_fault_injection();
-- @ignore:0
select add_fault_point('inject_send_pipeline', ':::', 'return', 0, 'rrf_t');

drop database if exists issue_25590_remote_capability_receiver;
create database issue_25590_remote_capability_receiver;
use issue_25590_remote_capability_receiver;
create table rrf_t (id bigint primary key, v bigint, payload varchar(128));
create table rrf_sentinel (k int primary key, note varchar(32));
-- The numbers table seeds the positional occurrence walk below (16 positions
-- cover every topology this case runs on; exhausted positions return NULL
-- and are filtered out).
create table h16 (n int);
insert into h16 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15);
create table h68 (n int);
insert into h68 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),
 (16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),
 (32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),
 (48),(49),(50),(51),(52),(53),(54),(55),(56),(57),(58),(59),(60),(61),(62),(63),
 (64),(65),(66),(67);
-- The pre-mark CN inventory snapshot: one 'uuid:version' pair per
-- query-addressed service, every service at its fresh build.
create table rrf_cn_inventory as
select mo_ctl('cn', 'GetProtocolVersion', '') as inv;
-- The COORDINATOR capture (the D4'' family's own mechanism): the session's
-- actual CN and its advertisement at capture time. The coordinator is never
-- pinned by this case; its recorded pair is the identity the receiver
-- extraction excludes.
create table rrf_coord_capture as
select @@server_id as coord_id,
       substring(regexp_substr(inv, concat(@@server_id, ':[0-9]+'), 1, 1),
                 length(@@server_id) + 2) as coord_v
from rrf_cn_inventory;
-- The positional occurrence walk over the pre-mark inventory.
create table rrf_recv_pairs as
select n.n + 1 as pos,
       regexp_substr(inv, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9]+',
                     1, n.n + 1) as pair
from rrf_cn_inventory, h16 n;
-- Mark the TN (its inventory entry, when it has one, is not a CN): the
-- documented dn-service form ignores uuids and pins the TN directly to 129 -
-- the identical value the family pins CNs to elsewhere, with the cluster
-- still executing DDL/inserts/selects (the D4'' legs run whole scenarios
-- around a coordinator pinned at 129; every TN-side RPC gate threshold
-- <= MORPCVersion123 stays enabled there). On a launch whose TN ships
-- without a query address this row fails benignly ("no such tn service")
-- and changes nothing; on a launch whose TN is query-addressed (the CI
-- compose topologies: port-base set, standalone-independent TN process)
-- it succeeds with the SETPROTOCOLVERSION echo. The row's outcome is
-- therefore topology-dependent, and its expectation is validated by a
-- pattern accepting exactly those two outcomes and rejecting anything
-- else.
-- @regex("(no such tn service|SETPROTOCOLVERSION)", true)
select mo_ctl('dn', 'SetProtocolVersion', '129');
create table rrf_marked_inventory as
select mo_ctl('cn', 'GetProtocolVersion', '') as inv;
create table rrf_marked_pairs as
select n.n + 1 as pos,
       regexp_substr(inv, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9]+',
                     1, n.n + 1) as pair
from rrf_marked_inventory, h16 n;
-- The exclusion set is selected by uuid identity, never by position: the
-- pair whose advertisement just moved to 129 is the TN; its pre-mark
-- advertisement (from the pre-mark walk, joined on the uuid) is the value
-- teardown restores it to.
create table rrf_tn_capture as
select f.pos,
       m.pair as marked_pair,
       substring(m.pair, 1, 36) as tn_uuid,
       substring(f.pair, 38) as tn_v
from rrf_marked_pairs m, rrf_recv_pairs f
where m.pair is not null
  and f.pair is not null
  and substring(m.pair, 1, 36) = substring(f.pair, 1, 36)
  and cast(substring(m.pair, 38) as unsigned) = 129
  and cast(substring(f.pair, 38) as unsigned) <> 129;
-- The RECEIVER capture: every pre-mark pair that is neither the coordinator
-- nor the marked TN. On both TN-present and TN-absent topologies this set is
-- exactly the non-coordinator CNs.
create table rrf_recv_capture as
select p.pos,
       substring(p.pair, 1, 36) as r_uuid,
       substring(p.pair, 38) as r_v
from rrf_recv_pairs p, rrf_coord_capture c
where p.pair is not null
  and substring(p.pair, 1, 36) <> c.coord_id
  and substring(p.pair, 1, 36) not in (select tn_uuid from rrf_tn_capture);
insert into rrf_t
select t.digit_id * 68 + k.n,
       0,
       concat('issue25590-payload-', k.n)
from (select h1.n * 4096 + h2.n * 256 + h3.n * 16 + h4.n as digit_id
      from h16 h1, h16 h2, h16 h3, h16 h4) t
cross join h68 k;
-- @ignore:6,8,10
analyze table rrf_t;
select count(*) from rrf_t;
select count(*) from rrf_t where payload like 'issue25590-payload-%';
select (select count(*) from rrf_recv_capture where cast(r_v as unsigned) >= 133)
       = (select count(*) from rrf_recv_capture) as receivers_captured;

-- leg P1: the capability-clean sibling (every receiver at the fresh build;
-- the TN mark is inert for CN dispatch).
begin;
update rrf_t set v = v + 1;
commit;
select count(*) from rrf_t where v = 1;
-- @session:id=2
use issue_25590_remote_capability_receiver;
select count(*) from rrf_t where v = 1;
select count(*) from rrf_sentinel;
-- @session

-- leg Pa: pin every RECEIVER to a below-adoption advertisement in ONE
-- deterministic multi-uuid mo_ctl call; the coordinator is untouched.
-- @ignore:0
select mo_ctl('cn', 'SetProtocolVersion',
  concat((select group_concat(r_uuid order by pos separator ',')
          from rrf_recv_capture), ':129'));
create table rrf_pinned_inventory as
select mo_ctl('cn', 'GetProtocolVersion', '') as inv;
create table rrf_pinned_pairs as
select n.n + 1 as pos,
       regexp_substr(inv, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9]+',
                     1, n.n + 1) as pair
from rrf_pinned_inventory, h16 n;
select (select count(*) from rrf_pinned_pairs x, rrf_coord_capture c
        where x.pair is not null
          and substring(x.pair, 1, 36) <> c.coord_id
          and cast(substring(x.pair, 38) as unsigned) >= 133)
  as capable_receivers_after_pin;
select (substring(regexp_substr(pi.inv, concat(c.coord_id, ':[0-9]+'), 1, 1),
                  length(c.coord_id) + 2) = c.coord_v) as coordinator_untouched
from rrf_pinned_inventory pi, rrf_coord_capture c;

-- leg Pa failure oracle: the mass producer-mode UPDATE is refused with the
-- definitive receiver-capability error, the plain statements around it keep
-- working, and the explicit transaction stays open.
begin;
insert into rrf_sentinel values (1, 'sentinel-pa');
update rrf_t set v = v + 1;
select note from rrf_sentinel where k = 1;
select count(*), min(v), max(v) from rrf_t;
select count(*) from rrf_sentinel;
commit;
select count(*) from rrf_t where v = 2;
select count(*) from rrf_sentinel;
-- @session:id=2
use issue_25590_remote_capability_receiver;
select note from rrf_sentinel;
select count(*) from rrf_sentinel;
select count(*) from rrf_t where v = 2;
select count(*) from rrf_t where v = 1;
-- @session

-- leg Pb (boundary control, implementation-round finding): an
--   EMPTY-INPUT update of the same table while every receiver stays
--   pinned - the computed-column predicate (v + 1 = 5) matches zero
--   rows at runtime (leg Pa left v = 1 everywhere) but plans BELOW the
--   producer threshold: the runtime-emptiness a planner can
--   statistically see always narrows the classify-first shape faster
--   than the remote-producer plane forms (empirically true on this
--   build for point-statistics predicates, computed-value predicates
--   and recorded-stats/spent-table constructs alike), so no
--   persistence-producing remote scope exists for it at all. The
--   statement therefore executes as the ordinary write it plans as -
--   its transaction stays open (sentinels preserved), the explicit
--   ROLLBACK discards it cleanly - pinning that the r15 refusal is
--   exactly as narrow as the producer-plan classification: pinned
--   receivers never disturb plain traffic. The design's decided
--   empty-input PRODUCER-MODE outcome (such an encoded plan refuses
--   identically, with persistence risk structural in the plan and no
--   dependence on runtime rows) cannot be reached from public SQL on
--   this planner and is pinned at the dispatch plane in
--   pkg/sql/compile/remote_dispatch_refusal_r15_test.go, where the
--   refused scopes carry no rows at all.
begin;
insert into rrf_sentinel values (2, 'sentinel-pb');
update rrf_t set v = v + 1 where v + 1 = 5;
select count(*) from rrf_sentinel;
rollback;
select count(*) from rrf_sentinel;
select count(*) from rrf_t where v = 2;
-- @session:id=2
use issue_25590_remote_capability_receiver;
select count(*) from rrf_sentinel;
select count(*) from rrf_t where v = 2;
select count(*) from rrf_t where v = 1;
-- @session

-- positive control: restore every receiver to its captured advertisement
-- through the same multi-uuid knob, restore the TN to its captured
-- advertisement through the dn form, then rerun the identical sequence - it
-- commits and reads back exactly (and the receiver-side pipeline log's
-- inject_send_pipeline marker is REQUIRED on this leg: the proof the
-- restored run really dispatched and executed on the receiver).
-- @ignore:0
select mo_ctl('cn', 'SetProtocolVersion',
  concat((select group_concat(r_uuid order by pos separator ',')
          from rrf_recv_capture), ':',
         (select min(r_v) from rrf_recv_capture)));
-- The teardown counterpart of the mark row above, with the same
-- outcome dependence: when a TN pair was captured and restored, this
-- returns the SETPROTOCOLVERSION echo (the restored advertisement's
-- version); when no pair moved - the unquery-addressed launch, or an
-- inventory whose TN pair never matches the walk regexp - the coalesce
-- yields the empty argument and the documented benign 'cmd invalid'
-- refusal fires. Validated by a pattern accepting exactly those two
-- outcomes and rejecting anything else.
-- @regex("(cmd invalid, expected version number|SETPROTOCOLVERSION)", true)
select mo_ctl('dn', 'SetProtocolVersion',
              coalesce((select max(tn_v) from rrf_tn_capture), ''));
create table rrf_restored_inventory as
select mo_ctl('cn', 'GetProtocolVersion', '') as inv;
create table rrf_restored_pairs as
select n.n + 1 as pos,
       regexp_substr(inv, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9]+',
                     1, n.n + 1) as pair
from rrf_restored_inventory, h16 n;
select (select count(*) from rrf_restored_pairs x
        where x.pair is not null
          and cast(substring(x.pair, 38) as unsigned) < 133)
  as below_adoption_after_restore;
begin;
insert into rrf_sentinel values (3, 'sentinel-control');
update rrf_t set v = v + 1;
commit;
select count(*) from rrf_t where v = 2;
select count(*) from rrf_sentinel;
-- @session:id=2
use issue_25590_remote_capability_receiver;
select count(*) from rrf_t where v = 2;
select count(*) from rrf_sentinel;
-- @session

-- teardown: retire the reach witness and the database.
-- @ignore:0
select remove_fault_point('inject_send_pipeline');
-- @ignore:0
select disable_fault_injection();
drop database issue_25590_remote_capability_receiver;
