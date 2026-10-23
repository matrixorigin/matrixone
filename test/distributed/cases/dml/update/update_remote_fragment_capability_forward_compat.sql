-- issue #25590 design r11 leg P3': the new-coordinator/old-fragment
-- forward-compatibility sibling of leg P1. The exact P1 sequence runs with
-- NO version pins and NO refusal rows: the coordinator CN runs the proposed
-- build (advertising the first adoption build) and the fragment receiver
-- runs whatever the cluster provides - on CI the same fresh build (this
-- file then re-proves leg P1), and in the handler's local two-binary
-- validation the UNCHANGED PARENT build, including its own protobuf and
-- producer/workspace machinery. The parent receiver skips the unknown
-- capability field (proto3) and runs base semantics; the mass remote DML
-- COMMITS and the fresh session reads the committed rows back.
-- The inject_send_pipeline fault point ('rfc_t') is armed so the receiving
-- CN's pipeline log records this file's DML fragments: on the mixed
-- cluster that log is the CN2 reach witness; it is removed at teardown.

-- @ignore:0
select enable_fault_injection();
-- @ignore:0
select add_fault_point('inject_send_pipeline', ':::', 'return', 0, 'rfc_t');

-- Seed calibration (same recorded basis as the D4'' capability case):
-- 65536 inner ids (h16^4) x 68 outer values = 4,456,448 rows = 545
-- estimated blocks, so the mass DML routes as ExecTypeAP_MULTICN
-- (BlockNum > 512, Cost > 160000) and crosses the workspace-spill
-- threshold; analyze table publishes the statistics deterministically.
drop database if exists issue_25590_remote_capability;
create database issue_25590_remote_capability;
use issue_25590_remote_capability;
create table rfc_t (id bigint primary key, v bigint, payload varchar(128));
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

-- leg P1's exact sequence, verbatim.
begin;
update rfc_t set v = v + 1;
commit;
select count(*) from rfc_t where v = 1;
-- @session:id=2
use issue_25590_remote_capability;
select count(*) from rfc_t where v = 1;
-- @session

-- teardown.
-- @ignore:0
select remove_fault_point('inject_send_pipeline');
-- @ignore:0
select disable_fault_injection();
drop database issue_25590_remote_capability;
