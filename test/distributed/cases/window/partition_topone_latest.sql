-- @suite
-- @case
-- @desc: Partition Top-1 preserves latest NULL metrics and deterministic secondary ties
-- @label:bvt

drop database if exists partition_topone_latest;
create database partition_topone_latest;
use partition_topone_latest;
create table tags(id int primary key, hostname varchar(20));
create table readings(tags_id int, created_at datetime, seq int, metric int);
insert into tags values(1,'host-a'),(2,'host-b');
insert into readings values
(1,'2026-01-01 00:00:00',1,10),
(1,'2026-01-01 00:00:10',2,null),
(2,'2026-01-01 00:00:00',1,20),
(2,'2026-01-01 00:00:10',2,30),
(2,'2026-01-01 00:00:10',3,30),
(99,'2026-01-01 00:00:20',1,99);

-- The original TSBS Q4 shape: inner join, partition by tag ID, latest
-- timestamp, then rn=1. The missing tag stays excluded and NULL is payload.
-- Host-b's timestamp peers project identical values in this first query.
select hostname, created_at, metric
from (
    select t.hostname, r.created_at, r.metric,
           row_number() over (partition by r.tags_id order by r.created_at desc) as rn
    from readings r join tags t on r.tags_id=t.id
) ranked
where rn=1
order by hostname;

-- A secondary ORDER key makes the winning timestamp peer deterministic.
select tags_id, created_at, seq, metric
from (
    select r.*,
           row_number() over (partition by tags_id order by created_at desc, seq desc) as rn
    from readings r
) ranked
where rn=1
order by tags_id;

drop database partition_topone_latest;
