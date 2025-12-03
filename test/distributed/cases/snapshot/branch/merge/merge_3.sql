drop database if exists test_null_merge;
create database test_null_merge;
use test_null_merge;

-- case 1: merge between branches sharing an LCA while NULL columns flip direction
create table payout_template (
    batch_id int,
    region varchar(8),
    total_count int,
    payout_amount decimal(12,2),
    approved_at timestamp,
    reviewer varchar(20),
    comments varchar(50),
    review_hash varbinary(6),
    escalation_reason varchar(40),
    primary key(batch_id)
);

insert into payout_template values
(10, 'east', 5, 1200.50, '2024-02-28 08:30:00', 'amy', 'baseline review', x'AAAA11', null),
(20, 'west', 3, null, null, null, 'requires data', null, 'missing invoices'),
(30, null, 7, 4800.00, '2024-02-28 10:00:00', 'leo', null, x'BBBB22', 'stale approvals');

data branch create table payout_stage from payout_template;
data branch create table payout_ops from payout_template;

update payout_stage
   set payout_amount = null,
       reviewer = null,
       escalation_reason = 'manual hold'
 where batch_id = 10;

update payout_stage
   set approved_at = '2024-03-01 12:00:00',
       reviewer = 'nina',
       comments = null
 where batch_id = 20;

insert into payout_stage values
(40, 'north', null, 2500.00, null, null, 'ad-hoc bonus', null, null);

update payout_ops
   set payout_amount = 1250.75,
       comments = 'auto adjusted',
       review_hash = null
 where batch_id = 10;

update payout_ops
   set total_count = null,
       payout_amount = null,
       escalation_reason = null
 where batch_id = 30;

insert into payout_ops values
(50, 'south', 2, null, null, 'kai', null, x'CCCC33', 'new workflow');

data branch diff payout_stage against payout_ops;
data branch merge payout_stage into payout_ops;
data branch merge payout_stage into payout_ops when conflict skip;
select batch_id, region, total_count, payout_amount, reviewer, escalation_reason
  from payout_ops
 order by batch_id;
data branch merge payout_stage into payout_ops when conflict accept;
select batch_id, region, total_count, payout_amount, reviewer, escalation_reason
  from payout_ops
 order by batch_id;

drop table payout_template;
drop table payout_stage;
drop table payout_ops;

-- case 2: merge tables without an LCA that rely on NULL heavy deltas
create table audit_left (
    job_id int,
    shard char(3),
    seq int,
    checksum varbinary(4),
    status varchar(8),
    payload varchar(20),
    metric decimal(8,3),
    event_time timestamp,
    primary key(job_id, shard)
);

insert into audit_left values
(101, 'a01', 1, x'AA11', 'open', 'inventory gap', null, '2024-03-02 09:00:00'),
(102, 'b02', null, null, 'pending', null, 1.250, '2024-03-02 10:30:00'),
(103, 'c03', 5, x'BB22', 'closed', 'retry', null, null);

create table audit_right (
    job_id int,
    shard char(3),
    seq int,
    checksum varbinary(4),
    status varchar(8),
    payload varchar(20),
    metric decimal(8,3),
    event_time timestamp,
    primary key(job_id, shard)
);

insert into audit_right values
(101, 'a01', 2, null, null, 'inventory gap', 0.750, '2024-03-03 11:00:00'),
(103, 'c03', 5, x'BB22', 'closed', 'retry', 1.500, '2024-03-02 08:30:00'),
(104, 'd04', 1, x'CC33', 'open', null, null, null);

data branch diff audit_right against audit_left;
-- data branch merge audit_right into audit_left;
-- internal error: conflict: audit_right INSERT and audit_left INSERT on pk(101,'a01') with different values
data branch merge audit_right into audit_left when conflict skip;
select job_id, shard, seq, status, payload, metric
  from audit_left
 order by job_id, shard;
data branch merge audit_right into audit_left when conflict accept;
select job_id, shard, seq, status, payload, metric
  from audit_left
 order by job_id, shard;

drop table audit_left;
drop table audit_right;

drop database test_null_merge;
