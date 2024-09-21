-- @bvt:issue#17208
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
insert into test_snapshot_read (a) values(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;

-- @cleanup
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;
create snapshot cluster_sp for cluster;
create snapshot account_sp for account sys;
-- @ignore:1
show snapshots;

delete from snapshot_read.test_snapshot_read where a > 10;
insert into test_snapshot_read (a) values(11),(12), (13), (14), (15),(16), (17), (18), (19), (20);

select count(*) from snapshot_read.test_snapshot_read where a in (select a from snapshot_read.test_snapshot_read {snapshot = 'cluster_sp'});
select count(*) from snapshot_read.test_snapshot_read where a in (select a from snapshot_read.test_snapshot_read {snapshot = 'account_sp'});

select count(*) from snapshot_read.test_snapshot_read where a not in (select a from snapshot_read.test_snapshot_read {snapshot = 'cluster_sp'});
select count(*) from snapshot_read.test_snapshot_read where a not in (select a from snapshot_read.test_snapshot_read {snapshot = 'account_sp'});

select count(*) from snapshot_read.test_snapshot_read {snapshot = 'cluster_sp'} where a in (select a from snapshot_read.test_snapshot_read);
select count(*) from snapshot_read.test_snapshot_read {snapshot = 'account_sp'} where a in (select a from snapshot_read.test_snapshot_read);

select count(*) from snapshot_read.test_snapshot_read {snapshot = 'cluster_sp'} where a not in (select a from snapshot_read.test_snapshot_read);
select count(*) from snapshot_read.test_snapshot_read {snapshot = 'account_sp'} where a not in (select a from snapshot_read.test_snapshot_read);
-- @cleanup
drop snapshot if exists cluster_sp;
drop database if exists snapshot_read;
-- @bvt:issue
