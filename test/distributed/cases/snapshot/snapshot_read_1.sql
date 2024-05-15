create snapshot snapshot_01 for account sys;
create database if not exists snapshot_read;
use snapshot_read;
create snapshot snapshot_02 for account sys;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_01'};
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_02'};
drop snapshot snapshot_01;
drop snapshot snapshot_02;
drop database snapshot_read;

drop account if exists test_account;
create account test_account admin_name = 'test_user' identified by '111';
-- @session:id=2&user=test_account:test_user&password=111
create snapshot snapshot_01 for account test_account;
create database if not exists snapshot_read;
use snapshot_read;
create snapshot snapshot_02 for account test_account;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_01'};
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_02'};
drop snapshot snapshot_01;
drop snapshot snapshot_02;
drop database snapshot_read;
-- @session
drop account if exists test_account;



create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from test_snapshot_read;
create snapshot snapshot_01 for account sys;
drop table test_snapshot_read;
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_01'};
drop database if exists snapshot_read;
drop snapshot snapshot_01;


create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from test_snapshot_read;
create snapshot snapshot_01 for account sys;
drop database snapshot_read;
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_01'};
drop database if exists snapshot_read;
drop snapshot snapshot_01;
