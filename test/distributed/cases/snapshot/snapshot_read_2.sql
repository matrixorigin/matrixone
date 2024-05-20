create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from test_snapshot_read;
create snapshot snapshot_01 for account sys;
delete from test_snapshot_read where a <= 50;

create table t2(b int);
INSERT INTO t2 (b) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from t2;
create snapshot snapshot_02 for account sys;

drop table if exists t3;
create table t3(a int);
insert into t3 SELECT * FROM test_snapshot_read{snapshot = 'snapshot_01'};
select count(*) from t3;
select count(*) from test_snapshot_read {snapshot = 'snapshot_01'};

-- 
SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read AS tsr INNER JOIN t2 ON tsr.a = t2.b;
SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read{snapshot = 'snapshot_01'} AS tsr INNER JOIN t2 ON tsr.a = t2.b;
-- t3 join t2
SELECT COUNT(tsr.a) AS total_sum FROM t3 AS tsr INNER JOIN t2 ON tsr.a = t2.b;

delete from t2 where b > 60;
SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read AS tsr INNER JOIN t2 ON tsr.a = t2.b;
SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read {snapshot = 'snapshot_01'} AS tsr INNER JOIN t2 ON tsr.a = t2.b;
SELECT COUNT(tsr.a) AS total_sum FROM t3 AS tsr INNER JOIN t2 ON tsr.a = t2.b;

drop table if exists t4;
create table t4(b int);
insert into t4 SELECT * FROM t2 {snapshot = 'snapshot_02'};
select count(*) from t4;
select count(*) from t2{snapshot = 'snapshot_02'};
SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read {snapshot = 'snapshot_01'} AS tsr  INNER JOIN t2 {snapshot = 'snapshot_02'} ON tsr.a = t2.b ;
SELECT COUNT(tsr.a) AS total_sum FROM t3 AS tsr INNER JOIN t2 {snapshot = 'snapshot_02'} ON tsr.a = t2.b ;
SELECT COUNT(tsr.a) AS total_sum FROM t3 AS tsr INNER JOIN t4 ON tsr.a = t4.b;
SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read {snapshot = 'snapshot_01'} AS tsr  INNER JOIN t4 ON tsr.a = t4.b;


SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read {snapshot = 'snapshot_02'}  AS tsr INNER JOIN t2 {snapshot = 'snapshot_02'} ON tsr.a = t2.b;
SELECT COUNT(tsr.a) AS total_sum FROM test_snapshot_read AS tsr  INNER JOIN t2 {snapshot = 'snapshot_02'} ON tsr.a = t2.b;

drop table if exists t3;
create table t3(c int);
insert into t3 SELECT tsr.a AS total_sum FROM test_snapshot_read {snapshot = 'snapshot_01'}  AS tsr INNER JOIN t2 {snapshot = 'snapshot_02'} ON tsr.a = t2.b ;
select count(*) from t3;
drop database snapshot_read;
drop snapshot snapshot_01;
drop snapshot snapshot_02;
