drop database if exists tpcc;
create database tpcc;
use tpcc;
create table bmsql_oorder (
  o_w_id       integer      not null,
  o_d_id       integer      not null,
  o_id         integer      not null,
  o_c_id       integer,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  o_entry_d    timestamp,
  primary key (o_w_id, o_d_id, o_id)
) PARTITION BY KEY(o_w_id) PARTITIONS 10;

INSERT INTO `bmsql_oorder` VALUES (5, 10, 2101, 2797, 2, 10, 1, '2024-05-22 11:55:57');
INSERT INTO `bmsql_oorder` VALUES (9, 6, 2106, 2106, 3, 5, 1, '2024-05-22 11:55:50');
INSERT INTO `bmsql_oorder` VALUES (1, 5, 2107, 2007, 4, 14, 1, '2024-05-22 11:55:48');
INSERT INTO `bmsql_oorder` VALUES (3, 10, 2116, 2116, 1, 10, 1, '2024-05-22 11:55:57');
INSERT INTO `bmsql_oorder` VALUES (4, 9, 2113, 1644, 4, 8, 1, '2024-05-22 11:55:53');
INSERT INTO `bmsql_oorder` VALUES (5, 9, 2113, 1206, 3, 13, 1, '2024-05-22 11:55:55');
INSERT INTO `bmsql_oorder` VALUES (3, 6, 2115, 816, NULL, 6, 1, '2024-05-22 11:55:48');
INSERT INTO `bmsql_oorder` VALUES (6, 10, 2116, 541, NULL, 8, 1, '2024-05-22 11:55:56');
INSERT INTO `bmsql_oorder` VALUES (6, 10, 2143, 2244, NULL, 12, 1, '2024-05-22 11:55:56');
INSERT INTO `bmsql_oorder` VALUES (2, 9, 2448, 627, NULL, 6, 1, '2024-05-22 11:55:56');
INSERT INTO `bmsql_oorder` VALUES (2, 9, 2471, 1137, NULL, 12, 1, '2024-05-22 11:55:56');
INSERT INTO `bmsql_oorder` VALUES (6, 1, 236, 2381, 7, 12, 1, '2024-05-22 11:55:38');
INSERT INTO `bmsql_oorder` VALUES (6, 1, 244, 2886, 4, 6, 1, '2024-05-22 11:55:38');
INSERT INTO `bmsql_oorder` VALUES (8, 5, 28, 2930, 5, 9, 1, '2024-05-22 11:55:47');
INSERT INTO `bmsql_oorder` VALUES (8, 5, 46, 1638, 1, 14, 1, '2024-05-22 11:55:47');
INSERT INTO `bmsql_oorder` VALUES (9, 4, 2816, 962, NULL, 7, 1, '2024-05-22 11:55:46');
INSERT INTO `bmsql_oorder` VALUES (1, 7, 1890, 1890, 3, 9, 1, '2024-05-22 11:55:51');
INSERT INTO `bmsql_oorder` VALUES (1, 9, 946, 946, 10, 15, 1, '2024-05-22 11:55:55');
INSERT INTO `bmsql_oorder` VALUES (1, 9, 950, 343, 2, 5, 1, '2024-05-22 11:55:55');
INSERT INTO `bmsql_oorder` VALUES (1, 9, 1005, 534, 2, 9, 1, '2024-05-22 11:55:55');
INSERT INTO `bmsql_oorder` VALUES (6, 9, 1321, 1272, 2, 8, 1, '2024-05-22 11:55:54');
INSERT INTO `bmsql_oorder` VALUES (6, 9, 1347, 775, 6, 9, 1, '2024-05-22 11:55:54');
INSERT INTO `bmsql_oorder` VALUES (4, 7, 1660, 2257, 5, 7, 1, '2024-05-22 11:55:49');
INSERT INTO `bmsql_oorder` VALUES (4, 7, 1672, 648, 5, 10, 1, '2024-05-22 11:55:49');
INSERT INTO `bmsql_oorder` VALUES (4, 7, 1766, 2804, 10, 9, 1, '2024-05-22 11:55:50');
INSERT INTO `bmsql_oorder` VALUES (5, 1, 1725, 2544, 9, 5, 1, '2024-05-22 11:55:38');
INSERT INTO `bmsql_oorder` VALUES (5, 1, 1842, 2982, 6, 5, 1, '2024-05-22 11:55:38');
INSERT INTO `bmsql_oorder` VALUES (9, 2, 1192, 1410, 10, 7, 1, '2024-05-22 11:55:41');
INSERT INTO `bmsql_oorder` VALUES (9, 2, 1199, 1199, 6, 7, 1, '2024-05-22 11:55:41');
INSERT INTO `bmsql_oorder` VALUES (3, 4, 1975, 731, 5, 14, 1, '2024-05-22 11:55:44');
INSERT INTO `bmsql_oorder` VALUES (3, 4, 2013, 120, 3, 12, 1, '2024-05-22 11:55:44');
INSERT INTO `bmsql_oorder` VALUES (6, 4, 1786, 1786, 9, 8, 1, '2024-05-22 11:55:45');
INSERT INTO `bmsql_oorder` VALUES (2, 2, 2610, 1049, NULL, 5, 1, '2024-05-22 11:55:41');
INSERT INTO `bmsql_oorder` VALUES (10, 5, 525, 1264, 9, 15, 1, '2024-05-22 11:55:46');
INSERT INTO `bmsql_oorder` VALUES (6, 7, 2147, 197, NULL, 14, 1, '2024-05-22 11:55:50');
INSERT INTO `bmsql_oorder` VALUES (6, 1, 2024, 1245, 9, 13, 1, '2024-05-22 11:55:39');
INSERT INTO `bmsql_oorder` VALUES (3, 2, 813, 1577, 8, 13, 1, '2024-05-22 11:55:40');
INSERT INTO `bmsql_oorder` VALUES (9, 8, 1438, 2048, 7, 10, 1, '2024-05-22 11:55:54');
INSERT INTO `bmsql_oorder` VALUES (10, 5, 387, 2815, 7, 13, 1, '2024-05-22 11:55:46');
INSERT INTO `bmsql_oorder` VALUES (6, 1, 2130, 1606, NULL, 11, 1, '2024-05-22 11:55:39');
INSERT INTO `bmsql_oorder` VALUES (9, 6, 2502, 1968, NULL, 13, 1, '2024-05-22 11:55:50');
INSERT INTO `bmsql_oorder` VALUES (9, 6, 2366, 1902, NULL, 5, 1, '2024-05-22 11:55:50');
INSERT INTO `bmsql_oorder` VALUES (9, 6, 2297, 565, NULL, 10, 1, '2024-05-22 11:55:50');
INSERT INTO `bmsql_oorder` VALUES (9, 6, 2309, 2309, NULL, 8, 1, '2024-05-22 11:55:50');
INSERT INTO `bmsql_oorder` VALUES (2, 6, 1641, 1375, 3, 12, 1, '2024-05-22 11:55:49');
INSERT INTO `bmsql_oorder` VALUES (10, 10, 2977, 1049, NULL, 13, 1, '2024-05-22 11:55:56');
INSERT INTO `bmsql_oorder` VALUES (10, 10, 2344, 1590, NULL, 11, 1, '2024-05-22 11:55:56');

select * from bmsql_oorder order by o_w_id, o_d_id, o_id;

--------------------------------------------------------------------------
-- @session:id=1&user=dump&password=111
use tpcc;
prepare __mo_stmt_id_4 from 'INSERT INTO bmsql_oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)';
prepare __mo_stmt_id_22 from 'SELECT o_id, o_entry_d, o_carrier_id FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT max(o_id) FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)';
prepare __mo_stmt_id_27 from 'SELECT o_c_id FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?';
prepare __mo_stmt_id_28 from 'UPDATE bmsql_oorder SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?';

SET @o_carrier_id = 7;
SET @o_w_id = 2;
SET @o_d_id = 7;
SET @o_id = 2105;
EXECUTE __mo_stmt_id_28 USING @o_carrier_id, @o_w_id, @o_d_id, @o_id;

SET @o_carrier_id = 2;
SET @o_w_id = 5;
SET @o_d_id = 10;
SET @o_id = 2101;
EXECUTE __mo_stmt_id_28 USING @o_carrier_id, @o_w_id, @o_d_id, @o_id;
begin;
select * from bmsql_oorder order by o_w_id, o_d_id, o_id;
commit;
select * from bmsql_oorder order by o_w_id, o_d_id, o_id;
-- @session

-----------------------------------------------------------------------------------
prepare __mo_stmt_id_4 from 'INSERT INTO bmsql_oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)';
prepare __mo_stmt_id_22 from 'SELECT o_id, o_entry_d, o_carrier_id FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT max(o_id) FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)';
prepare __mo_stmt_id_27 from 'SELECT o_c_id FROM bmsql_oorder WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?';
prepare __mo_stmt_id_28 from 'UPDATE bmsql_oorder SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?';
-----------------------------------------------------------------------------------
begin;
SET @o_carrier_id = 3;
SET @o_w_id = 9;
SET @o_d_id = 6;
SET @o_id = 2106;
EXECUTE __mo_stmt_id_28 USING @o_carrier_id, @o_w_id, @o_d_id, @o_id;
select * from bmsql_oorder order by o_w_id, o_d_id, o_id;
commit;
select * from bmsql_oorder order by o_w_id, o_d_id, o_id;
-----------------------------------------------------------------------------------
begin;
SET @o_carrier_id = 4;
SET @o_w_id = 1;
SET @o_d_id = 5;
SET @o_id = 2107;
EXECUTE __mo_stmt_id_28 USING @o_carrier_id, @o_w_id, @o_d_id, @o_id;

SET @o_id = 3011;
SET @o_d_id = 10;
SET @o_w_id = 9;
SET @o_c_id = 1961;
SET @o_entry_d = CURRENT_TIMESTAMP();
SET @o_ol_cnt = 5;
SET @o_all_local = 1;
EXECUTE __mo_stmt_id_4 USING @o_id, @o_d_id, @o_w_id, @o_c_id, @o_entry_d, @o_ol_cnt, @o_all_local;

select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;
commit;
select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;
-------------------------------------------------------------------------------------------------------------------------------
-- @session:id=1&user=dump&password=111
begin;
SET @o_carrier_id = 1;
SET @o_w_id = 3;
SET @o_d_id = 10;
SET @o_id = 2116;
EXECUTE __mo_stmt_id_28 USING @o_carrier_id, @o_w_id, @o_d_id, @o_id;


SET @o_carrier_id = 4;
SET @o_w_id = 4;
SET @o_d_id = 9;
SET @o_id = 2113;
EXECUTE __mo_stmt_id_28 USING @o_carrier_id, @o_w_id, @o_d_id, @o_id;


SET @o_carrier_id = 3;
SET @o_w_id = 5;
SET @o_d_id = 9;
SET @o_id = 2113;
EXECUTE __mo_stmt_id_28 USING @o_carrier_id, @o_w_id, @o_d_id, @o_id;

SET @o_w_id = 3;
SET @o_d_id = 6;
SET @o_id = 2115;
EXECUTE __mo_stmt_id_27 USING @o_w_id, @o_d_id, @o_id;

select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;
commit;
select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;
-- @session
-------------------------------------------------------------------------------------------------------------------------------
begin;
SET @o_w_id1 = 2;
SET @o_d_id1 = 9;
SET @o_c_id1 = 524;
SET @o_w_id2 = 2;
SET @o_d_id2 = 9;
SET @o_c_id2 = 524;
EXECUTE __mo_stmt_id_22 USING @o_w_id1, @o_d_id1, @o_c_id1, @o_w_id2, @o_d_id2, @o_c_id2;

SET @o_id = 3030;
SET @o_d_id = 5;
SET @o_w_id = 8;
SET @o_c_id = 935;
SET @o_entry_d = CURRENT_TIMESTAMP();
SET @o_ol_cnt = 5;
SET @o_all_local = 1;
EXECUTE __mo_stmt_id_4 USING @o_id, @o_d_id, @o_w_id, @o_c_id, @o_entry_d, @o_ol_cnt, @o_all_local;

SET @o_w_id = 6;
SET @o_d_id = 10;
SET @o_id = 2116;
EXECUTE __mo_stmt_id_27 USING @o_w_id, @o_d_id, @o_id;

select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;
commit;

select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;
-------------------------------------------------------------------------------------------------------------------------------
begin;
SET @o_id = 3047;
SET @o_d_id = 1;
SET @o_w_id = 2;
SET @o_c_id = 30;
SET @o_entry_d = CURRENT_TIMESTAMP();
SET @o_ol_cnt = 15;
SET @o_all_local = 1;
EXECUTE __mo_stmt_id_4 USING @o_id, @o_d_id, @o_w_id, @o_c_id, @o_entry_d, @o_ol_cnt, @o_all_local;

select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;
commit;
select o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local from bmsql_oorder order by o_w_id, o_d_id, o_id;

drop table bmsql_oorder;
drop database if exists tpcc;