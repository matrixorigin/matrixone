SET experimental_fulltext_index = 1;
SET experimental_ivf_index = 1;
SET experimental_hnsw_index = 1;

drop database if exists db;
create database db;
use db;

-- case 1: primary table duplicate
DROP TABLE IF EXISTS stress_alter_table;
CREATE TABLE `stress_alter_table` (
    `md5_id` int NOT NULL,
    `b` varchar(65535) DEFAULT NULL,
    `delete_flag` int DEFAULT NULL,
    PRIMARY KEY (`md5_id`),
    FULLTEXT `fb`(`b`) WITH PARSER ngram,
    KEY `delete_flag_idx` (`delete_flag`)
);

INSERT INTO stress_alter_table VALUES (1, 'this is a one', 0);

DELETE FROM `stress_alter_table` WHERE delete_flag = 0;
INSERT INTO `stress_alter_table` (`md5_id`,`b`,`delete_flag`) VALUES (1,'this is a one',0);
ALTER TABLE `stress_alter_table` MODIFY COLUMN delete_flag INT DEFAULT 0;
-- @ignore:0
select mo_ctl('dn','checkpoint','');
SELECT COUNT(*) FROM `stress_alter_table`;
UPDATE `stress_alter_table` SET delete_flag = 1 WHERE md5_id = '1';
SELECT COUNT(*) FROM `stress_alter_table`;

drop table stress_alter_table;

-- case 2
create table t1 (id bigint primary key, delete_flag int, a int, body varchar(10), title text, FULLTEXT (title, body), key(delete_flag));
insert into t1 values(1,1,1, "body", "title");
ALTER TABLE t1 MODIFY COLUMN a int DEFAULT NULL;

SET @inner_sql = (
    SELECT GROUP_CONCAT(
                   DISTINCT CONCAT(
                    'SELECT ''', mi.index_table_name,
                    ''' AS index_table_name, COUNT(*) AS cnt FROM `',
                    mi.index_table_name, '`'
                            ) SEPARATOR ' UNION ALL '
           )
    FROM mo_catalog.mo_indexes mi
             JOIN mo_catalog.mo_tables mt ON mi.table_id = mt.rel_id
    WHERE mt.relname = 't1'
      AND mt.reldatabase = 'db'
      AND mi.type IN ('MULTIPLE', 'UNIQUE')
      AND mi.index_table_name IS NOT NULL
      AND mi.index_table_name != ''
      AND mi.column_name <> 'question'
      AND mi.column_name <> 'keyword'
);

SET @sql = CONCAT(
        'SELECT * FROM (',
        @inner_sql,
        ') AS t ORDER BY cnt DESC'
           );

PREPARE stmt FROM @sql;
-- @ignore:0
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


drop table t1;

-- case 3

create table t2 (id int primary key, a int, b int, c int, key(a), key(b));
insert into t2 values(1,1,1,1);
ALTER TABLE t2 MODIFY COLUMN c int DEFAULT NULL;

SET @inner_sql = (
    SELECT GROUP_CONCAT(
                   DISTINCT CONCAT(
                    'SELECT ''', mi.index_table_name,
                    ''' AS index_table_name, COUNT(*) AS cnt FROM `',
                    mi.index_table_name, '`'
                            ) SEPARATOR ' UNION ALL '
           )
    FROM mo_catalog.mo_indexes mi
             JOIN mo_catalog.mo_tables mt ON mi.table_id = mt.rel_id
    WHERE mt.relname = 't2'
      AND mt.reldatabase = 'db'
      AND mi.type IN ('MULTIPLE', 'UNIQUE')
      AND mi.index_table_name IS NOT NULL
      AND mi.index_table_name != ''
      AND mi.column_name <> 'question'
      AND mi.column_name <> 'keyword'
);

SET @sql = CONCAT(
        'SELECT * FROM (',
        @inner_sql,
        ') AS t ORDER BY cnt DESC'
           );

PREPARE stmt FROM @sql;
-- @ignore:0
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

drop table t2;

SET experimental_fulltext_index = 0;
SET experimental_ivf_index = 0;
SET experimental_hnsw_index = 0;

drop database db;