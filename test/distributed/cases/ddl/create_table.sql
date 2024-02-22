drop database if exists test;
create database test;
use test;

/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE test01 (
                       `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'pk',
                       `pk_id` bigint NOT NULL COMMENT 'pk_id',
                       `config_id` bigint NOT NULL COMMENT 'config_id',
                       `trace_id` varchar(255) COLLATE utf8mb4_general_ci NOT NULL,
                       `type` varchar(8) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'message_type',
                       `data` varchar(1024) COLLATE utf8mb4_general_ci NOT NULL,
                       `times` tinyint NOT NULL,
                       `status` tinyint NOT NULL,
                       `push_time` varchar(255) COLLATE utf8mb4_general_ci NOT NULL,
                       `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                       `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                       PRIMARY KEY (`id`,`create_time`) USING BTREE,
                       UNIQUE KEY (`trace_id`,`create_time`)
) ENGINE=InnoDB AUTO_INCREMENT=654 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='trace id'
/*!50500 PARTITION BY RANGE  COLUMNS(create_time)
(PARTITION p20240115 VALUES LESS THAN ('2024-01-15 00:00:00') ENGINE = InnoDB,
 PARTITION p20240116 VALUES LESS THAN ('2024-01-16 00:00:00') ENGINE = InnoDB,
 PARTITION p20240117 VALUES LESS THAN ('2024-01-17 00:00:00') ENGINE = InnoDB,
 PARTITION p20240118 VALUES LESS THAN ('2024-01-18 00:00:00') ENGINE = InnoDB,
 PARTITION p20240119 VALUES LESS THAN ('2024-01-19 00:00:00') ENGINE = InnoDB,
 PARTITION p20240120 VALUES LESS THAN ('2024-01-20 00:00:00') ENGINE = InnoDB,
 PARTITION p20240121 VALUES LESS THAN ('2024-01-21 00:00:00') ENGINE = InnoDB,
 PARTITION p20240122 VALUES LESS THAN ('2024-01-22 00:00:00') ENGINE = InnoDB,
 PARTITION p20240123 VALUES LESS THAN ('2024-01-23 00:00:00') ENGINE = InnoDB,
 PARTITION p20240124 VALUES LESS THAN ('2024-01-24 00:00:00') ENGINE = InnoDB,
 PARTITION p20240125 VALUES LESS THAN ('2024-01-25 00:00:00') ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;
show create table test01;
drop table test01;

/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE test02 (
                        `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'pk',
                        `pk_id` bigint NOT NULL COMMENT 'pk_id',
                        `config_id` bigint NOT NULL COMMENT 'config_id',
                        `trace_id` varchar(255) COLLATE utf8mb4_general_ci NOT NULL,
                        `type` varchar(8) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'message_type',
                        `data` varchar(1024) COLLATE utf8mb4_general_ci NOT NULL,
                        `times` tinyint NOT NULL,
                        `status` tinyint NOT NULL,
                        `push_time` varchar(255) COLLATE utf8mb4_general_ci NOT NULL,
                        `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (`id`,`create_time`) USING BTREE,
                        UNIQUE KEY (`trace_id`,`create_time`)
) ENGINE=InnoDB AUTO_INCREMENT=654 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='trace id'
PARTITION BY RANGE  COLUMNS(create_time)
(PARTITION p20240115 VALUES LESS THAN ('2024-01-15 00:00:00') ENGINE = InnoDB,
 PARTITION p20240116 VALUES LESS THAN ('2024-01-16 00:00:00') ENGINE = InnoDB,
 PARTITION p20240117 VALUES LESS THAN ('2024-01-17 00:00:00') ENGINE = InnoDB,
 PARTITION p20240118 VALUES LESS THAN ('2024-01-18 00:00:00') ENGINE = InnoDB,
 PARTITION p20240119 VALUES LESS THAN ('2024-01-19 00:00:00') ENGINE = InnoDB,
 PARTITION p20240120 VALUES LESS THAN ('2024-01-20 00:00:00') ENGINE = InnoDB,
 PARTITION p20240121 VALUES LESS THAN ('2024-01-21 00:00:00') ENGINE = InnoDB,
 PARTITION p20240122 VALUES LESS THAN ('2024-01-22 00:00:00') ENGINE = InnoDB,
 PARTITION p20240123 VALUES LESS THAN ('2024-01-23 00:00:00') ENGINE = InnoDB,
 PARTITION p20240124 VALUES LESS THAN ('2024-01-24 00:00:00') ENGINE = InnoDB,
 PARTITION p20240125 VALUES LESS THAN ('2024-01-25 00:00:00') ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;
show create table test02;
drop table test02;

drop database test;