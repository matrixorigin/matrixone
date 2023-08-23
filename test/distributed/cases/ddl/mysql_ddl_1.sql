-- testcase_1:  /*! */  /* */

/*!40101 -- SET @saved_cs_client = @@character_set_client */;
/*!40101 -- SET character_set_client = utf8 */;
/*!40000 create database if not exists db_1 */;
/*!40000 show tables in db_1 */;
/* this is a comment */
/* and this 
is also 
a coment */

drop database /*!40000 if exists */ mysql_ddl_test_db_1;
create database /*!40000 if not exists */ mysql_ddl_test_db_1;
use mysql_ddl_test_db_1;
select database();

drop database if exists mysql_ddl_test_db_2;
create database if not exists mysql_ddl_test_db_2;
use mysql_ddl_test_db_2;
select database();

/*!40101 use mysql_ddl_test_db_3; */ 
select database();
create database /* this is another comment */ if not exists mysql_ddl_test_db_3;
use mysql_ddl_test_db_3;
select database();

-- @bvt:issue#moc 1231
CREATE DATABASE /*!32312 IF NOT EXISTS*/ `mysql_ddl_test_db_4` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
/*!40101 use mysql_ddl_test_db_4; */
select database();
-- @bvt:issue

SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;
select @MYSQLDUMP_TEMP_LOG_BIN;

SET @@SESSION.SQL_LOG_BIN= 0;                                                                                                          
select @@SESSION.SQL_LOG_BIN;

SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '65c4c218-d343-11eb-8106-525400f4f901:1-769275';
select @@GLOBAL.GTID_PURGED;

/*!40103 SET TIME_ZONE='+00:00' */;
select @@TIME_ZONE;

SET TIME_ZONE='+08:00';
select @@TIME_ZONE;

SET @saved_cs_client = xxx;
select @saved_cs_client;
SET character_set_client  = xxx;
select @@character_set_client;

