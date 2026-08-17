-- MatrixOne #25103: Connector/J DatabaseMetaData.getTables() uses HAVING
-- on a projected TABLE_TYPE alias without GROUP BY.
drop database if exists mysql_compat_jdbc_get_tables;
create database mysql_compat_jdbc_get_tables;
use mysql_compat_jdbc_get_tables;
set session sql_mode='ONLY_FULL_GROUP_BY';
create table t (id int primary key);
create table t2 (id int primary key);

select TABLE_SCHEMA AS TABLE_CAT,
       NULL AS TABLE_SCHEM,
       TABLE_NAME,
       CASE
           WHEN TABLE_TYPE = 'BASE TABLE' THEN
               CASE
                   WHEN TABLE_SCHEMA = 'mysql'
                       OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE'
                   ELSE 'TABLE'
                   END
           WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
           ELSE TABLE_TYPE
           END AS TABLE_TYPE,
       TABLE_COMMENT AS REMARKS,
       NULL AS TYPE_CAT,
       NULL AS TYPE_SCHEM,
       NULL AS TYPE_NAME,
       NULL AS SELF_REFERENCING_COL_NAME,
       NULL AS REF_GENERATION
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'mysql_compat_jdbc_get_tables'
HAVING TABLE_TYPE IN ('TABLE', NULL, NULL, NULL, NULL)
ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME;

-- ONLY_FULL_GROUP_BY still rejects an unprojected HAVING source column.
select TABLE_SCHEMA
from INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'mysql_compat_jdbc_get_tables'
having TABLE_TYPE = 'BASE TABLE';

drop database mysql_compat_jdbc_get_tables;
