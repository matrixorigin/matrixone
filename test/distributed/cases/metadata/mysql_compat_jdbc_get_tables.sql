-- MatrixOne #25103: Connector/J DatabaseMetaData.getTables() uses HAVING
-- on a projected TABLE_TYPE alias without GROUP BY.
drop database if exists mysql_compat_jdbc_get_tables;
create database mysql_compat_jdbc_get_tables;
use mysql_compat_jdbc_get_tables;
set session sql_mode='ONLY_FULL_GROUP_BY';
create table t (id int primary key);
create table t2 (id int primary key);
insert into t values (1), (0);

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

-- A directly projected column is also visible to non-aggregate HAVING.
select TABLE_SCHEMA
from INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'mysql_compat_jdbc_get_tables'
having TABLE_SCHEMA = 'mysql_compat_jdbc_get_tables'
order by TABLE_SCHEMA, TABLE_NAME;

-- ONLY_FULL_GROUP_BY still rejects an unprojected HAVING source column.
-- @regex("must appear in the GROUP BY clause",true)
select TABLE_SCHEMA
from INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'mysql_compat_jdbc_get_tables'
having TABLE_TYPE = 'BASE TABLE';

-- Equivalent duplicate aliases are still valid and resolve to the same output.
select id AS duplicate_name, id AS duplicate_name
from t
having duplicate_name > 0;

-- An anonymous repeated expression does not acquire HAVING output visibility.
-- @regex("must appear in the GROUP BY clause",true)
select id + 1
from t
having id + 1 > 1;

-- Different expressions under the same alias are ambiguous in HAVING.
-- @regex("Column 'duplicate_name' in having clause is ambiguous",true)
select id AS duplicate_name, id + 1 AS duplicate_name
from t
having duplicate_name > 0;

drop database mysql_compat_jdbc_get_tables;
