// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sysview

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

// `mysql` database system tables
// They are all Tenant level system tables
var (
	MysqlUserDDL = `CREATE TABLE mysql.user (
			Host char(255)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Select_priv varchar(10) NOT NULL DEFAULT 'N',
			Insert_priv varchar(10) NOT NULL DEFAULT 'N',
			Update_priv varchar(10) NOT NULL DEFAULT 'N',
			Delete_priv varchar(10) NOT NULL DEFAULT 'N',
			Create_priv varchar(10) NOT NULL DEFAULT 'N',
			Drop_priv varchar(10)  NOT NULL DEFAULT 'N',
			Reload_priv varchar(10)  NOT NULL DEFAULT 'N',
			Shutdown_priv varchar(10)  NOT NULL DEFAULT 'N',
			Process_priv varchar(10)  NOT NULL DEFAULT 'N',
			File_priv varchar(10)  NOT NULL DEFAULT 'N',
			Grant_priv varchar(10)  NOT NULL DEFAULT 'N',
			References_priv varchar(10)  NOT NULL DEFAULT 'N',
			Index_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_priv varchar(10)  NOT NULL DEFAULT 'N',
			Show_db_priv varchar(10)  NOT NULL DEFAULT 'N',
			Super_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_tmp_table_priv varchar(10)  NOT NULL DEFAULT 'N',
			Lock_tables_priv varchar(10)  NOT NULL DEFAULT 'N',
			Execute_priv varchar(10)  NOT NULL DEFAULT 'N',
			Repl_slave_priv varchar(10)  NOT NULL DEFAULT 'N',
			Repl_client_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Show_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_user_priv varchar(10)  NOT NULL DEFAULT 'N',
			Event_priv varchar(10)  NOT NULL DEFAULT 'N',
			Trigger_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_tablespace_priv varchar(10)  NOT NULL DEFAULT 'N',
			ssl_type varchar(10)  NOT NULL DEFAULT '',
			ssl_cipher blob NOT NULL,
			x509_issuer blob NOT NULL,
			x509_subject blob NOT NULL,
			max_questions int unsigned NOT NULL DEFAULT '0',
			max_updates int unsigned NOT NULL DEFAULT '0',
			max_connections int unsigned NOT NULL DEFAULT '0',
			max_user_connections int unsigned NOT NULL DEFAULT '0',
			plugin char(64)  NOT NULL DEFAULT 'caching_sha2_password',
			authentication_string text ,
			password_expired varchar(10)  NOT NULL DEFAULT 'N',
			password_last_changed timestamp NULL DEFAULT NULL,
			password_lifetime smallint unsigned DEFAULT NULL,
			account_locked varchar(10)  NOT NULL DEFAULT 'N',
			Create_role_priv varchar(10)  NOT NULL DEFAULT 'N',
			Drop_role_priv varchar(10)  NOT NULL DEFAULT 'N',
			Password_reuse_history smallint unsigned DEFAULT NULL,
			Password_reuse_time smallint unsigned DEFAULT NULL,
			Password_require_current varchar(10)  DEFAULT NULL,
			User_attributes json DEFAULT NULL,
			PRIMARY KEY (Host,User)
		  )`

	MysqlDbDDL = `CREATE TABLE mysql.db (
			Host char(255) NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Select_priv varchar(10)  NOT NULL DEFAULT 'N',
			Insert_priv varchar(10)  NOT NULL DEFAULT 'N',
			Update_priv varchar(10)  NOT NULL DEFAULT 'N',
			Delete_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_priv varchar(10)  NOT NULL DEFAULT 'N',
			Drop_priv varchar(10)  NOT NULL DEFAULT 'N',
			Grant_priv varchar(10)  NOT NULL DEFAULT 'N',
			References_priv varchar(10)  NOT NULL DEFAULT 'N',
			Index_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_tmp_table_priv varchar(10)  NOT NULL DEFAULT 'N',
			Lock_tables_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Show_view_priv varchar(10)  NOT NULL DEFAULT 'N',
			Create_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Alter_routine_priv varchar(10)  NOT NULL DEFAULT 'N',
			Execute_priv varchar(10)  NOT NULL DEFAULT 'N',
			Event_priv varchar(10)  NOT NULL DEFAULT 'N',
			Trigger_priv varchar(10)  NOT NULL DEFAULT 'N',
			PRIMARY KEY (Host,Db,User),
			KEY User (User)
		  )`

	MysqlProcsPrivDDL = `CREATE TABLE mysql.procs_priv (
			Host char(255)  NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Routine_name char(64)  NOT NULL DEFAULT '',
			Routine_type varchar(10)  NOT NULL,
			Grantor varchar(288)  NOT NULL DEFAULT '',
			Proc_priv varchar(10)  NOT NULL DEFAULT '',
			Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (Host,Db,User,Routine_name,Routine_type),
			KEY Grantor (Grantor)
		  )`

	MysqlColumnsPrivDDL = `CREATE TABLE mysql.columns_priv (
			Host char(255)  NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Table_name char(64)  NOT NULL DEFAULT '',
			Column_name char(64)  NOT NULL DEFAULT '',
			Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			Column_priv varchar(10) NOT NULL DEFAULT '',
			PRIMARY KEY (Host,Db,User,Table_name,Column_name)
		  )`

	MysqlTablesPrivDDL = `CREATE TABLE mysql.tables_priv (
			Host char(255)  NOT NULL DEFAULT '',
			Db char(64)  NOT NULL DEFAULT '',
			User char(32)  NOT NULL DEFAULT '',
			Table_name char(64)  NOT NULL DEFAULT '',
			Grantor varchar(288)  NOT NULL DEFAULT '',
			Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			Table_priv varchar(10) NOT NULL DEFAULT '',
			Column_priv varchar(10) NOT NULL DEFAULT '',
			PRIMARY KEY (Host,Db,User,Table_name),
			KEY Grantor (Grantor)
		  )`

	MysqlRoleEdgesDDL = `CREATE TABLE mysql.role_edges (
			FROM_HOST char(255) NOT NULL DEFAULT '',
			FROM_USER char(32) NOT NULL DEFAULT '',
			TO_HOST char(255) NOT NULL DEFAULT '',
			TO_USER char(32) NOT NULL DEFAULT '',
			WITH_ADMIN_OPTION enum('N','Y') NOT NULL DEFAULT 'N',
			PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
		)`
)

// informationSchemaMetadataVisibilityCTE limits user-object metadata to the
// objects visible to the session's active role and its full inherited-role
// closure. The closure is produced locally by mo_current_roles(), avoiding
// distributed recursive pipelines in every information_schema query. System
// schemas remain universally visible for MySQL/tooling compatibility.
func informationSchemaMetadataVisibilityCTE() string {
	return informationSchemaMetadataVisibilityCTEWithActiveRoles(
		"SELECT role_id FROM mo_current_roles() role_closure")
}

// informationSchemaMetadataVisibilityCompatibilityCTE is used only while a
// rolling deployment's common protocol is below the mo_current_roles()
// capability. It keeps tenant bootstrap executable on every CN and remains
// cycle-safe by limiting the compatibility closure to the active role and its
// directly inherited roles. The v35 same-version upgrade replaces these
// definitions with the complete canonical closure after all CNs support it.
func informationSchemaMetadataVisibilityCompatibilityCTE() string {
	return informationSchemaMetadataVisibilityCTEWithActiveRoles(
		"SELECT current_role_id() UNION " +
			"SELECT rg.granted_id FROM mo_catalog.mo_role_grant rg " +
			"WHERE rg.grantee_id = current_role_id()")
}

func informationSchemaMetadataVisibilityCTEWithActiveRoles(activeRolesSQL string) string {
	return "WITH __mo_active_roles(role_id) AS (" + activeRolesSQL + "), " +
		"__mo_visible_tables AS (" +
		"SELECT tbl.account_id, tbl.rel_id, tbl.relname, tbl.reldatabase, tbl.reldatabase_id, tbl.relkind, " +
		"tbl.rel_createsql, tbl.created_time, tbl.partitioned, tbl.rel_comment, tbl.extra_info, tbl.rel_logical_id, " +
		"tbl.owner, tbl.`constraint` FROM mo_catalog.mo_tables tbl " +
		"WHERE tbl.account_id = current_account_id() AND (" +
		"tbl.reldatabase IN ('mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug') " +
		"OR tbl.owner IN (SELECT role_id FROM __mo_active_roles) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_database db JOIN __mo_active_roles ar ON db.owner = ar.role_id " +
		"WHERE db.dat_id = tbl.reldatabase_id) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_role_privs rp JOIN __mo_active_roles ar ON rp.role_id = ar.role_id " +
		"WHERE (rp.obj_type IN ('table','view') AND (" +
		"(rp.privilege_level = '*.*' AND rp.obj_id = 0) " +
		"OR (rp.privilege_level IN ('d.*','*') AND rp.obj_id = tbl.reldatabase_id) " +
		"OR (rp.privilege_level IN ('d.t','t') AND rp.obj_id = tbl.rel_logical_id))) " +
		"OR (rp.obj_type = 'database' AND rp.privilege_name IN ('show tables','database all','database ownership') AND (" +
		"(rp.privilege_level IN ('*','*.*') AND rp.obj_id = 0) " +
		"OR (rp.privilege_level = 'd' AND rp.obj_id = tbl.reldatabase_id)))))), " +
		"__mo_visible_databases AS (" +
		"SELECT db.account_id, db.dat_id, db.datname, db.owner FROM mo_catalog.mo_database db " +
		"WHERE (db.account_id = current_account_id() AND (" +
		"db.datname IN ('mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug') " +
		"OR db.owner IN (SELECT role_id FROM __mo_active_roles) " +
		"OR EXISTS (SELECT 1 FROM __mo_visible_tables tbl WHERE tbl.reldatabase_id = db.dat_id) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_role_privs rp JOIN __mo_active_roles ar ON rp.role_id = ar.role_id " +
		"WHERE rp.obj_type = 'account' AND rp.privilege_name IN ('show databases','account all') " +
		"AND rp.privilege_level = '*' AND rp.obj_id = 0) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_role_privs rp JOIN __mo_active_roles ar ON rp.role_id = ar.role_id " +
		"WHERE rp.obj_type = 'database' AND rp.privilege_name IN ('show tables','database all','database ownership') AND (" +
		"(rp.privilege_level IN ('*','*.*') AND rp.obj_id = 0) " +
		"OR (rp.privilege_level = 'd' AND rp.obj_id = db.dat_id))))) " +
		"OR (db.account_id = 0 AND db.datname = 'mo_catalog')) "
}

func informationSchemaSubscriptionTableAuthorizationPredicate() string {
	return "(" +
		"tbl.reldatabase IN ('mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug') " +
		"OR tbl.owner IN (SELECT role_id FROM __mo_active_roles) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_database db JOIN __mo_active_roles ar ON db.owner = ar.role_id " +
		"WHERE db.dat_id = tbl.reldatabase_id) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_role_privs rp JOIN __mo_active_roles ar ON rp.role_id = ar.role_id " +
		"WHERE (rp.obj_type IN ('table','view') AND (" +
		"(rp.privilege_level = '*.*' AND rp.obj_id = 0) " +
		"OR (rp.privilege_level IN ('d.*','*') AND rp.obj_id = tbl.reldatabase_id) " +
		"OR (rp.privilege_level IN ('d.t','t') AND rp.obj_id = tbl.rel_logical_id))) " +
		"OR (rp.obj_type = 'database' AND rp.privilege_name IN ('show tables','database all','database ownership') AND (" +
		"(rp.privilege_level IN ('*','*.*') AND rp.obj_id = 0) " +
		"OR (rp.privilege_level = 'd' AND rp.obj_id = tbl.reldatabase_id)))))"
}

func informationSchemaSubscriptionTablesDDL() string {
	prefix := "CREATE VIEW information_schema.TABLES AS " + informationSchemaMetadataVisibilityCTE()
	localSelect := strings.TrimPrefix(InformationSchemaTablesV41DDL, prefix)
	subscriptionSelect := strings.Replace(
		localSelect,
		"if(relkind = 'v', NULL, internal_auto_increment(reldatabase, relname)) AS `AUTO_INCREMENT`,",
		"if(relkind = 'v', NULL, cast(0 as bigint unsigned)) AS `AUTO_INCREMENT`,",
		1,
	)
	subscriptionSelect = strings.Replace(
		subscriptionSelect,
		"FROM __mo_visible_tables tbl ",
		"FROM mo_subscription_tables() tbl ",
		1,
	)
	subscriptionSelect = strings.Replace(
		subscriptionSelect,
		"WHERE tbl.account_id = current_account_id() and",
		"WHERE tbl.account_id = current_account_id() AND "+
			informationSchemaSubscriptionTableAuthorizationPredicate()+" and",
		1,
	)
	return prefix + localSelect + " UNION ALL " + subscriptionSelect
}

func informationSchemaColumnsLocalFromSQL() string {
	return "from mo_catalog.mo_columns mc join __mo_visible_tables mt " +
		"ON mc.account_id = mt.account_id AND mc.att_database = mt.reldatabase AND mc.att_relname = mt.relname " +
		"left join (select ki.table_id, ki.column_name, " +
		"max(case when ki.type = 'PRIMARY' then 3 when ki.type = 'UNIQUE' and kp.part_count = 1 then 2 else 1 end) as key_priority " +
		"from mo_catalog.mo_indexes ki " +
		"join (select id, count(*) as part_count from mo_catalog.mo_indexes group by id) kp on ki.id = kp.id " +
		"where (ki.type = 'PRIMARY' or ki.ordinal_position = 1) and ki.type in ('PRIMARY', 'UNIQUE', 'MULTIPLE', 'FULLTEXT', 'SPATIAL') " +
		"group by ki.table_id, ki.column_name) mk ON mk.table_id = mt.rel_id AND mk.column_name = mc.attname "
}

func informationSchemaSubscriptionColumnAuthorizationPredicate() string {
	return "(" +
		"mc.att_database IN ('mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug') " +
		"OR mc.table_owner IN (SELECT role_id FROM __mo_active_roles) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_database db JOIN __mo_active_roles ar ON db.owner = ar.role_id " +
		"WHERE db.dat_id = mc.att_database_id) " +
		"OR EXISTS (SELECT 1 FROM mo_catalog.mo_role_privs rp JOIN __mo_active_roles ar ON rp.role_id = ar.role_id " +
		"WHERE (rp.obj_type IN ('table','view') AND (" +
		"(rp.privilege_level = '*.*' AND rp.obj_id = 0) " +
		"OR (rp.privilege_level IN ('d.*','*') AND rp.obj_id = mc.att_database_id) " +
		"OR (rp.privilege_level IN ('d.t','t') AND rp.obj_id = mc.rel_logical_id))) " +
		"OR (rp.obj_type = 'database' AND rp.privilege_name IN ('show tables','database all','database ownership') AND (" +
		"(rp.privilege_level IN ('*','*.*') AND rp.obj_id = 0) " +
		"OR (rp.privilege_level = 'd' AND rp.obj_id = mc.att_database_id)))))"
}

func informationSchemaSubscriptionColumnsDDL() string {
	prefix := "CREATE VIEW information_schema.COLUMNS AS " + informationSchemaMetadataVisibilityCTE()
	localSelect := strings.TrimPrefix(InformationSchemaColumnsV41DDL, prefix)
	subscriptionSelect := strings.Replace(
		localSelect,
		"case when mc.att_constraint_type = 'p' or mk.key_priority = 3 then 'PRI' when mk.key_priority = 2 then 'UNI' when mk.key_priority = 1 then 'MUL' else '' end as COLUMN_KEY,",
		"case when mc.att_constraint_type = 'p' or mc.key_priority = 3 then 'PRI' "+
			"when mc.key_priority = 2 then 'UNI' when mc.key_priority = 1 then 'MUL' else '' end as COLUMN_KEY,",
		1,
	)
	subscriptionSelect = strings.Replace(
		subscriptionSelect,
		informationSchemaColumnsLocalFromSQL(),
		"from mo_subscription_columns() mc ",
		1,
	)
	subscriptionSelect = strings.Replace(
		subscriptionSelect,
		"where mc.account_id = current_account_id() and",
		"where mc.account_id = current_account_id() and "+
			informationSchemaSubscriptionColumnAuthorizationPredicate()+" and",
		1,
	)
	subscriptionSelect = strings.NewReplacer(
		"mt.relkind", "mc.relkind",
		"mt.relname", "mc.att_relname",
		"mt.reldatabase", "mc.att_database",
		"mt.rel_createsql", "mc.rel_createsql",
		"mt.extra_info", "mc.extra_info",
	).Replace(subscriptionSelect)
	return "CREATE VIEW information_schema.COLUMNS AS " +
		informationSchemaMetadataVisibilityCTE() + localSelect + " UNION ALL " + subscriptionSelect
}

// `information_schema` database
// They are all Tenant level system tables/system views
var (
	InformationSchemaKeyColumnUsageDDL = fmt.Sprintf("CREATE VIEW information_schema.KEY_COLUMN_USAGE AS "+
		informationSchemaMetadataVisibilityCTE()+"SELECT "+
		"CAST('def' AS varchar(64)) AS CONSTRAINT_CATALOG, "+
		"CAST(coalesce(tbl.reldatabase, '') AS varchar(64)) AS CONSTRAINT_SCHEMA, "+
		"CAST(idx.name AS varchar(64)) AS CONSTRAINT_NAME, "+
		"CAST('def' AS varchar(64)) AS TABLE_CATALOG, "+
		"CAST(coalesce(tbl.reldatabase, '') AS varchar(64)) AS TABLE_SCHEMA, "+
		"CAST(coalesce(tbl.relname, '') AS varchar(64)) AS TABLE_NAME, "+
		"CAST(idx.column_name AS varchar(64)) AS COLUMN_NAME, "+
		"CAST(idx.ordinal_position AS int unsigned) AS ORDINAL_POSITION, "+
		"CAST(NULL AS int unsigned) AS POSITION_IN_UNIQUE_CONSTRAINT, "+
		"CAST(NULL AS varchar(64)) AS REFERENCED_TABLE_SCHEMA, "+
		"CAST(NULL AS varchar(64)) AS REFERENCED_TABLE_NAME, "+
		"CAST(NULL AS varchar(64)) AS REFERENCED_COLUMN_NAME "+
		"FROM mo_catalog.mo_indexes idx "+
		"JOIN __mo_visible_tables tbl ON idx.table_id = tbl.rel_id "+
		"WHERE tbl.account_id = current_account_id() "+
		"AND idx.type IN ('PRIMARY', 'UNIQUE') "+
		"AND NOT startswith(tbl.relname, '%s') AND %s "+
		"UNION ALL "+
		"SELECT "+
		"CAST('def' AS varchar(64)) AS CONSTRAINT_CATALOG, "+
		"CAST(fk.db_name AS varchar(64)) AS CONSTRAINT_SCHEMA, "+
		"CAST(fk.constraint_name AS varchar(64)) AS CONSTRAINT_NAME, "+
		"CAST('def' AS varchar(64)) AS TABLE_CATALOG, "+
		"CAST(fk.db_name AS varchar(64)) AS TABLE_SCHEMA, "+
		"CAST(fk.table_name AS varchar(64)) AS TABLE_NAME, "+
		"CAST(fk.column_name AS varchar(64)) AS COLUMN_NAME, "+
		"CAST(fk.constraint_id AS int unsigned) AS ORDINAL_POSITION, "+
		"CAST(fk.constraint_id AS int unsigned) AS POSITION_IN_UNIQUE_CONSTRAINT, "+
		"CAST(fk.refer_db_name AS varchar(64)) AS REFERENCED_TABLE_SCHEMA, "+
		"CAST(fk.refer_table_name AS varchar(64)) AS REFERENCED_TABLE_NAME, "+
		"CAST(fk.refer_column_name AS varchar(64)) AS REFERENCED_COLUMN_NAME "+
		"FROM mo_catalog.mo_foreign_keys fk "+
		"JOIN __mo_visible_tables fk_tbl "+
		"ON fk.db_name = fk_tbl.reldatabase AND fk.table_name = fk_tbl.relname",
		catalog.IndexTableNamePrefix, catalog.NonTemporaryTableSQLPredicate("tbl"))

	InformationSchemaColumnsV41DDL = fmt.Sprintf("CREATE VIEW information_schema.COLUMNS AS "+informationSchemaMetadataVisibilityCTE()+"select "+
		"'def' as TABLE_CATALOG,"+
		"mc.att_database as TABLE_SCHEMA,"+
		"mc.att_relname AS TABLE_NAME,"+
		"mc.attname AS COLUMN_NAME,"+
		"mc.attnum AS ORDINAL_POSITION,"+
		"mo_show_visible_bin(mc.att_default,1) as COLUMN_DEFAULT,"+
		"(case when mc.attnotnull != 0 then 'NO' else 'YES' end) as IS_NULLABLE,"+
		"lower(case when length(mc.attr_enum) > 0 then "+
		"  (case when mo_show_visible_bin(mc.atttyp,2) = 'GEOMETRY' then "+
		"    upper(case when upper(split_part(mc.attr_enum, ';', 1)) like 'SRID=%%' then 'GEOMETRY' else split_part(mc.attr_enum, ';', 1) end) "+
		"  else upper(split_part(mo_show_visible_bin_enum(mc.atttyp, mc.attr_enum), '(', 1)) end) "+
		" else (case when upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'TINYINT' "+
		"  else split_part(mo_show_visible_bin(mc.atttyp,2), ' ', 1) end) end) as DATA_TYPE,"+
		"internal_char_length(mc.atttyp) AS CHARACTER_MAXIMUM_LENGTH,"+
		"internal_char_size(mc.atttyp) AS CHARACTER_OCTET_LENGTH,"+
		"internal_numeric_precision(mc.atttyp) AS NUMERIC_PRECISION,"+
		"internal_numeric_scale(mc.atttyp) AS NUMERIC_SCALE,"+
		"internal_datetime_scale(mc.atttyp) AS DATETIME_PRECISION,"+
		"(case internal_column_character_set(mc.atttyp) WHEN 0 then 'utf8' WHEN 1 then 'utf8' WHEN 2 then 'binary' else NULL end) AS CHARACTER_SET_NAME,"+
		"(case internal_column_character_set(mc.atttyp) WHEN 0 then 'utf8_bin' WHEN 1 then 'utf8_bin' WHEN 2 then 'binary' else NULL end) AS COLLATION_NAME,"+
		"(case when length(mc.attr_enum) > 0 then mo_show_visible_bin_enum(mc.atttyp, mc.attr_enum) else mo_show_visible_bin(mc.atttyp,3) end) as COLUMN_TYPE,"+
		"case when mc.att_constraint_type = 'p' or mk.key_priority = 3 then 'PRI' when mk.key_priority = 2 then 'UNI' when mk.key_priority = 1 then 'MUL' else '' end as COLUMN_KEY,"+
		"cast(case when mc.att_is_auto_increment = 1 then 'auto_increment' when mc.attr_has_generated = 1 then ifnull(mo_show_visible_bin(mc.attr_generated, 6), '') else '' end as varchar(24)) as EXTRA,"+
		"'select,insert,update,references' as `PRIVILEGES`,"+
		"mc.att_comment as COLUMN_COMMENT,"+
		"cast(case when mc.attr_has_generated = 1 then ifnull(cast(mo_show_visible_bin(mc.attr_generated, 5) as varchar(500)), '') else '' end as varchar(500)) as GENERATION_EXPRESSION,"+
		"(case when upper(mo_show_visible_bin(mc.atttyp,3)) like '%% SRID %%' "+
		" then cast(split_part(upper(mo_show_visible_bin(mc.atttyp,3)), ' SRID ', 2) as bigint) else NULL end) as SRS_ID "+
		"from mo_catalog.mo_columns mc join __mo_visible_tables mt ON mc.account_id = mt.account_id AND mc.att_database = mt.reldatabase AND mc.att_relname = mt.relname "+
		"left join (select ki.table_id, ki.column_name, "+
		"max(case when ki.type = 'PRIMARY' then 3 when ki.type = 'UNIQUE' and kp.part_count = 1 then 2 else 1 end) as key_priority "+
		"from mo_catalog.mo_indexes ki "+
		"join (select id, count(*) as part_count from mo_catalog.mo_indexes group by id) kp on ki.id = kp.id "+
		"where (ki.type = 'PRIMARY' or ki.ordinal_position = 1) and ki.type in ('PRIMARY', 'UNIQUE', 'MULTIPLE', 'FULLTEXT', 'SPATIAL') "+
		"group by ki.table_id, ki.column_name) mk ON mk.table_id = mt.rel_id AND mk.column_name = mc.attname "+
		"where mc.account_id = current_account_id() "+
		"and mc.att_is_hidden = 0 and mc.att_relname!='%s' and mc.att_relname not like '%s' and mc.attname != '%s' and mc.att_relname not like '%s' and mc.att_relname != '%s' and not startswith(mc.att_relname, '%s') and %s",
		catalog.MOAutoIncrTable, catalog.PrefixPriColName+"%", catalog.Row_ID, catalog.PartitionSubTableWildcard, catalog.MO_ACCOUNT_LOCK, catalog.IndexTableNamePrefix, catalog.NonTemporaryTableSQLPredicate("mt"))

	InformationSchemaColumnsDDL = informationSchemaSubscriptionColumnsDDL()

	InformationSchemaProfilingDDL = "CREATE TABLE information_schema.PROFILING (" +
		"QUERY_ID int NOT NULL DEFAULT '0'," +
		"SEQ int NOT NULL DEFAULT '0'," +
		"STATE varchar(30) NOT NULL DEFAULT ''," +
		"DURATION decimal(9,6) NOT NULL DEFAULT '0.000000'," +
		"CPU_USER decimal(9,6) DEFAULT NULL," +
		"CPU_SYSTEM decimal(9,6) DEFAULT NULL," +
		"CONTEXT_VOLUNTARY int DEFAULT NULL," +
		"CONTEXT_INVOLUNTARY int DEFAULT NULL," +
		"BLOCK_OPS_IN int DEFAULT NULL," +
		"BLOCK_OPS_OUT int DEFAULT NULL," +
		"MESSAGES_SENT int DEFAULT NULL," +
		"MESSAGES_RECEIVED int DEFAULT NULL," +
		"PAGE_FAULTS_MAJOR int DEFAULT NULL," +
		"PAGE_FAULTS_MINOR int DEFAULT NULL," +
		"SWAPS int DEFAULT NULL," +
		"SOURCE_FUNCTION varchar(30) DEFAULT NULL," +
		"SOURCE_FILE varchar(20) DEFAULT NULL," +
		"SOURCE_LINE int DEFAULT NULL" +
		")"

	InformationSchemaProcesslistDDL = fmt.Sprintf("CREATE VIEW %s.PROCESSLIST AS "+
		"select node_id, conn_id, session_id, account, user, host, db, "+
		"session_start, command, info, txn_id, statement_id, statement_type, "+
		"query_type, sql_source_type, query_start, client_host, role, proxy_host "+
		"from PROCESSLIST() A", InformationDBConst)

	InformationSchemaUserPrivilegesDDL = "CREATE TABLE information_schema.USER_PRIVILEGES (" +
		"GRANTEE varchar(292) NOT NULL DEFAULT ''," +
		"TABLE_CATALOG varchar(512) NOT NULL DEFAULT ''," +
		"PRIVILEGE_TYPE varchar(64) NOT NULL DEFAULT ''," +
		"IS_GRANTABLE varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaSchemataDDL = "CREATE VIEW information_schema.SCHEMATA AS " +
		informationSchemaMetadataVisibilityCTE() + "SELECT " +
		"'def' AS CATALOG_NAME," +
		"datname AS SCHEMA_NAME," +
		"'utf8mb4' AS DEFAULT_CHARACTER_SET_NAME," +
		"'" + DefaultCollationForCharset("utf8mb4") + "' AS DEFAULT_COLLATION_NAME," +
		"if(true, NULL, '') AS SQL_PATH," +
		"cast('NO' as varchar(3)) AS DEFAULT_ENCRYPTION " +
		"FROM __mo_visible_databases"

	InformationSchemaCharacterSetsDDL = "CREATE TABLE information_schema.CHARACTER_SETS (" +
		"CHARACTER_SET_NAME varchar(64)," +
		"DEFAULT_COLLATE_NAME varchar(64)," +
		"DESCRIPTION varchar(2048)," +
		"MAXLEN int unsigned" +
		")"

	InformationSchemaCharacterSetsData = informationSchemaCharacterSetsDataSQL()

	InformationSchemaTriggersDDL = "CREATE TABLE information_schema.TRIGGERS (" +
		"TRIGGER_CATALOG varchar(64)," +
		"TRIGGER_SCHEMA varchar(64)," +
		"TRIGGER_NAME varchar(64)," +
		"EVENT_MANIPULATION varchar(10)," +
		"EVENT_OBJECT_CATALOG varchar(64)," +
		"EVENT_OBJECT_SCHEMA varchar(64)," +
		"EVENT_OBJECT_TABLE varchar(64)," +
		"ACTION_ORDER int unsigned," +
		"ACTION_CONDITION binary(0)," +
		"ACTION_STATEMENT longtext," +
		"ACTION_ORIENTATION varchar(3)," +
		"ACTION_TIMING varchar(10)," +
		"ACTION_REFERENCE_OLD_TABLE binary(0)," +
		"ACTION_REFERENCE_NEW_TABLE binary(0)," +
		"ACTION_REFERENCE_OLD_ROW varchar(3)," +
		"ACTION_REFERENCE_NEW_ROW varchar(3)," +
		"CREATED timestamp(2)," +
		"SQL_MODE varchar(10)," +
		"DEFINER varchar(288)," +
		"CHARACTER_SET_CLIENT varchar(64)," +
		"COLLATION_CONNECTION varchar(64)," +
		"DATABASE_COLLATION varchar(64)" +
		")"

	InformationSchemaTablesV41DDL = fmt.Sprintf("CREATE VIEW information_schema.TABLES AS "+informationSchemaMetadataVisibilityCTE()+
		"SELECT 'def' AS TABLE_CATALOG,"+
		"reldatabase AS TABLE_SCHEMA,"+
		"relname AS TABLE_NAME,"+
		"(case when relkind = 'v' and (reldatabase='mo_catalog' or reldatabase='information_schema') then 'SYSTEM VIEW' "+
		"when relkind = 'v'  then 'VIEW' "+
		"when relkind = 'e' then 'EXTERNAL TABLE' "+
		"when relkind = 'r' then 'BASE TABLE' "+
		"else 'INTERNAL TABLE' end) AS TABLE_TYPE,"+
		"if(relkind = 'r','Tae',NULL) AS ENGINE,"+
		"if(relkind = 'v',NULL,10) AS VERSION,"+
		"'Compressed' AS ROW_FORMAT,"+
		"if(relkind = 'v', NULL, 0) AS TABLE_ROWS,"+
		"if(relkind = 'v', NULL, 0) AS AVG_ROW_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS DATA_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS MAX_DATA_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS INDEX_LENGTH,"+
		"if(relkind = 'v', NULL, 0) AS DATA_FREE,"+
		"if(relkind = 'v', NULL, internal_auto_increment(reldatabase, relname)) AS `AUTO_INCREMENT`,"+
		"created_time AS CREATE_TIME,"+
		"if(relkind = 'v', NULL, created_time) AS UPDATE_TIME,"+
		"if(relkind = 'v', NULL, created_time) AS CHECK_TIME,"+
		"'"+DefaultCollationForCharset("utf8mb4")+"' AS TABLE_COLLATION,"+
		"if(relkind = 'v', NULL, 0) AS CHECKSUM,"+
		"if(relkind = 'v', NULL, if(partitioned = 0, '', cast('partitioned' as varchar(256)))) AS CREATE_OPTIONS,"+
		"cast(rel_comment as text) AS TABLE_COMMENT "+
		"FROM __mo_visible_tables tbl "+
		"WHERE tbl.account_id = current_account_id() and tbl.relname not like '%s' and %s and tbl.relname != '%s' and tbl.relkind != '%s'",
		catalog.IndexTableNamePrefix+"%", catalog.NonTemporaryTableSQLPredicate("tbl"), catalog.MO_ACCOUNT_LOCK, catalog.SystemPartitionRel)

	InformationSchemaTablesDDL = informationSchemaSubscriptionTablesDDL()

	InformationSchemaPartitionsDDL = "CREATE VIEW information_schema.`PARTITIONS` AS " +
		informationSchemaMetadataVisibilityCTE() + "SELECT " +
		"'def' AS `TABLE_CATALOG`," +
		"`tbl`.`reldatabase` AS `TABLE_SCHEMA`," +
		"`tbl`.`relname` AS `TABLE_NAME`," +
		"`pt`.`partition_name` AS `PARTITION_NAME`," +
		"NULL AS `SUBPARTITION_NAME`," +
		"(`pt`.`partition_ordinal_position` + 1) AS `PARTITION_ORDINAL_POSITION`," +
		"NULL AS `SUBPARTITION_ORDINAL_POSITION`," +
		"(case `meta`.`partition_method` " +
		"when 'Key' then NULL " +
		"when 'LinearKey' then 'LINEAR KEY' " +
		"when 'Hash' then 'HASH' " +
		"when 'LinearHash' then 'LINEAR HASH' " +
		"when 'Range' then (case when `meta`.`partition_description` like '%columns%' then 'RANGE COLUMNS' else 'RANGE' end) " +
		"when 'List' then 'LIST' " +
		"else NULL end) AS `PARTITION_METHOD`," +
		"NULL AS `SUBPARTITION_METHOD`," +
		"(case when `meta`.`partition_description` like '%(%' then " +
		"  replace( " +
		"    substring( " +
		"      `meta`.`partition_description`, " +
		"      locate('(', `meta`.`partition_description`) + 1, " +
		"      (length(`meta`.`partition_description`) - locate(')', reverse(`meta`.`partition_description`)) + 1) - locate('(', `meta`.`partition_description`) - 1 " +
		"    ), '`', '' " +
		"  ) " +
		"when `meta`.`partition_description` like '%)' then " +
		"  replace(`meta`.`partition_description`, '`', '') " +
		"else `meta`.`partition_description` end) AS `PARTITION_EXPRESSION`," +
		"NULL AS `SUBPARTITION_EXPRESSION`," +
		"(case when `pt`.`partition_expression_str` like 'values less than%' then " +
		"  substring(`pt`.`partition_expression_str`, locate('(', `pt`.`partition_expression_str`) + 1, " +
		"    locate(')', `pt`.`partition_expression_str`) - locate('(', `pt`.`partition_expression_str`) - 1" +
		"  ) " +
		"when `pt`.`partition_expression_str` like 'values in%' then " +
		"  `pt`.`partition_expression_str` " +
		"else `pt`.`partition_expression_str` end) AS `PARTITION_DESCRIPTION`," +
		"mo_table_rows(`tbl`.`reldatabase`, `pt`.`partition_table_name`) AS `TABLE_ROWS`," +
		"0 AS `AVG_ROW_LENGTH`," +
		"mo_table_size(`tbl`.`reldatabase`, `pt`.`partition_table_name`) AS `DATA_LENGTH`," +
		"0 AS `MAX_DATA_LENGTH`," +
		"0 AS `INDEX_LENGTH`," +
		"0 AS `DATA_FREE`," +
		"`tbl`.`created_time` AS `CREATE_TIME`," +
		"NULL AS `UPDATE_TIME`," +
		"NULL AS `CHECK_TIME`," +
		"NULL AS `CHECKSUM`," +
		"''  AS `PARTITION_COMMENT`," +
		"'default' AS `NODEGROUP`," +
		"NULL AS `TABLESPACE_NAME` " +
		"FROM `__mo_visible_tables` `tbl` " +
		"JOIN `mo_catalog`.`mo_partition_metadata` `meta` ON `meta`.`table_id` = `tbl`.`rel_id` " +
		"JOIN `mo_catalog`.`mo_partition_tables` `pt` ON `pt`.`primary_table_id` = `tbl`.`rel_id` " +
		"WHERE `tbl`.`account_id` = current_account_id()"

	InformationSchemaViewsDDL = "CREATE VIEW information_schema.VIEWS AS " +
		informationSchemaMetadataVisibilityCTE() + "SELECT 'def' AS `TABLE_CATALOG`," +
		"tbl.reldatabase AS `TABLE_SCHEMA`," +
		"tbl.relname AS `TABLE_NAME`," +
		"tbl.rel_createsql AS `VIEW_DEFINITION`," +
		"'NONE' AS `CHECK_OPTION`," +
		"'YES' AS `IS_UPDATABLE`," +
		"usr.user_name + '@' + usr.user_host AS `DEFINER`," +
		"'DEFINER' AS `SECURITY_TYPE`," +
		"'utf8mb4' AS `CHARACTER_SET_CLIENT`," +
		"'" + DefaultCollationForCharset("utf8mb4") + "' AS `COLLATION_CONNECTION` " +
		"FROM mo_catalog.mo_tables tbl " +
		"JOIN __mo_visible_tables visible_tbl ON tbl.account_id = visible_tbl.account_id AND tbl.rel_id = visible_tbl.rel_id " +
		"LEFT JOIN mo_catalog.mo_user usr ON tbl.creator = usr.user_id " +
		"WHERE tbl.account_id = current_account_id() and tbl.relkind = 'v' and tbl.reldatabase != 'information_schema'"

	InformationSchemaStatisticsDDL = fmt.Sprintf("CREATE VIEW information_schema.`STATISTICS` AS "+informationSchemaMetadataVisibilityCTE()+
		"select 'def' AS `TABLE_CATALOG`,"+
		"`tbl`.`reldatabase` AS `TABLE_SCHEMA`,"+
		"`tbl`.`relname` AS `TABLE_NAME`,"+
		"if(((`idx`.`type` = 'PRIMARY') or (`idx`.`type` = 'UNIQUE')),0,1) AS `NON_UNIQUE`,"+
		"`tbl`.`reldatabase` AS `INDEX_SCHEMA`,"+
		"`idx`.`name` AS `INDEX_NAME`,"+
		"`idx`.`ordinal_position` AS `SEQ_IN_INDEX`,"+
		"`idx`.`column_name` AS `COLUMN_NAME`,"+
		"'A' AS `COLLATION`,"+
		"0 AS `CARDINALITY`,"+
		"NULL AS `SUB_PART`,"+
		"NULL AS `PACKED`,"+
		"if((`tcl`.`attnotnull` = 0),'YES','') AS `NULLABLE`,"+
		"`idx`.`algo` AS `INDEX_TYPE`,"+
		"if(((`idx`.`type` = 'PRIMARY') or (`idx`.`type` = 'UNIQUE')),'','') AS `COMMENT`,"+
		"`idx`.`comment` AS `INDEX_COMMENT`,"+
		"if(`idx`.`is_visible`,'YES','NO') AS `IS_VISIBLE`,"+
		"NULL AS `EXPRESSION` "+
		"from (`mo_catalog`.`mo_indexes` `idx` "+
		"join `__mo_visible_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)) "+
		"join `mo_catalog`.`mo_columns` `tcl` on (`idx`.`table_id` = `tcl`.`att_relname_id` and `idx`.`column_name` = `tcl`.`attname` "+
		"and `tcl`.`account_id` = `tbl`.`account_id` and `tcl`.`att_database` = `tbl`.`reldatabase` and `tcl`.`att_relname` = `tbl`.`relname`) "+
		"where `tbl`.`account_id` = current_account_id() and not startswith(`tbl`.`relname`, '%s') and %s "+
		"group by `tbl`.`reldatabase`, `tbl`.`relname`, `idx`.`type`, `idx`.`name`, "+
		"`idx`.`ordinal_position`, `idx`.`column_name`, `tcl`.`attnotnull`, `idx`.`algo`, "+
		"`idx`.`comment`, `idx`.`is_visible`",
		catalog.IndexTableNamePrefix, catalog.NonTemporaryTableSQLPredicate("tbl"))

	InformationSchemaReferentialConstraintsDDL = "CREATE VIEW information_schema.REFERENTIAL_CONSTRAINTS AS " +
		informationSchemaMetadataVisibilityCTE() + "SELECT " +
		"'def' AS CONSTRAINT_CATALOG, " +
		"fk.db_name AS CONSTRAINT_SCHEMA, " +
		"fk.constraint_name AS CONSTRAINT_NAME, " +
		"'def' AS UNIQUE_CONSTRAINT_CATALOG, " +
		"fk.refer_db_name AS UNIQUE_CONSTRAINT_SCHEMA, " +
		"fk.referenced_index_name AS UNIQUE_CONSTRAINT_NAME," +
		"'NONE' AS MATCH_OPTION, " +
		"replace(fk.on_update, '_', ' ') AS UPDATE_RULE, " +
		"replace(fk.on_delete, '_', ' ') AS DELETE_RULE, " +
		"fk.table_name AS TABLE_NAME, " +
		"fk.refer_table_name AS REFERENCED_TABLE_NAME " +
		"FROM (" +
		"SELECT db_name, table_name, constraint_name, refer_db_name, refer_table_name, on_update, on_delete, referenced_index_name " +
		"FROM mo_catalog.mo_foreign_keys " +
		"GROUP BY db_name, table_name, constraint_name, refer_db_name, refer_table_name, on_update, on_delete, referenced_index_name" +
		") fk " +
		"JOIN __mo_visible_tables fk_tbl " +
		"ON fk.db_name = fk_tbl.reldatabase AND fk.table_name = fk_tbl.relname"

	// CHECK_CONSTRAINTS is backed by a table function because CHECK metadata is
	// stored in the serialized SchemaExtra of each table.  The function decodes
	// that metadata at query time and applies the current tenant's visibility.
	InformationSchemaCheckConstraintsDDL = "CREATE VIEW information_schema.CHECK_CONSTRAINTS AS " +
		informationSchemaMetadataVisibilityCTE() + "SELECT " +
		"cc.constraint_catalog AS CONSTRAINT_CATALOG, " +
		"cc.constraint_schema AS CONSTRAINT_SCHEMA, " +
		"cc.constraint_name AS CONSTRAINT_NAME, " +
		"cc.check_clause AS CHECK_CLAUSE " +
		"FROM mo_check_constraints() cc " +
		"JOIN __mo_visible_tables check_tbl " +
		"ON cc.constraint_schema = check_tbl.reldatabase AND cc.table_name = check_tbl.relname"

	InformationSchemaEnginesDDL = "CREATE TABLE information_schema.ENGINES (" +
		"ENGINE varchar(64)," +
		"SUPPORT varchar(8)," +
		"COMMENT varchar(160)," +
		"TRANSACTIONS varchar(3)," +
		"XA varchar(3)," +
		"SAVEPOINTS varchar(3)" +
		")"

	InformationSchemaRoutinesDDL = "CREATE TABLE information_schema.ROUTINES (" +
		"SPECIFIC_NAME varchar(64)," +
		"ROUTINE_CATALOG varchar(64)," +
		"ROUTINE_SCHEMA varchar(64)," +
		"ROUTINE_NAME varchar(64)," +
		"ROUTINE_TYPE varchar(10)," +
		"DATA_TYPE  longtext," +
		"CHARACTER_MAXIMUM_LENGTH bigint," +
		"CHARACTER_OCTET_LENGTH bigint," +
		"NUMERIC_PRECISION int unsigned," +
		"NUMERIC_SCALE int unsigned," +
		"DATETIME_PRECISION int unsigned," +
		"CHARACTER_SET_NAME varchar(64)," +
		"COLLATION_NAME varchar(64)," +
		"DTD_IDENTIFIER longtext," +
		"ROUTINE_BODY varchar(3)," +
		"ROUTINE_DEFINITION longtext," +
		"EXTERNAL_NAME binary(0)," +
		"EXTERNAL_LANGUAGE varchar(64)," +
		"PARAMETER_STYLE varchar(3)," +
		"IS_DETERMINISTIC varchar(3)," +
		"SQL_DATA_ACCESS varchar(10)," +
		"SQL_PATH varchar(1000)," +
		"SECURITY_TYPE varchar(10)," +
		"CREATED timestamp," +
		"LAST_ALTERED timestamp," +
		"SQL_MODE varchar(1000)," +
		"ROUTINE_COMMENT text," +
		"DEFINER varchar(288)," +
		"CHARACTER_SET_CLIENT varchar(64)," +
		"COLLATION_CONNECTION varchar(64)," +
		"DATABASE_COLLATION  varchar(64)" +
		")"

	InformationSchemaParametersDDL = "CREATE TABLE information_schema.PARAMETERS (" +
		"SPECIFIC_CATALOG varchar(64)," +
		"SPECIFIC_SCHEMA varchar(64)," +
		"SPECIFIC_NAME varchar(64)," +
		"ORDINAL_POSITION bigint unsigned," +
		"PARAMETER_MODE varchar(5)," +
		"PARAMETER_NAME varchar(64)," +
		"DATA_TYPE longtext," +
		"CHARACTER_MAXIMUM_LENGTH bigint," +
		"CHARACTER_OCTET_LENGTH bigint," +
		"NUMERIC_PRECISION int unsigned," +
		"NUMERIC_SCALE bigint," +
		"DATETIME_PRECISION int unsigned," +
		"CHARACTER_SET_NAME varchar(64)," +
		"COLLATION_NAME varchar(64)," +
		"DTD_IDENTIFIER mediumtext," +
		"ROUTINE_TYPE  varchar(64)" +
		")"

	InformationSchemaKeywordsDDL = "CREATE TABLE information_schema.KEYWORDS (" +
		"WORD varchar(64)," +
		"RESERVED int unsigned" +
		")"

	InformationSchemaSchemaPrivilegesDDL = "CREATE TABLE information_schema.`SCHEMA_PRIVILEGES` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaTablePrivilegesDDL = "CREATE VIEW information_schema.`TABLE_PRIVILEGES` AS " +
		informationSchemaMetadataVisibilityCTE() +
		", __mo_can_inspect_all_table_grants AS (" +
		"SELECT 1 FROM mo_catalog.mo_role_privs inspect_priv " +
		"JOIN __mo_active_roles inspect_role ON inspect_priv.role_id = inspect_role.role_id " +
		"WHERE inspect_priv.obj_type = 'account' AND inspect_priv.obj_id = 0 " +
		"AND inspect_priv.privilege_level = '*' " +
		"AND inspect_priv.privilege_name IN ('manage grants','account all','account ownership') LIMIT 1" +
		"), __mo_authorized_table_grants AS (" +
		"SELECT grant_priv.role_id, grant_priv.obj_id, grant_priv.privilege_name, grant_priv.with_grant_option " +
		"FROM mo_catalog.mo_role_privs grant_priv " +
		"JOIN __mo_active_roles grant_role ON grant_priv.role_id = grant_role.role_id " +
		"WHERE grant_priv.obj_type IN ('table','view') AND grant_priv.privilege_level IN ('d.t','t') " +
		"UNION ALL " +
		"SELECT grant_priv.role_id, grant_priv.obj_id, grant_priv.privilege_name, grant_priv.with_grant_option " +
		"FROM mo_catalog.mo_role_privs grant_priv " +
		"WHERE EXISTS (SELECT 1 FROM __mo_can_inspect_all_table_grants) " +
		"AND grant_priv.role_id NOT IN (SELECT role_id FROM __mo_active_roles) " +
		"AND grant_priv.obj_type IN ('table','view') AND grant_priv.privilege_level IN ('d.t','t')" +
		"), __mo_concrete_table_privileges(privilege_type) AS (" +
		"SELECT 'SELECT' UNION ALL SELECT 'INSERT' UNION ALL SELECT 'UPDATE' UNION ALL SELECT 'TRUNCATE' " +
		"UNION ALL SELECT 'DELETE' UNION ALL SELECT 'REFERENCE' UNION ALL SELECT 'INDEX' UNION ALL SELECT 'VALUES'" +
		"), __mo_expanded_table_grant_rows AS (" +
		"SELECT grant_priv.role_id, grant_priv.obj_id, upper(grant_priv.privilege_name) AS privilege_type, " +
		"grant_priv.with_grant_option FROM __mo_authorized_table_grants grant_priv " +
		"WHERE grant_priv.privilege_name <> 'table all' " +
		"UNION ALL " +
		"SELECT grant_priv.role_id, grant_priv.obj_id, concrete_priv.privilege_type, grant_priv.with_grant_option " +
		"FROM __mo_authorized_table_grants grant_priv CROSS JOIN __mo_concrete_table_privileges concrete_priv " +
		"WHERE grant_priv.privilege_name = 'table all'" +
		"), __mo_expanded_table_grants AS (" +
		"SELECT role_id, obj_id, privilege_type, " +
		"max(cast(with_grant_option AS int)) = 1 AS with_grant_option " +
		"FROM __mo_expanded_table_grant_rows GROUP BY role_id, obj_id, privilege_type" +
		") SELECT " +
		"CAST(coalesce(granted_role.role_name, '') AS varchar(292)) AS `GRANTEE`," +
		"CAST('def' AS varchar(512)) AS `TABLE_CATALOG`," +
		"CAST(coalesce(tbl.reldatabase, '') AS varchar(64)) AS `TABLE_SCHEMA`," +
		"CAST(coalesce(tbl.relname, '') AS varchar(64)) AS `TABLE_NAME`," +
		"CAST(coalesce(grant_priv.privilege_type, '') AS varchar(64)) AS `PRIVILEGE_TYPE`," +
		"CAST(coalesce(case when grant_priv.with_grant_option then 'YES' else 'NO' end, '') AS varchar(3)) AS `IS_GRANTABLE` " +
		"FROM __mo_expanded_table_grants grant_priv " +
		"JOIN mo_catalog.mo_role granted_role ON grant_priv.role_id = granted_role.role_id " +
		"JOIN __mo_visible_tables tbl ON grant_priv.obj_id = tbl.rel_logical_id " +
		"WHERE tbl.account_id = current_account_id()"

	InformationSchemaColumnPrivilegesDDL = "CREATE TABLE information_schema.`COLUMN_PRIVILEGES` (" +
		"`GRANTEE` varchar(292) NOT NULL DEFAULT ''," +
		"`TABLE_CATALOG` varchar(512) NOT NULL DEFAULT ''," +
		"`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT ''," +
		"`TABLE_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`COLUMN_NAME` varchar(64) NOT NULL DEFAULT ''," +
		"`PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT ''," +
		"`IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''" +
		")"

	InformationSchemaCollationsDDL = "CREATE TABLE information_schema.COLLATIONS (" +
		"COLLATION_NAME varchar(64) NOT NULL," +
		"CHARACTER_SET_NAME varchar(64) NOT NULL," +
		"ID bigint unsigned NOT NULL DEFAULT 0," +
		"IS_DEFAULT varchar(3) NOT NULL DEFAULT ''," +
		"IS_COMPILED varchar(3) NOT NULL DEFAULT ''," +
		"SORTLEN int unsigned NOT NULL," +
		"PAD_ATTRIBUTE enum('PAD SPACE','NO PAD') NOT NULL" +
		")"

	InformationSchemaCollationsData = informationSchemaCollationsDataSQL()

	// MySQL exposes the collation-to-character-set mapping as a separate
	// information_schema object.  Keep it derived from COLLATIONS so the two
	// metadata surfaces cannot disagree when collation rows are populated.
	InformationSchemaCollationCharacterSetApplicabilityDDL = "CREATE VIEW information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS " +
		"SELECT COLLATION_NAME, CHARACTER_SET_NAME " +
		"FROM information_schema.COLLATIONS"

	InformationSchemaTableConstraintsDDL = fmt.Sprintf("CREATE VIEW information_schema.TABLE_CONSTRAINTS AS "+informationSchemaMetadataVisibilityCTE()+"SELECT "+
		"'def' AS CONSTRAINT_CATALOG, "+
		"tbl.reldatabase AS CONSTRAINT_SCHEMA, "+
		"idx.name AS CONSTRAINT_NAME, "+
		"tbl.reldatabase AS TABLE_SCHEMA, "+
		"tbl.relname AS TABLE_NAME, "+
		"case idx.type when 'PRIMARY' then 'PRIMARY KEY' else idx.type end AS CONSTRAINT_TYPE, "+
		"'YES' AS ENFORCED "+
		"FROM mo_catalog.mo_indexes idx "+
		"join __mo_visible_tables tbl on idx.table_id = tbl.rel_id "+
		"where tbl.account_id = current_account_id() and idx.type in ('PRIMARY', 'UNIQUE') and not startswith(tbl.relname, '%s') and %s "+
		"group by tbl.reldatabase, idx.name, tbl.relname, idx.type UNION ALL "+
		"SELECT 'def' AS CONSTRAINT_CATALOG, "+
		"fk.db_name AS CONSTRAINT_SCHEMA, "+
		"fk.constraint_name AS CONSTRAINT_NAME, "+
		"fk.db_name AS TABLE_SCHEMA, "+
		"fk.table_name AS TABLE_NAME, "+
		"'FOREIGN KEY' AS CONSTRAINT_TYPE, "+
		"'YES' AS ENFORCED "+
		"FROM mo_catalog.mo_foreign_keys fk "+
		"join __mo_visible_tables fk_tbl on fk.db_name = fk_tbl.reldatabase and fk.table_name = fk_tbl.relname "+
		"group by fk.db_name, fk.constraint_name, fk.table_name UNION ALL "+
		"SELECT cc.constraint_catalog AS CONSTRAINT_CATALOG, "+
		"cc.constraint_schema AS CONSTRAINT_SCHEMA, "+
		"cc.constraint_name AS CONSTRAINT_NAME, "+
		"cc.constraint_schema AS TABLE_SCHEMA, "+
		"cc.table_name AS TABLE_NAME, "+
		"cc.constraint_type AS CONSTRAINT_TYPE, "+
		"cc.enforced AS ENFORCED "+
		"FROM mo_check_constraints() cc "+
		"join __mo_visible_tables check_tbl on cc.constraint_schema = check_tbl.reldatabase and cc.table_name = check_tbl.relname", catalog.IndexTableNamePrefix, catalog.NonTemporaryTableSQLPredicate("tbl"))

	InformationSchemaTableConstraintsLegacyDDL = fmt.Sprintf("CREATE VIEW information_schema.TABLE_CONSTRAINTS AS "+informationSchemaMetadataVisibilityCTE()+"SELECT "+
		"'def' AS CONSTRAINT_CATALOG, "+
		"tbl.reldatabase AS CONSTRAINT_SCHEMA, "+
		"idx.name AS CONSTRAINT_NAME, "+
		"tbl.reldatabase AS TABLE_SCHEMA, "+
		"tbl.relname AS TABLE_NAME, "+
		"case idx.type when 'PRIMARY' then 'PRIMARY KEY' else idx.type end AS CONSTRAINT_TYPE, "+
		"'YES' AS ENFORCED "+
		"FROM mo_catalog.mo_indexes idx "+
		"join __mo_visible_tables tbl on idx.table_id = tbl.rel_id "+
		"where tbl.account_id = current_account_id() and idx.type in ('PRIMARY', 'UNIQUE') and not startswith(tbl.relname, '%s') and %s "+
		"group by tbl.reldatabase, idx.name, tbl.relname, idx.type UNION ALL "+
		"SELECT 'def' AS CONSTRAINT_CATALOG, "+
		"fk.db_name AS CONSTRAINT_SCHEMA, "+
		"fk.constraint_name AS CONSTRAINT_NAME, "+
		"fk.db_name AS TABLE_SCHEMA, "+
		"fk.table_name AS TABLE_NAME, "+
		"'FOREIGN KEY' AS CONSTRAINT_TYPE, "+
		"'YES' AS ENFORCED "+
		"FROM mo_catalog.mo_foreign_keys fk "+
		"join __mo_visible_tables fk_tbl on fk.db_name = fk_tbl.reldatabase and fk.table_name = fk_tbl.relname "+
		"group by fk.db_name, fk.constraint_name, fk.table_name",
		catalog.IndexTableNamePrefix, catalog.NonTemporaryTableSQLPredicate("tbl"))

	InformationSchemaEventsDDL = "CREATE TABLE information_schema.EVENTS (" +
		"EVENT_CATALOG varchar(64)," +
		"EVENT_SCHEMA varchar(64)," +
		"EVENT_NAME varchar(64) NOT NULL," +
		"`DEFINER` varchar(288) NOT NULL," +
		"TIME_ZONE varchar(64) NOT NULL," +
		"EVENT_BODY varchar(3) NOT NULL DEFAULT ''," +
		"EVENT_DEFINITION longtext NOT NULL," +
		"EVENT_TYPE varchar(9) NOT NULL DEFAULT ''," +
		"EXECUTE_AT datetime," +
		"INTERVAL_VALUE varchar(256)," +
		"INTERVAL_FIELD enum('YEAR','QUARTER','MONTH','DAY','HOUR','MINUTE','WEEK','SECOND','MICROSECOND','YEAR_MONTH','DAY_HOUR','DAY_MINUTE','DAY_SECOND','HOUR_MINUTE','HOUR_SECOND','MINUTE_SECOND','DAY_MICROSECOND','HOUR_MICROSECOND','MINUTE_MICROSECOND','SECOND_MICROSECOND')," +
		"SQL_MODE varchar(64) NOT NULL," +
		"STARTS datetime," +
		"ENDS datetime," +
		"STATUS varchar(21) NOT NULL DEFAULT ''," +
		"ON_COMPLETION varchar(12) NOT NULL DEFAULT ''," +
		"CREATED timestamp NOT NULL," +
		"LAST_ALTERED timestamp NOT NULL," +
		"LAST_EXECUTED datetime," +
		"EVENT_COMMENT varchar(2048) NOT NULL," +
		"ORIGINATOR int unsigned NOT NULL," +
		"CHARACTER_SET_CLIENT varchar(64) NOT NULL," +
		"COLLATION_CONNECTION varchar(64) NOT NULL," +
		"DATABASE_COLLATION varchar(64) NOT NULL" +
		")"

	InformationSchemaFilesDDL = "CREATE TABLE information_schema.FILES (" +
		"FILE_ID  bigint NOT NULL," +
		"FILE_NAME  text NOT NULL," +
		"FILE_TYPE  varchar(256)," +
		"TABLESPACE_NAME  varchar(268) NOT NULL," +
		"TABLE_CATALOG  char(0) NOT NULL," +
		"TABLE_SCHEMA  binary(0)," +
		"TABLE_NAME  binary(0)," +
		"LOGFILE_GROUP_NAME  varchar(256)," +
		"LOGFILE_GROUP_NUMBER  bigint," +
		"ENGINE  varchar(64) NOT NULL," +
		"FULLTEXT_KEYS  binary(0)," +
		"DELETED_ROWS  binary(0)," +
		"UPDATE_COUNT  binary(0)," +
		"FREE_EXTENTS  bigint," +
		"TOTAL_EXTENTS  bigint," +
		"EXTENT_SIZE  bigint," +
		"INITIAL_SIZE  bigint," +
		"MAXIMUM_SIZE  bigint," +
		"AUTOEXTEND_SIZE  bigint," +
		"CREATION_TIME  binary(0)," +
		"LAST_UPDATE_TIME  binary(0)," +
		"LAST_ACCESS_TIME  binary(0)," +
		"RECOVER_TIME  binary(0)," +
		"TRANSACTION_COUNTER  binary(0)," +
		"VERSION  bigint," +
		"ROW_FORMAT  varchar(256)," +
		"TABLE_ROWS  binary(0)," +
		"AVG_ROW_LENGTH  binary(0)," +
		"DATA_LENGTH  binary(0)," +
		"MAX_DATA_LENGTH  binary(0)," +
		"INDEX_LENGTH  binary(0)," +
		"DATA_FREE  bigint," +
		"CREATE_TIME  binary(0)," +
		"UPDATE_TIME  binary(0)," +
		"CHECK_TIME  binary(0)," +
		"CHECKSUM  binary(0)," +
		"STATUS  varchar(256)," +
		"EXTRA  varchar(256)" +
		")"
)

func informationSchemaCollationsDataSQL() string {
	values := make([]string, 0, len(SupportedCollationDefinitions))
	for _, collation := range SupportedCollationDefinitions {
		values = append(values, fmt.Sprintf("('%s', '%s', %d, '%s', '%s', %d, '%s')",
			collation.Name,
			collation.Charset,
			collation.ID,
			collation.IsDefault,
			collation.IsCompiled,
			collation.SortLen,
			collation.PadAttribute,
		))
	}
	return "INSERT INTO information_schema.COLLATIONS VALUES " + strings.Join(values, ",")
}

func informationSchemaCharacterSetsDataSQL() string {
	values := []string{
		fmt.Sprintf("('binary','%s','Binary pseudo charset',1)", DefaultCollationForCharset("binary")),
		fmt.Sprintf("('utf8','%s','UTF-8 Unicode',4)", DefaultCollationForCharset("utf8")),
		fmt.Sprintf("('utf8mb4','%s','UTF-8 Unicode',4)", DefaultCollationForCharset("utf8mb4")),
	}
	return "INSERT INTO information_schema.CHARACTER_SETS VALUES " + strings.Join(values, ",")
}
