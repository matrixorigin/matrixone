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

package frontend

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

var (
	// the sqls creating many tables for the tenant.
	// Wrap them in a transaction
	MoCatalogMoUserDDL = `create table mo_catalog.mo_user (
				user_id int signed auto_increment primary key,
				user_host varchar(100),
				user_name varchar(300) unique key,
				authentication_string varchar(100),
				status   varchar(8),
				created_time  timestamp,
				expired_time timestamp,
				login_type  varchar(16),
				creator int signed,
				owner int signed,
				default_role int signed
    		)`

	MoCatalogMoAccountDDL = `create table mo_catalog.mo_account (
				account_id int signed auto_increment primary key,
				account_name varchar(300) unique key,
				admin_name varchar(300),
				status varchar(300),
				created_time timestamp,
				comments varchar(256),
				version bigint unsigned auto_increment,
				suspended_time timestamp default NULL,
				create_version varchar(50) default '1.2.0'
			)`

	MoCatalogMoRoleDDL = `create table mo_catalog.mo_role (
				role_id int signed auto_increment primary key,
				role_name varchar(300) unique key,
				creator int signed,
				owner int signed,
				created_time timestamp,
				comments text
			)`

	MoCatalogMoUserGrantDDL = `create table mo_catalog.mo_user_grant (
				role_id int signed,
				user_id int signed,
				granted_time timestamp,
				with_grant_option bool,
				primary key(role_id, user_id)
			)`

	MoCatalogMoRoleGrantDDL = `create table mo_catalog.mo_role_grant (
				granted_id int signed,
				grantee_id int signed,
				operation_role_id int signed,
				operation_user_id int signed,
				granted_time timestamp,
				with_grant_option bool,
				primary key(granted_id, grantee_id)
			)`

	MoCatalogMoRolePrivsDDL = `create table mo_catalog.mo_role_privs (
				role_id int signed,
				role_name  varchar(100),
				obj_type  varchar(16),
				obj_id bigint unsigned,
				privilege_id int,
				privilege_name varchar(100),
				privilege_level varchar(100),
				operation_user_id int unsigned,
				granted_time timestamp,
				with_grant_option bool,
				primary key(role_id, obj_type, obj_id, privilege_id, privilege_level)
			)`

	MoCatalogMoUserDefinedFunctionDDL = `create table mo_catalog.mo_user_defined_function (
				function_id int auto_increment,
				name     varchar(100) unique key,
				owner  int unsigned,
				args     json,
				retType  varchar(20),
				body     text,
				language varchar(20),
				db       varchar(100),
				definer  varchar(50),
				modified_time timestamp,
				created_time  timestamp,
				type    varchar(10),
				security_type varchar(10),
				comment  varchar(5000),
				character_set_client varchar(64),
				collation_connection varchar(64),
				database_collation varchar(64),
				primary key(function_id)
			)`

	MoCatalogMoMysqlCompatibilityModeDDL = `create table mo_catalog.mo_mysql_compatibility_mode (
				configuration_id int auto_increment,
				account_id int,
				account_name varchar(300),
				dat_name     varchar(5000) default NULL,
				variable_name  varchar(300),
				variable_value varchar(5000),
				system_variables bool,
				primary key(configuration_id)
			)`

	MoCatalogMoSnapshotsDDL = fmt.Sprintf(`CREATE TABLE %s.%s (
			snapshot_id uuid unique key,
			sname varchar(64) primary key,
			ts bigint,
			level enum('cluster','account','database','table'),
	        account_name varchar(300),
			database_name varchar(5000),
			table_name  varchar(5000),
			obj_id bigint unsigned
			)`, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS)

	MoCatalogMoPitrDDL = fmt.Sprintf(`CREATE TABLE %s.%s (
			pitr_id uuid unique key,
			pitr_name varchar(5000),
			create_account bigint unsigned,
			create_time timestamp,
			modified_time timestamp,
			level varchar(10),
			account_id bigint unsigned,
			account_name varchar(300),
			database_name varchar(5000),
			table_name varchar(5000),
			obj_id bigint unsigned,
			pitr_length tinyint unsigned,
			pitr_unit varchar(10),
			primary key(pitr_name, create_account)
			)`, catalog.MO_CATALOG, catalog.MO_PITR)

	MoCatalogMoRetentionDDL = fmt.Sprintf(`CREATE TABLE %s.%s (
    		database_name varchar(5000),
			table_name varchar(5000),
    		retention_deadline bigint unsigned,
    		primary key(database_name, table_name)
    		)`, catalog.MO_CATALOG, catalog.MO_RETENTION)

	MoCatalogMoPubsDDL = `create table mo_catalog.mo_pubs (
    		pub_name varchar(64) primary key,
    		database_name varchar(5000),
    		database_id bigint unsigned,
    		all_table bool,
    		table_list text,
    		account_list text,
    		created_time timestamp,
    		update_time timestamp default NULL,
    		owner int unsigned,
    		creator int unsigned,
    		comment text
    		)`

	MoCatalogMoSubsDDL = `create table mo_catalog.mo_subs (
			sub_account_id INT NOT NULL, 
			sub_name VARCHAR(5000) DEFAULT NULL,
			sub_time TIMESTAMP DEFAULT NULL,
			pub_account_name VARCHAR(300) NOT NULL,
			pub_name VARCHAR(64) NOT NULL,
			pub_database VARCHAR(5000) NOT NULL,
			pub_tables TEXT NOT NULL,
			pub_time TIMESTAMP NOT NULL,
			pub_comment TEXT NOT NULL,
			status TINYINT(8) NOT NULL,
			PRIMARY KEY (pub_account_name, pub_name, sub_account_id),
			UNIQUE KEY (sub_account_id, sub_name)
	)`

	MoCatalogMoStoredProcedureDDL = `create table mo_catalog.mo_stored_procedure (
				proc_id int auto_increment,
				name     varchar(100) unique key,
				creator  int unsigned,
				args     text,
				body     text,
				db       varchar(100),
				definer  varchar(50),
				modified_time timestamp,
				created_time  timestamp,
				type    varchar(10),
				security_type varchar(10),
				comment  varchar(5000),
				character_set_client varchar(64),
				collation_connection varchar(64),
				database_collation varchar(64),
				primary key(proc_id)
			)`

	MoCatalogMoStagesDDL = `create table mo_catalog.mo_stages (
				stage_id int unsigned auto_increment,
				stage_name varchar(64) unique key,
				url text,
				stage_credentials text,
				stage_status varchar(64),
				created_time timestamp,
				comment text,
				primary key(stage_id)
			)`

	MoCatalogMoCdcTaskDDL = `create table mo_catalog.mo_cdc_task (
    			account_id bigint unsigned,			
    			task_id uuid,
    			task_name varchar(1000),
    			source_uri text not null,
    			source_password  varchar(1000),
    			sink_uri text not null,
    			sink_type      varchar(20),
    			sink_password  varchar(1000),
    			sink_ssl_ca_path varchar(65535),
    			sink_ssl_cert_path varchar(65535),
    			sink_ssl_key_path varchar(65535),
    			tables text not null,
    			filters text,
    			opfilters text,
    			source_state varchar(20),
    			sink_state varchar(20),
    			start_ts varchar(1000),
    			end_ts varchar(1000),
    			config_file varchar(65535),
    			task_create_time datetime,
    			state varchar(20),
    			checkpoint bigint unsigned,
    			checkpoint_str varchar(1000),
    			no_full bool,
    			incr_config varchar(1000),
    			reserved0 text,
    			reserved1 text,
    			reserved2 text,
    			reserved3 text,
    			reserved4 text,
    			primary key(account_id, task_id),
    			unique key(account_id, task_name)
			)`

	MoCatalogMoCdcWatermarkDDL = `create table mo_catalog.mo_cdc_watermark (
    			account_id bigint unsigned,			
    			task_id uuid,
    			table_id varchar(64),			
    			watermark varchar(128),			
    			primary key(account_id,task_id,table_id)
			)`

	MoCatalogMoSessionsDDL       = `CREATE VIEW mo_catalog.mo_sessions AS SELECT node_id, conn_id, session_id, account, user, host, db, session_start, command, info, txn_id, statement_id, statement_type, query_type, sql_source_type, query_start, client_host, role, proxy_host FROM mo_sessions() AS mo_sessions_tmp`
	MoCatalogMoConfigurationsDDL = `CREATE VIEW mo_catalog.mo_configurations AS SELECT node_type, node_id, name, current_value, default_value, internal FROM mo_configurations() AS mo_configurations_tmp`
	MoCatalogMoLocksDDL          = `CREATE VIEW mo_catalog.mo_locks AS SELECT cn_id, txn_id, table_id, lock_key, lock_content, lock_mode, lock_status, lock_wait FROM mo_locks() AS mo_locks_tmp`
	MoCatalogMoVariablesDDL      = `CREATE VIEW mo_catalog.mo_variables AS SELECT configuration_id, account_id, account_name, dat_name, variable_name, variable_value, system_variables FROM mo_catalog.mo_mysql_compatibility_mode`
	MoCatalogMoTransactionsDDL   = `CREATE VIEW mo_catalog.mo_transactions AS SELECT cn_id, txn_id, create_ts, snapshot_ts, prepared_ts, commit_ts, txn_mode, isolation, user_txn, txn_status, table_id, lock_key, lock_content, lock_mode FROM mo_transactions() AS mo_transactions_tmp`
	MoCatalogMoCacheDDL          = `CREATE VIEW mo_catalog.mo_cache AS SELECT node_type, node_id, type, used, free, hit_ratio FROM mo_cache() AS mo_cache_tmp`

	MoCatalogMoDataKeyDDL = `create table mo_catalog.mo_data_key (
    			account_id bigint unsigned,			
    			key_id uuid, 
    			encrypted_key varchar(128), 
				create_time timestamp not null default current_timestamp,
				update_time timestamp not null default current_timestamp on update current_timestamp,
    			primary key(account_id, key_id)
			)`
)

// `mo_catalog` database system tables
// Note: The following tables belong to data dictionary table, and system tables's creation will depend on
// the following system tables. Therefore, when creating tenants, they must be created first
var (
	MoCatalogMoAutoIncrTableDDL = fmt.Sprintf(`create table %s.%s (
			table_id   bigint unsigned, 
			col_name   varchar(770), 
			col_index  int,
			offset     bigint unsigned, 
			step       bigint unsigned,  
			primary key(table_id, col_name)
		)`, catalog.MO_CATALOG, catalog.MOAutoIncrTable)

	MoCatalogMoIndexesDDL = fmt.Sprintf(`create table %s.%s (
			id 			bigint unsigned not null,
			table_id 	bigint unsigned not null,
			database_id bigint unsigned not null,
			name 		varchar(64) not null,
			type        varchar(11) not null,
    		algo	varchar(11),
    		algo_table_type varchar(11),
			algo_params varchar(2048),
			is_visible  tinyint not null,
			hidden      tinyint not null,
			comment 	varchar(2048) not null,
			column_name    varchar(256) not null,
			ordinal_position  int unsigned  not null,
			options     text,
			index_table_name varchar(5000),
			primary key(id, column_name)
		)`, catalog.MO_CATALOG, catalog.MO_INDEXES)

	MoCatalogMoForeignKeysDDL = fmt.Sprintf(`create table %s.%s (
			constraint_name varchar(5000) not null,
			constraint_id BIGINT UNSIGNED not null default 0,
			db_name varchar(5000) not null,
			db_id BIGINT UNSIGNED not null default 0,
			table_name varchar(5000) not null,
			table_id BIGINT UNSIGNED not null default 0,
			column_name varchar(256) not null,
			column_id BIGINT UNSIGNED not null default 0,
			refer_db_name varchar(5000) not null,
			refer_db_id BIGINT UNSIGNED not null default 0,
			refer_table_name varchar(5000) not null,
			refer_table_id BIGINT UNSIGNED not null default 0,
			refer_column_name varchar(256) not null,
			refer_column_id BIGINT UNSIGNED not null default 0,
			on_delete varchar(128) not null,
			on_update varchar(128) not null,
	
			primary key(
				constraint_name,
				constraint_id,
				db_name,
				db_id,
				table_name,
				table_id,
				column_name,
				column_id,
				refer_db_name,
				refer_db_id,
				refer_table_name,
				refer_table_id,
				refer_column_name,
				refer_column_id)
		)`, catalog.MO_CATALOG, catalog.MOForeignKeys)

	MoCatalogMoTablePartitionsDDL = fmt.Sprintf(`CREATE TABLE %s.%s (
			  table_id bigint unsigned NOT NULL,
			  database_id bigint unsigned not null,
			  number smallint unsigned NOT NULL,
			  name varchar(64) NOT NULL,
    		  partition_type varchar(50) NOT NULL,
              partition_expression varchar(2048) NULL,
			  description_utf8 text,
			  comment varchar(2048) NOT NULL,
			  options text,
			  partition_table_name varchar(1024) NOT NULL,
    		  PRIMARY KEY table_id (table_id, name)
			)`, catalog.MO_CATALOG, catalog.MO_TABLE_PARTITIONS)
)

// step3InitSQLs
// `mo_catalog` database system tables
// They are all Cluster level system tables for system upgrades
var (
	MoCatalogMoVersionDDL = fmt.Sprintf(`create table %s.%s (
			version             varchar(50) not null,
		    version_offset      int unsigned default 0,
			state               int,
			create_at           timestamp not null,
			update_at           timestamp not null,
			primary key(version, version_offset)
		)`, catalog.MO_CATALOG, catalog.MOVersionTable)

	MoCatalogMoUpgradeDDL = fmt.Sprintf(`create table %s.%s (
			id                   bigint unsigned not null primary key auto_increment,
			from_version         varchar(50) not null,
			to_version           varchar(50) not null,
			final_version        varchar(50) not null,
            final_version_offset int unsigned default 0,
			state                int,
			upgrade_cluster      int,
			upgrade_tenant       int,
			upgrade_order        int,
			total_tenant         int,
			ready_tenant         int,
			create_at            timestamp not null,
			update_at            timestamp not null
		)`, catalog.MO_CATALOG, catalog.MOUpgradeTable)

	MoCatalogMoUpgradeTenantDDL = fmt.Sprintf(`create table %s.%s (
			id                  bigint unsigned not null primary key auto_increment,
			upgrade_id		    bigint unsigned not null,
			target_version      varchar(50) not null,
			from_account_id     int not null,
			to_account_id       int not null,
			ready               int,
			create_at           timestamp not null,
			update_at           timestamp not null
		)`, catalog.MO_CATALOG, catalog.MOUpgradeTenantTable)
)

// ----------------------------------------------------------------------------------------------------------------------
// step2InitSQLs
// `mo_task` database system tables
// They are all Cluster level system tables
var (
	MoTaskSysAsyncTaskDDL = fmt.Sprintf(`create table %s.sys_async_task (
			task_id                     bigint primary key auto_increment,
			task_metadata_id            varchar(50) not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option        varchar(1000),
			task_parent_id              varchar(50),
			task_status                 int,
			task_runner                 varchar(50),
			task_epoch                  int,
			last_heartbeat              bigint,
			result_code                 int null,
			error_msg                   varchar(1000) null,
			create_at                   bigint,
			end_at                      bigint)`,
		catalog.MOTaskDB)

	MoTaskSysCronTaskDDL = fmt.Sprintf(`create table %s.sys_cron_task (
			cron_task_id				bigint primary key auto_increment,
    		task_metadata_id            varchar(50) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option 		varchar(1000),
			cron_expr					varchar(100) not null,
			next_time					bigint,
			trigger_times				int,
			create_at					bigint,
			update_at					bigint)`,
		catalog.MOTaskDB)

	MoTaskSysDaemonTaskDDL = fmt.Sprintf(`create table %s.sys_daemon_task (
			task_id                     bigint primary key auto_increment,
			task_metadata_id            varchar(50),
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option        varchar(1000),
			account_id                  int unsigned not null,
			account                     varchar(128) not null,
			task_type                   varchar(64) not null,
			task_runner                 varchar(64),
			task_status                 int not null,
			last_heartbeat              timestamp,
			create_at                   timestamp not null,
			update_at                   timestamp not null,
			end_at                      timestamp,
			last_run                    timestamp,
			details                     blob)`,
		catalog.MOTaskDB)
)
