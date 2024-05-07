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
	//the sqls creating many tables for the tenant.
	//Wrap them in a transaction
	MoCatalogMoUser = `create table mo_user(
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
    		);`

	MoCatalogMoAccount = `create table mo_account(
				account_id int signed auto_increment primary key,
				account_name varchar(300) unique key,
				status varchar(300),
				created_time timestamp,
				comments varchar(256),
				version bigint unsigned auto_increment,
				suspended_time timestamp default NULL,
				create_version varchar(50) default '1.2.0'
			);`

	MoCatalogMoRole = `create table mo_role(
				role_id int signed auto_increment primary key,
				role_name varchar(300) unique key,
				creator int signed,
				owner int signed,
				created_time timestamp,
				comments text
			);`

	MoCatalogMoUserGrant = `create table mo_user_grant(
				role_id int signed,
				user_id int signed,
				granted_time timestamp,
				with_grant_option bool,
				primary key(role_id, user_id)
			);`

	MoCatalogMoRoleGrant = `create table mo_role_grant(
				granted_id int signed,
				grantee_id int signed,
				operation_role_id int signed,
				operation_user_id int signed,
				granted_time timestamp,
				with_grant_option bool,
				primary key(granted_id, grantee_id)
			);`

	MoCatalogMoRolePrivs = `create table mo_role_privs(
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
			);`

	MoCatalogMoUserDefinedFunction = `create table mo_user_defined_function(
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
			);`

	MoCatalogMoMysqlCompatibilityMode = `create table mo_mysql_compatibility_mode(
				configuration_id int auto_increment,
				account_id int,
				account_name varchar(300),
				dat_name     varchar(5000) default NULL,
				variable_name  varchar(300),
				variable_value varchar(5000),
				system_variables bool,
				primary key(configuration_id)
			);`

	MoCatalogMoSnapshots = `create table mo_snapshots(
			snapshot_id uuid unique key,
			sname varchar(64) primary key,
			ts bigint,
			level enum('cluster','account','database','table'),
	        account_name varchar(300),
			database_name varchar(5000),
			table_name  varchar(5000),
			obj_id bigint unsigned
			);`

	MoCatalogMoPubs = `create table mo_pubs(
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
    		);`

	MoCatalogMoStoredProcedure = `create table mo_stored_procedure(
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
			);`

	MoCatalogMoStages = `create table mo_stages(
				stage_id int unsigned auto_increment,
				stage_name varchar(64) unique key,
				url text,
				stage_credentials text,
				stage_status varchar(64),
				created_time timestamp,
				comment text,
				primary key(stage_id)
			);`

	MoCatalogMoSessions       = `CREATE VIEW IF NOT EXISTS mo_sessions AS SELECT * FROM mo_sessions() AS mo_sessions_tmp;`
	MoCatalogMoConfigurations = `CREATE VIEW IF NOT EXISTS mo_configurations AS SELECT * FROM mo_configurations() AS mo_configurations_tmp;`
	MoCatalogMoLocks          = `CREATE VIEW IF NOT EXISTS mo_locks AS SELECT * FROM mo_locks() AS mo_locks_tmp;`
	MoCatalogMoVariables      = `CREATE VIEW IF NOT EXISTS mo_variables AS SELECT * FROM mo_catalog.mo_mysql_compatibility_mode;`
	MoCatalogMoTransactions   = `CREATE VIEW IF NOT EXISTS mo_transactions AS SELECT * FROM mo_transactions() AS mo_transactions_tmp;`
	MoCatalogMoCache          = `CREATE VIEW IF NOT EXISTS mo_cache AS SELECT * FROM mo_cache() AS mo_cache_tmp;`
)

var (
	MoCatalogMoAutoIncrTable = fmt.Sprintf(`create table if not exists %s (
		table_id   bigint unsigned, 
		col_name     varchar(770), 
		col_index      int,
		offset     bigint unsigned, 
		step       bigint unsigned,  
		primary key(table_id, col_name)
	);`, catalog.MOAutoIncrTable)

	// mo_indexes is a data dictionary table, must be created first when creating tenants, and last when deleting tenants
	// mo_indexes table does not have `auto_increment` column,
	MoCatalogMoIndexes = fmt.Sprintf(`create table %s(
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
			);`, catalog.MO_INDEXES)

	MoCatalogMoForeignKeys = fmt.Sprintf(`create table %s(
				constraint_name varchar(5000) not null,
				constraint_id BIGINT UNSIGNED not null,
				db_name varchar(5000) not null,
				db_id BIGINT UNSIGNED not null,
				table_name varchar(5000) not null,
				table_id BIGINT UNSIGNED not null,
				column_name varchar(256) not null,
				column_id BIGINT UNSIGNED not null,
				refer_db_name varchar(5000) not null,
				refer_db_id BIGINT UNSIGNED not null,
				refer_table_name varchar(5000) not null,
				refer_table_id BIGINT UNSIGNED not null,
				refer_column_name varchar(256) not null,
				refer_column_id BIGINT UNSIGNED not null,
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
			);`, catalog.MOForeignKeys)

	MoCatalogMoTablePartitions = fmt.Sprintf(`CREATE TABLE %s (
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
			);`, catalog.MO_TABLE_PARTITIONS)
)
