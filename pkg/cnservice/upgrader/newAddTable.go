// Copyright 2021 - 2023 Matrix Origin
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

package upgrader

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

var (
	// mo_table_partitions;
	MoTablePartitionsTable = &table.Table{
		Account:  table.AccountAll,
		Database: catalog.MO_CATALOG,
		Table:    catalog.MO_TABLE_PARTITIONS,
		CreateTableSql: fmt.Sprintf(`CREATE TABLE %s.%s (
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
			);`, catalog.MO_CATALOG, catalog.MO_TABLE_PARTITIONS),
	}
)

var needUpgradNewTable = []*table.Table{MoTablePartitionsTable}

var PARTITIONSView = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "PARTITIONS",
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `information_schema`.`PARTITIONS` AS " +
		"SELECT " +
		"'def' AS `TABLE_CATALOG`," +
		"`tbl`.`reldatabase` AS `TABLE_SCHEMA`," +
		"`tbl`.`relname` AS `TABLE_NAME`," +
		"`part`.`name` AS `PARTITION_NAME`," +
		"NULL AS `SUBPARTITION_NAME`," +
		"`part`.`number` AS `PARTITION_ORDINAL_POSITION`," +
		"NULL AS `SUBPARTITION_ORDINAL_POSITION`," +
		"(case `part`.`partition_type` when 'HASH' then 'HASH' " +
		"when 'RANGE' then 'RANGE' " +
		"when 'LIST' then 'LIST' " +
		"when 'AUTO' then 'AUTO' " +
		"when 'KEY_51' then 'KEY' " +
		"when 'KEY_55' then 'KEY' " +
		"when 'LINEAR_KEY_51' then 'LINEAR KEY' " +
		"when 'LINEAR_KEY_55' then 'LINEAR KEY' " +
		"when 'LINEAR_HASH' then 'LINEAR HASH' " +
		"when 'RANGE_COLUMNS' then 'RANGE COLUMNS' " +
		"when 'LIST_COLUMNS' then 'LIST COLUMNS' else NULL end) AS `PARTITION_METHOD`," +
		"NULL AS `SUBPARTITION_METHOD`," +
		"`part`.`partition_expression` AS `PARTITION_EXPRESSION`," +
		"NULL AS `SUBPARTITION_EXPRESSION`," +
		"`part`.`description_utf8` AS `PARTITION_DESCRIPTION`," +
		"mo_table_rows(`tbl`.`reldatabase`, `part`.`partition_table_name`) AS `TABLE_ROWS`," +
		"0 AS `AVG_ROW_LENGTH`," +
		"mo_table_size(`tbl`.`reldatabase`, `part`.`partition_table_name`) AS `DATA_LENGTH`," +
		"0 AS `MAX_DATA_LENGTH`," +
		"0 AS `INDEX_LENGTH`," +
		"0 AS `DATA_FREE`," +
		"`tbl`.`created_time` AS `CREATE_TIME`," +
		"NULL AS `UPDATE_TIME`," +
		"NULL AS `CHECK_TIME`," +
		"NULL AS `CHECKSUM`," +
		"ifnull(`part`.`comment`,'')  AS `PARTITION_COMMENT`," +
		"'default' AS `NODEGROUP`," +
		"NULL AS `TABLESPACE_NAME` " +
		"FROM `mo_catalog`.`mo_tables` `tbl` LEFT JOIN `mo_catalog`.`mo_table_partitions` `part` " +
		"ON `part`.`table_id` = `tbl`.`rel_id` " +
		"WHERE `tbl`.`partitioned` = 1;",
}

var STATISTICSView = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "STATISTICS",
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `information_schema`.`STATISTICS` AS " +
		"select 'def' AS `TABLE_CATALOG`," +
		"`tbl`.`reldatabase` AS `TABLE_SCHEMA`," +
		"`tbl`.`relname` AS `TABLE_NAME`," +
		"if(((`idx`.`type` = 'PRIMARY') or (`idx`.`type` = 'UNIQUE')),0,1) AS `NON_UNIQUE`," +
		"`tbl`.`reldatabase` AS `INDEX_SCHEMA`," +
		"`idx`.`name` AS `INDEX_NAME`," +
		"`idx`.`ordinal_position` AS `SEQ_IN_INDEX`," +
		"`idx`.`column_name` AS `COLUMN_NAME`," +
		"'A' AS `COLLATION`," +
		"0 AS `CARDINALITY`," +
		"NULL AS `SUB_PART`," +
		"NULL AS `PACKED`," +
		"if((`tcl`.`attnotnull` = 0),'YES','') AS `NULLABLE`," +
		"NULL AS `INDEX_TYPE`," +
		"if(((`idx`.`type` = 'PRIMARY') or (`idx`.`type` = 'UNIQUE')),'','') AS `COMMENT`," +
		"`idx`.`comment` AS `INDEX_COMMENT`," +
		"if(`idx`.`is_visible`,'YES','NO') AS `IS_VISIBLE`," +
		"NULL AS `EXPRESSION`" +
		"from (`mo_catalog`.`mo_indexes` `idx` " +
		"join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`))" +
		"join `mo_catalog`.`mo_columns` `tcl` on (`idx`.`table_id` = `tcl`.`att_relname_id` and `idx`.`column_name` = `tcl`.`attname`)",
}

var processlistView = &table.Table{
	Account:  table.AccountAll,
	Database: sysview.InformationDBConst,
	Table:    "processlist",
	Columns: []table.Column{
		table.StringColumn("account", "the account name"),
		table.StringColumn("client_host", "the ip:port of the client"),
		table.StringColumn("command", "the COMMAND send by client"),
		table.UInt64Column("conn_id", "the connection id of the tcp between client"),
		table.StringColumn("db", "the database be used"),
		table.StringColumn("host", "the ip:port of the mo-server"),
		table.StringColumn("info", "the sql"),
		table.StringColumn("node_id", "the id of the cn"),
		table.StringColumn("query_start", "the start time of the statement"),
		table.StringColumn("query_type", "the kind of the statement. DQL,TCL,etc"),
		table.StringColumn("role", "the role of the user"),
		table.StringColumn("session_id", "the id of the session"),
		table.StringColumn("session_start", "the start time of the session"),
		table.StringColumn("sql_source_type", "where does the sql come from. internal,external, etc"),
		table.StringColumn("statement_id", "the id of the statement"),
		table.StringColumn("statement_type", "the type of the statement.Select,Delete,Insert,etc"),
		table.StringColumn("txn_id", "the id of the transaction"),
		table.StringColumn("user", "the user name"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `information_schema`.`PROCESSLIST` AS SELECT * FROM PROCESSLIST() A;",
	//actually drop view here
	CreateTableSql: "drop view if exists `information_schema`.`PROCESSLIST`;",
}

var MoSessionsView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_sessions",
	Columns: []table.Column{
		table.StringColumn("account", "the account name"),
		table.StringColumn("client_host", "the ip:port of the client"),
		table.StringColumn("command", "the COMMAND send by client"),
		table.UInt64Column("conn_id", "the connection id of the tcp between client"),
		table.StringColumn("db", "the database be used"),
		table.StringColumn("host", "the ip:port of the mo-server"),
		table.StringColumn("info", "the sql"),
		table.StringColumn("node_id", "the id of the cn"),
		table.StringColumn("query_start", "the start time of the statement"),
		table.StringColumn("query_type", "the kind of the statement. DQL,TCL,etc"),
		table.StringColumn("role", "the role of the user"),
		table.StringColumn("session_id", "the id of the session"),
		table.StringColumn("session_start", "the start time of the session"),
		table.StringColumn("sql_source_type", "where does the sql come from. internal,external, etc"),
		table.StringColumn("statement_id", "the id of the statement"),
		table.StringColumn("statement_type", "the type of the statement.Select,Delete,Insert,etc"),
		table.StringColumn("txn_id", "the id of the transaction"),
		table.StringColumn("user", "the user name"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_sessions` AS SELECT * FROM mo_sessions() AS mo_sessions_tmp;",
	//actually drop view here
	CreateTableSql: "drop view `mo_catalog`.`mo_sessions`;",
}

var MoConfigurationsView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_configurations",
	Columns: []table.Column{
		table.StringColumn("node_type", "the type of the node. cn,tn,log,proxy."),
		table.StringColumn("node_id", "the id of the node"),
		table.StringColumn("name", "the name of the configuration item"),
		table.UInt64Column("current_value", "the current value of the configuration item"),
		table.StringColumn("default_value", "the default value of the configuration item"),
		table.StringColumn("internal", "the configuration item is internal or external"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_configurations` AS SELECT * FROM mo_configurations() AS mo_configurations_tmp;",
	//actually drop view here
	CreateTableSql: "drop view `mo_catalog`.`mo_configurations`;",
}

var MoLocksView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_CATALOG,
	Table:    "mo_locks",
	Columns: []table.Column{
		table.StringColumn("txn_id", "the txn id which holds the lock"),
		table.StringColumn("table_id", "the table that the lock is on"),
		table.StringColumn("lock_key", "point or range"),
		table.UInt64Column("lock_content", "the content the clock is on"),
		table.StringColumn("lock_mode", "shared or exclusive"),
		table.StringColumn("lock_status", "acquired or wait"),
		table.StringColumn("lock_wait", "the txn that waits on the lock"),
	},
	CreateViewSql: "CREATE VIEW IF NOT EXISTS `mo_catalog`.`mo_locks` AS SELECT * FROM mo_locks() AS mo_locks_tmp;",
	//actually drop view here
	CreateTableSql: "drop view `mo_catalog`.`mo_locks`;",
}

var SqlStatementHotspotView = &table.Table{
	Account:  table.AccountAll,
	Database: catalog.MO_SYSTEM,
	Table:    motrace.SqlStatementHotspotTbl,
	Columns:  []table.Column{},
	// CreateViewSql get sql from original View define.
	CreateViewSql: motrace.SqlStatementHotspotView.ToCreateSql(context.Background(), true),
	//actually drop view here
	CreateTableSql: "DROP VIEW IF EXISTS `system`.`sql_statement_hotspot`;",
}

var needUpgradNewView = []*table.Table{PARTITIONSView, STATISTICSView, MoSessionsView, SqlStatementHotspotView, MoLocksView, MoConfigurationsView}
var registeredViews = []*table.Table{processlistView}
