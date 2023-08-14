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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
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

var needUpgradNewView = []*table.Table{PARTITIONSView}
