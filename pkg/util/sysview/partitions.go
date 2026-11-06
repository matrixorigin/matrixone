// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sysview

// These projection fragments are part of the v4.2.8 published migration.
// A current-only projection change must use new fragments, not alter these.
const (
	informationSchemaPartitionsCreate     = "CREATE VIEW information_schema.`PARTITIONS` AS "
	informationSchemaPartitionsProjection = "'def' AS `TABLE_CATALOG`," +
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
		"else `pt`.`partition_expression_str` end) AS `PARTITION_DESCRIPTION`,"
	informationSchemaPartitionsAverageRowLength = "0 AS `AVG_ROW_LENGTH`,"
	informationSchemaPartitionsProjectionTail   = "0 AS `MAX_DATA_LENGTH`," +
		"0 AS `INDEX_LENGTH`," +
		"0 AS `DATA_FREE`," +
		"`tbl`.`created_time` AS `CREATE_TIME`," +
		"NULL AS `UPDATE_TIME`," +
		"NULL AS `CHECK_TIME`," +
		"NULL AS `CHECKSUM`," +
		"''  AS `PARTITION_COMMENT`," +
		"'default' AS `NODEGROUP`," +
		"NULL AS `TABLESPACE_NAME` "

	// InformationSchemaPartitionsRelease428DDL is the frozen published view.
	InformationSchemaPartitionsRelease428DDL = informationSchemaPartitionsCreate + "SELECT " +
		informationSchemaPartitionsProjection +
		"mo_table_rows(`tbl`.`reldatabase`, `pt`.`partition_table_name`) AS `TABLE_ROWS`," +
		informationSchemaPartitionsAverageRowLength +
		"mo_table_size(`tbl`.`reldatabase`, `pt`.`partition_table_name`) AS `DATA_LENGTH`," +
		informationSchemaPartitionsProjectionTail +
		"FROM `mo_catalog`.`mo_tables` `tbl` " +
		"JOIN `mo_catalog`.`mo_partition_metadata` `meta` ON `meta`.`table_id` = `tbl`.`rel_id` " +
		"JOIN `mo_catalog`.`mo_partition_tables` `pt` ON `pt`.`primary_table_id` = `tbl`.`rel_id` " +
		"WHERE `tbl`.`account_id` = current_account_id()"
)
