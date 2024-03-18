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

package v1_2_1

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_test_view,
	upg_test_table,
}

var test_view_sql = fmt.Sprintf("CREATE VIEW information_schema.wuxiliang_cluster_view_v121 AS select "+
	"'def' as TABLE_CATALOG,"+
	"att_database as TABLE_SCHEMA,"+
	"att_relname AS TABLE_NAME,"+
	"attname AS COLUMN_NAME,"+
	"attnum AS ORDINAL_POSITION,"+
	"mo_show_visible_bin(att_default,1) as COLUMN_DEFAULT,"+
	"(case when attnotnull != 0 then 'NO' else 'YES' end) as IS_NULLABLE,"+
	"mo_show_visible_bin(atttyp,2) as DATA_TYPE,"+
	"internal_char_length(atttyp) AS CHARACTER_MAXIMUM_LENGTH,"+
	"internal_char_size(atttyp) AS CHARACTER_OCTET_LENGTH,"+
	"internal_numeric_precision(atttyp) AS NUMERIC_PRECISION,"+
	"internal_numeric_scale(atttyp) AS NUMERIC_SCALE,"+
	"internal_datetime_scale(atttyp) AS DATETIME_PRECISION,"+
	"(case internal_column_character_set(atttyp) WHEN 0 then 'utf8' WHEN 1 then 'utf8' else NULL end) AS CHARACTER_SET_NAME,"+
	"(case internal_column_character_set(atttyp) WHEN 0 then 'utf8_bin' WHEN 1 then 'utf8_bin' else NULL end) AS COLLATION_NAME,"+
	"mo_show_visible_bin(atttyp,3) as COLUMN_TYPE,"+
	"case when att_constraint_type = 'p' then 'PRI' else '' end as COLUMN_KEY,"+
	"case when att_is_auto_increment = 1 then 'auto_increment' else '' end as EXTRA,"+
	"'select,insert,update,references' as `PRIVILEGES`,"+
	"att_comment as COLUMN_COMMENT,"+
	"cast('' as varchar(500)) as GENERATION_EXPRESSION,"+
	"if(true, NULL, 0) as SRS_ID "+
	"from mo_catalog.mo_columns where att_relname!='%s' and att_relname not like '%s' and attname != '%s'", catalog.MOAutoIncrTable, catalog.PrefixPriColName+"%", catalog.Row_ID)

var upg_test_view = versions.UpgradeEntry{
	Schema:    "information_schema",
	TableName: "wuxiliang_cluster_view_v121",
	UpgType:   versions.CREATE_VIEW,
	TableType: versions.SYSTEM_VIEW,
	UpgSql:    test_view_sql,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExisted, _, err := versions.CheckViewDefinition(txn, accountId, "information_schema", "wuxiliang_cluster_view_v121")
		if err != nil {
			return false, err
		}

		if isExisted {
			return true, nil
		}
		return false, nil
	},
}

var upg_test_table = versions.UpgradeEntry{
	Schema:    "mo_catalog",
	TableName: "wuxiliang_cluster_table_v121",
	UpgType:   versions.CREATE_NEW_TABLE,
	TableType: versions.BASE_TABLE,
	UpgSql: fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ("+
		"SPECIFIC_CATALOG varchar(64),"+
		"SPECIFIC_SCHEMA varchar(64),"+
		"SPECIFIC_NAME varchar(64),"+
		"ORDINAL_POSITION bigint unsigned,"+
		"PARAMETER_MODE varchar(5),"+
		"PARAMETER_NAME varchar(64),"+
		"DATA_TYPE longtext,"+
		"CHARACTER_MAXIMUM_LENGTH bigint,"+
		"CHARACTER_OCTET_LENGTH bigint,"+
		"NUMERIC_PRECISION int unsigned,"+
		"NUMERIC_SCALE bigint,"+
		"DATETIME_PRECISION int unsigned,"+
		"CHARACTER_SET_NAME varchar(64),"+
		"COLLATION_NAME varchar(64),"+
		"DTD_IDENTIFIER mediumtext,"+
		"ROUTINE_TYPE  varchar(64)"+
		");", catalog.MO_CATALOG, "wuxiliang_cluster_table_v121"),

	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, "wuxiliang_cluster_table_v121")
		if err != nil {
			return false, nil
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

//var test_view_sql2 = fmt.Sprintf("CREATE VIEW information_schema.test_view AS select "+
//	"'def' as TABLE_WUXILIANG,"+
//	"att_database as TABLE_SCHEMA,"+
//	"att_relname AS TABLE_NAME,"+
//	"attname AS COLUMN_NAME,"+
//	"attnum AS ORDINAL_POSITION,"+
//	"mo_show_visible_bin(att_default,1) as COLUMN_DEFAULT,"+
//	"(case when attnotnull != 0 then 'NO' else 'YES' end) as IS_NULLABLE,"+
//	"mo_show_visible_bin(atttyp,2) as DATA_TYPE,"+
//	"internal_char_length(atttyp) AS CHARACTER_MAXIMUM_LENGTH,"+
//	"internal_char_size(atttyp) AS CHARACTER_OCTET_LENGTH,"+
//	"internal_numeric_precision(atttyp) AS NUMERIC_PRECISION,"+
//	"internal_numeric_scale(atttyp) AS NUMERIC_SCALE,"+
//	"internal_datetime_scale(atttyp) AS DATETIME_PRECISION,"+
//	"(case internal_column_character_set(atttyp) WHEN 0 then 'utf8' WHEN 1 then 'utf8' else NULL end) AS CHARACTER_SET_NAME,"+
//	"(case internal_column_character_set(atttyp) WHEN 0 then 'utf8_bin' WHEN 1 then 'utf8_bin' else NULL end) AS COLLATION_NAME,"+
//	"mo_show_visible_bin(atttyp,3) as COLUMN_TYPE,"+
//	"case when att_constraint_type = 'p' then 'PRI' else '' end as COLUMN_KEY,"+
//	"case when att_is_auto_increment = 1 then 'auto_increment' else '' end as EXTRA,"+
//	"'select,insert,update,references' as `PRIVILEGES`,"+
//	"att_comment as COLUMN_COMMENT,"+
//	"cast('' as varchar(500)) as GENERATION_EXPRESSION,"+
//	"if(true, NULL, 0) as SRS_ID "+
//	"from mo_catalog.mo_columns where att_relname!='%s' and att_relname not like '%s' and attname != '%s'", catalog.MOAutoIncrTable, catalog.PrefixPriColName+"%", catalog.Row_ID)
//
//var upg_test_view2 = versions.UpgradeEntry{
//	Schema:    "information_schema",
//	TableName: "test_view",
//	UpgType:   versions.MODIFY_VIEW,
//	TableType: versions.SYSTEM_VIEW,
//	UpgSql:    test_view_sql2,
//	CheckFunc: func(txn executor.TxnExecutor) (bool, error) {
//		isExisted, curViewDef, err := versions.CheckViewDefinition(txn, "information_schema", "test_view")
//		if err != nil {
//			return false, err
//		}
//
//		if isExisted {
//			// If the current view definition is different from the new view definition, execute the update view definition operation
//			if curViewDef != test_view_sql2 {
//				return true, nil
//			} else {
//				// Delete the current view
//				err := versions.DropView(txn, "information_schema", "test_view")
//				if err != nil {
//					return false, err
//				}
//				return false, nil
//			}
//		} else {
//			return false, nil
//		}
//	},
//}
