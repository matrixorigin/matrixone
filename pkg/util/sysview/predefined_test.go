// Copyright 2025 Matrix Origin
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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func TestInformationSchemaMetadataViewsHideTemporaryTables(t *testing.T) {
	tests := []struct {
		name       string
		ddl        string
		tableAlias string
	}{
		{name: "tables", ddl: InformationSchemaTablesDDL, tableAlias: "tbl"},
		{name: "key column usage", ddl: InformationSchemaKeyColumnUsageDDL, tableAlias: "tbl"},
		{name: "columns", ddl: InformationSchemaColumnsDDL, tableAlias: "mt"},
		{name: "statistics", ddl: InformationSchemaStatisticsDDL, tableAlias: "tbl"},
		{name: "table constraints", ddl: InformationSchemaTableConstraintsDDL, tableAlias: "tbl"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Contains(t, test.ddl, catalog.NonTemporaryTableSQLPredicate(test.tableAlias))
			assert.Contains(t, test.ddl, catalog.SystemTemporaryTable)
			assert.Contains(t, test.ddl, catalog.SystemRelAttr_CreateSQL)
			assert.Contains(t, test.ddl, `[0-9a-f]{32}`)
			assert.NotContains(t, test.ddl, "relname not like '__mo_tmp_%'")
		})
	}
}

func TestInformationSchemaMetadataViewsEnforceObjectPrivileges(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
	}{
		{name: "tables", ddl: InformationSchemaTablesDDL},
		{name: "columns", ddl: InformationSchemaColumnsDDL},
		{name: "statistics", ddl: InformationSchemaStatisticsDDL},
		{name: "table constraints", ddl: InformationSchemaTableConstraintsDDL},
		{name: "legacy table constraints", ddl: InformationSchemaTableConstraintsLegacyDDL},
		{name: "key column usage", ddl: InformationSchemaKeyColumnUsageDDL},
		{name: "referential constraints", ddl: InformationSchemaReferentialConstraintsDDL},
		{name: "check constraints", ddl: InformationSchemaCheckConstraintsDDL},
		{name: "views", ddl: InformationSchemaViewsDDL},
		{name: "partitions", ddl: InformationSchemaPartitionsDDL},
		{name: "schemata", ddl: InformationSchemaSchemataDDL},
		{name: "table privileges", ddl: InformationSchemaTablePrivilegesDDL},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, expected := range []string{
				"WITH __mo_active_roles(role_id)",
				"SELECT role_id FROM mo_current_roles() role_closure",
				"__mo_visible_tables AS",
				"__mo_visible_databases AS",
				"tbl.account_id = current_account_id()",
				"tbl.owner IN (SELECT role_id FROM __mo_active_roles)",
				"db.owner = ar.role_id",
				"rp.obj_type IN ('table','view')",
				"rp.privilege_level = '*.*'",
				"rp.privilege_level IN ('d.*','*')",
				"rp.privilege_level IN ('d.t','t')",
				"rp.privilege_name IN ('show tables','database all','database ownership')",
			} {
				assert.Contains(t, test.ddl, expected)
			}
			assert.NotContains(t, test.ddl, "WITH RECURSIVE")
			assert.NotContains(t, test.ddl, "mo_catalog.mo_role_grant")
			assert.NotContains(t, test.ddl, "SELECT tbl.*")
			assert.NotContains(t, test.ddl, "current_role()")
			assert.NotContains(t, test.ddl, "FROM mo_catalog.mo_role ")

			statements, err := mysql.Parse(context.Background(), test.ddl, 1)
			assert.NoError(t, err)
			for _, statement := range statements {
				statement.Free()
			}
		})
	}

	assert.Contains(t, InformationSchemaTablesDDL, "FROM __mo_visible_tables tbl")
	assert.Contains(t, InformationSchemaColumnsDDL, "join __mo_visible_tables mt")
	assert.Contains(t, InformationSchemaStatisticsDDL, "join `__mo_visible_tables` `tbl`")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "join __mo_visible_tables tbl")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "join __mo_visible_tables check_tbl")
	fkVisibilityJoin := "ON fk.db_name = fk_tbl.reldatabase AND fk.table_name = fk_tbl.relname"
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, "JOIN __mo_visible_tables fk_tbl")
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, fkVisibilityJoin)
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL, "JOIN __mo_visible_tables fk_tbl")
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL, fkVisibilityJoin)
	assert.NotContains(t, InformationSchemaKeyColumnUsageDDL, "fk.table_id = fk_tbl.rel_id")
	assert.NotContains(t, InformationSchemaReferentialConstraintsDDL, "fk.table_id = fk_tbl.rel_id")
	assert.Contains(t, InformationSchemaCheckConstraintsDDL, "JOIN __mo_visible_tables check_tbl")
	assert.Contains(t, InformationSchemaViewsDDL, "JOIN __mo_visible_tables visible_tbl")
	assert.Contains(t, InformationSchemaPartitionsDDL, "FROM `__mo_visible_tables` `tbl`")
	assert.Contains(t, InformationSchemaSchemataDDL, "FROM __mo_visible_databases")
	assert.Contains(t, InformationSchemaSchemataDDL, "db.owner IN (SELECT role_id FROM __mo_active_roles)")
	assert.Contains(t, InformationSchemaSchemataDDL,
		"EXISTS (SELECT 1 FROM __mo_visible_tables tbl WHERE tbl.reldatabase_id = db.dat_id)")
	assert.Contains(t, InformationSchemaSchemataDDL,
		"rp.obj_type = 'account' AND rp.privilege_name IN ('show databases','account all')")
	assert.Contains(t, InformationSchemaSchemataDDL,
		"rp.privilege_level = '*' AND rp.obj_id = 0")
}

func TestInformationSchemaTablePrivilegesDDL(t *testing.T) {
	for _, expected := range []string{
		"CREATE VIEW information_schema.`TABLE_PRIVILEGES` AS",
		"CAST(coalesce(granted_role.role_name, '') AS varchar(292)) AS `GRANTEE`",
		"CAST('def' AS varchar(512)) AS `TABLE_CATALOG`",
		"CAST(coalesce(tbl.reldatabase, '') AS varchar(64)) AS `TABLE_SCHEMA`",
		"CAST(coalesce(tbl.relname, '') AS varchar(64)) AS `TABLE_NAME`",
		"CAST(coalesce(grant_priv.privilege_type, '') AS varchar(64)) AS `PRIVILEGE_TYPE`",
		"coalesce(case when grant_priv.with_grant_option then 'YES' else 'NO' end, '')",
		"JOIN __mo_active_roles grant_role ON grant_priv.role_id = grant_role.role_id",
		"inspect_priv.privilege_name IN ('manage grants','account all','account ownership')",
		"grant_priv.role_id NOT IN (SELECT role_id FROM __mo_active_roles)",
		"SELECT 'SELECT' UNION ALL SELECT 'INSERT' UNION ALL SELECT 'UPDATE' UNION ALL SELECT 'TRUNCATE'",
		"SELECT 'DELETE' UNION ALL SELECT 'REFERENCE' UNION ALL SELECT 'INDEX' UNION ALL SELECT 'VALUES'",
		"WHERE grant_priv.privilege_name <> 'table all'",
		"WHERE grant_priv.privilege_name = 'table all'",
		"FROM __mo_authorized_table_grants grant_priv CROSS JOIN __mo_concrete_table_privileges concrete_priv",
		"max(cast(with_grant_option AS int)) = 1 AS with_grant_option",
		"FROM __mo_expanded_table_grant_rows GROUP BY role_id, obj_id, privilege_type",
		"FROM __mo_expanded_table_grants grant_priv",
		"JOIN mo_catalog.mo_role granted_role ON grant_priv.role_id = granted_role.role_id",
		"JOIN __mo_visible_tables tbl ON grant_priv.obj_id = tbl.rel_logical_id",
		"tbl.account_id = current_account_id()",
		"grant_priv.obj_type IN ('table','view')",
		"grant_priv.privilege_level IN ('d.t','t')",
	} {
		assert.Contains(t, InformationSchemaTablePrivilegesDDL, expected)
	}
	assert.NotContains(t, InformationSchemaTablePrivilegesDDL, "grant_priv.privilege_level IN ('d.*','*')")
	assert.NotContains(t, InformationSchemaTablePrivilegesDDL, "grant_priv.privilege_level = '*.*'")
	assert.NotContains(t, InformationSchemaTablePrivilegesDDL,
		"CAST(coalesce(upper(grant_priv.privilege_name), '') AS varchar(64)) AS `PRIVILEGE_TYPE`")

	statements, err := mysql.Parse(context.Background(), InformationSchemaTablePrivilegesDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaStatisticsDDL_ContainsIdxAlgo(t *testing.T) {
	assert.True(t, strings.Contains(InformationSchemaStatisticsDDL, "`idx`.`algo` AS `INDEX_TYPE`"))
	assert.False(t, strings.Contains(InformationSchemaStatisticsDDL, "NULL AS `INDEX_TYPE`"))
	assert.Contains(t, InformationSchemaStatisticsDDL, "group by `tbl`.`reldatabase`, `tbl`.`relname`, `idx`.`type`, `idx`.`name`")
	assert.Contains(t, InformationSchemaStatisticsDDL, "not startswith(`tbl`.`relname`, '"+catalog.IndexTableNamePrefix+"')")
	statements, err := mysql.Parse(context.Background(), InformationSchemaStatisticsDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaTableConstraintsDDL_ContainsCheckConstraints(t *testing.T) {
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "UNION ALL")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "FROM mo_check_constraints() cc")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "FROM mo_catalog.mo_foreign_keys fk")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "idx.type in ('PRIMARY', 'UNIQUE')")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "then 'PRIMARY KEY'")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "group by tbl.reldatabase, idx.name, tbl.relname, idx.type")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "not startswith(tbl.relname, '"+catalog.IndexTableNamePrefix+"')")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "cc.table_name AS TABLE_NAME")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, catalog.NonTemporaryTableSQLPredicate("tbl"))
	statements, err := mysql.Parse(context.Background(), InformationSchemaTableConstraintsDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaTableConstraintsLegacyDDL_DoesNotUseCheckConstraints(t *testing.T) {
	assert.NotContains(t, InformationSchemaTableConstraintsLegacyDDL, "mo_check_constraints()")
	assert.Contains(t, InformationSchemaTableConstraintsLegacyDDL, "FROM mo_catalog.mo_foreign_keys fk")
	assert.Contains(t, InformationSchemaTableConstraintsLegacyDDL, "idx.type in ('PRIMARY', 'UNIQUE')")
	assert.Contains(t, InformationSchemaTableConstraintsLegacyDDL, catalog.NonTemporaryTableSQLPredicate("tbl"))
	statements, err := mysql.Parse(context.Background(), InformationSchemaTableConstraintsLegacyDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInitInformationSchemaSysTablesForProtocol(t *testing.T) {
	legacy := InitInformationSchemaSysTablesForProtocol(defines.MORPCVersion15)
	assert.NotContains(t, legacy, InformationSchemaCheckConstraintsDDL)
	assert.NotContains(t, legacy, InformationSchemaTableConstraintsDDL)
	assert.Contains(t, legacy,
		informationSchemaMetadataVisibilityCompatibilityDDL(InformationSchemaTableConstraintsLegacyDDL))
	assert.Contains(t, legacy, InformationSchemaCollationCharacterSetApplicabilityDDL)
	for _, sql := range legacy {
		assert.NotContains(t, sql, "mo_check_constraints()")
		assert.NotContains(t, sql, "mo_current_roles()")
		assertInformationSchemaInitSQLParses(t, sql)
	}

	for _, protocol := range []int64{
		defines.MORPCVersion16,
		defines.MORPCVersion32,
		defines.MORPCVersion34,
		defines.MORPCVersion35,
	} {
		t.Run(fmt.Sprintf("compatibility-v%d", protocol), func(t *testing.T) {
			compatibility := InitInformationSchemaSysTablesForProtocol(protocol)
			assert.Len(t, compatibility, len(InitInformationSchemaSysTables))
			assert.Contains(t, compatibility,
				informationSchemaMetadataVisibilityCompatibilityDDL(InformationSchemaCheckConstraintsDDL))
			assert.Contains(t, compatibility,
				informationSchemaMetadataVisibilityCompatibilityDDL(InformationSchemaTableConstraintsDDL))
			for _, sql := range compatibility {
				assert.NotContains(t, sql, "mo_current_roles()")
				assertInformationSchemaInitSQLParses(t, sql)
			}
			assert.Contains(t, strings.Join(compatibility, "\n"), "FROM mo_catalog.mo_role_grant rg")
		})
	}

	for _, protocol := range []int64{
		defines.MORPCVersion41,
		defines.MORPCVersion42,
		defines.MORPCVersion43,
		defines.MORPCVersion44,
		defines.MORPCVersion45,
	} {
		t.Run(fmt.Sprintf("local-catalog-v%d", protocol), func(t *testing.T) {
			localCatalog := InitInformationSchemaSysTablesForProtocol(protocol)
			assert.Len(t, localCatalog, len(InitInformationSchemaSysTables))
			assert.Contains(t, localCatalog, InformationSchemaTablesV41DDL)
			assert.Contains(t, localCatalog, InformationSchemaColumnsV41DDL)
			assert.NotContains(t, strings.Join(localCatalog, "\n"), "mo_subscription_tables()")
			assert.NotContains(t, strings.Join(localCatalog, "\n"), "mo_subscription_columns()")
			assert.Contains(t, strings.Join(localCatalog, "\n"), "mo_current_roles()")
			for _, sql := range localCatalog {
				assertInformationSchemaInitSQLParses(t, sql)
			}
		})
	}

	latest := InitInformationSchemaSysTablesForProtocol(defines.MORPCVersion46)
	assert.Equal(t, InitInformationSchemaSysTables, latest)
}

func assertInformationSchemaInitSQLParses(t *testing.T, sql string) {
	t.Helper()
	statements, err := mysql.Parse(context.Background(), sql, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaStatisticsDDL_RestrictsCatalogJoins(t *testing.T) {
	assert.True(t, strings.Contains(InformationSchemaStatisticsDDL, "`tcl`.`account_id` = `tbl`.`account_id`"))
	assert.True(t, strings.Contains(InformationSchemaStatisticsDDL, "`tcl`.`att_database` = `tbl`.`reldatabase`"))
	assert.True(t, strings.Contains(InformationSchemaStatisticsDDL, "`tcl`.`att_relname` = `tbl`.`relname`"))
	assert.True(t, strings.Contains(InformationSchemaStatisticsDDL, "`tbl`.`account_id` = current_account_id()"))
}

func TestInformationSchemaColumnsDDL_UsesConnectorCompatibleDataType(t *testing.T) {
	assert.Contains(t, InformationSchemaColumnsDDL, "lower(case when length(mc.attr_enum) > 0 then")
	assert.Contains(t, InformationSchemaColumnsDDL, "case when upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'TINYINT'")
	assert.Contains(t, InformationSchemaColumnsDDL, "else split_part(mo_show_visible_bin(mc.atttyp,2), ' ', 1) end) end) as DATA_TYPE")
}

func TestInformationSchemaSubscriptionMetadataDDL(t *testing.T) {
	assert.Contains(t, InformationSchemaTablesDDL, "FROM mo_subscription_tables()")
	assert.NotContains(t, InformationSchemaTablesV41DDL, "mo_subscription_tables()")
	assert.Equal(t, 1, strings.Count(InformationSchemaTablesV41DDL, "internal_auto_increment("))
	assert.NotContains(t, InformationSchemaColumnsDDL, "mo_subscription_tables()")
	assert.Contains(t, InformationSchemaColumnsDDL, "from mo_subscription_columns() mc")
	assert.NotContains(t, InformationSchemaColumnsV41DDL, "mo_subscription_tables()")
	assert.NotContains(t, InformationSchemaColumnsV41DDL, "mo_subscription_columns()")
	assert.Contains(t, InformationSchemaTablesDDL, "FROM __mo_visible_tables tbl")
	assert.Contains(t, InformationSchemaTablesDDL, "FROM mo_subscription_tables() tbl")
	assert.Contains(t, InformationSchemaTablesDDL, "tbl.owner IN (SELECT role_id FROM __mo_active_roles)")
	assert.Contains(t, InformationSchemaTablesDDL, "rp.obj_id = tbl.rel_logical_id")
	assert.Equal(t, 1, strings.Count(InformationSchemaTablesDDL, "mo_subscription_tables()"))
	assert.Equal(t, 1, strings.Count(InformationSchemaTablesDDL, "internal_auto_increment("))
	assert.Contains(t, InformationSchemaTablesDDL,
		"if(relkind = 'v', NULL, cast(0 as bigint unsigned)) AS `AUTO_INCREMENT`")
	assert.Contains(t, InformationSchemaColumnsDDL, "UNION ALL select 'def' as TABLE_CATALOG")
	assert.Contains(t, InformationSchemaColumnsDDL, "mc.table_owner IN (SELECT role_id FROM __mo_active_roles)")
	assert.Contains(t, InformationSchemaColumnsDDL, "rp.obj_id = mc.rel_logical_id")

	for _, ddl := range []string{
		InformationSchemaTablesDDL,
		InformationSchemaColumnsDDL,
		InformationSchemaTablesV41DDL,
		InformationSchemaColumnsV41DDL,
	} {
		assertInformationSchemaInitSQLParses(t, ddl)
	}
}

func TestInformationSchemaColumnsDDL_HidesInternalColumns(t *testing.T) {
	assert.Contains(t, InformationSchemaColumnsDDL, "mc.att_is_hidden = 0")
	assert.Contains(t, InformationSchemaColumnsDDL, "not startswith(mc.att_relname, '"+catalog.IndexTableNamePrefix+"')")
	assert.Contains(t, InformationSchemaColumnsDDL, "mk.key_priority = 3 then 'PRI'")
	assert.Contains(t, InformationSchemaColumnsDDL, "when mk.key_priority = 2 then 'UNI'")
	assert.Contains(t, InformationSchemaColumnsDDL, "when mk.key_priority = 1 then 'MUL'")
	assert.Contains(t, InformationSchemaColumnsDDL, "mc.key_priority = 3 then 'PRI'")
	assert.Contains(t, InformationSchemaColumnsDDL, "when mc.key_priority = 2 then 'UNI'")
	assert.Contains(t, InformationSchemaColumnsDDL, "when mc.key_priority = 1 then 'MUL'")
	assert.Contains(t, InformationSchemaColumnsDDL, "ki.ordinal_position = 1")
	assert.Contains(t, InformationSchemaColumnsDDL, "ki.type = 'PRIMARY'")
	assert.Contains(t, InformationSchemaColumnsDDL, "kp.part_count = 1")
	assert.NotContains(t, InformationSchemaColumnsDDL, "mo_show_col_unique")

	statements, err := mysql.Parse(context.Background(), InformationSchemaColumnsDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaKeyColumnUsageDDL_ProjectsForeignKeyMappings(t *testing.T) {
	assert.True(t, strings.HasPrefix(InformationSchemaKeyColumnUsageDDL, "CREATE VIEW information_schema.KEY_COLUMN_USAGE AS"))
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, "FROM mo_catalog.mo_indexes idx")
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, "idx.type IN ('PRIMARY', 'UNIQUE')")
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, "CAST(coalesce(tbl.reldatabase, '') AS varchar(64)) AS CONSTRAINT_SCHEMA")
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, "CAST(NULL AS int unsigned) AS POSITION_IN_UNIQUE_CONSTRAINT")
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, "NOT startswith(tbl.relname, '"+catalog.IndexTableNamePrefix+"')")
	assert.Contains(t, InformationSchemaKeyColumnUsageDDL, "UNION ALL")
	for _, column := range []string{
		"CAST(fk.column_name AS varchar(64)) AS COLUMN_NAME",
		"CAST(fk.refer_db_name AS varchar(64)) AS REFERENCED_TABLE_SCHEMA",
		"CAST(fk.refer_table_name AS varchar(64)) AS REFERENCED_TABLE_NAME",
		"CAST(fk.refer_column_name AS varchar(64)) AS REFERENCED_COLUMN_NAME",
		"CAST(fk.constraint_id AS int unsigned) AS ORDINAL_POSITION",
	} {
		assert.Contains(t, InformationSchemaKeyColumnUsageDDL, column)
	}

	statements, err := mysql.Parse(context.Background(), InformationSchemaKeyColumnUsageDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaReferentialConstraintsDDL_UsesMySQLDefaultAction(t *testing.T) {
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL,
		"replace(fk.on_update, '_', ' ') AS UPDATE_RULE")
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL,
		"replace(fk.on_delete, '_', ' ') AS DELETE_RULE")
	assert.NotContains(t, InformationSchemaReferentialConstraintsDDL, "upper(fk.on_update)")
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL,
		"fk.referenced_index_name AS UNIQUE_CONSTRAINT_NAME")
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL,
		"referenced_index_name")
	assert.NotContains(t, InformationSchemaReferentialConstraintsDDL, "mo_catalog.mo_indexes")
	assert.NotContains(t, InformationSchemaReferentialConstraintsDDL, "group_concat")
	assert.NotContains(t, InformationSchemaReferentialConstraintsDDL, "min(idx.type)")
}

func TestInformationSchemaReferentialConstraintsDDL_Parses(t *testing.T) {
	statements, err := mysql.Parse(context.Background(), InformationSchemaReferentialConstraintsDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaCheckConstraintsDDL(t *testing.T) {
	assert.True(t, strings.HasPrefix(
		InformationSchemaCheckConstraintsDDL,
		"CREATE VIEW information_schema.CHECK_CONSTRAINTS AS"))
	assert.Contains(t, InformationSchemaCheckConstraintsDDL, "mo_check_constraints()")
	for _, column := range []string{
		"CONSTRAINT_CATALOG",
		"CONSTRAINT_SCHEMA",
		"CONSTRAINT_NAME",
		"CHECK_CLAUSE",
	} {
		assert.Contains(t, InformationSchemaCheckConstraintsDDL, column)
	}

	statements, err := mysql.Parse(context.Background(), InformationSchemaCheckConstraintsDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaCharacterSetsData(t *testing.T) {
	for _, expected := range []string{
		"('binary','binary','Binary pseudo charset',1)",
		"('utf8','utf8_general_ci','UTF-8 Unicode',4)",
		"('utf8mb4','utf8mb4_general_ci','UTF-8 Unicode',4)",
	} {
		assert.Contains(t, InformationSchemaCharacterSetsData, expected)
	}

	ddlIndex := -1
	dataIndex := -1
	for i, sql := range InitInformationSchemaSysTables {
		switch sql {
		case InformationSchemaCharacterSetsDDL:
			ddlIndex = i
		case InformationSchemaCharacterSetsData:
			dataIndex = i
		}
	}
	assert.GreaterOrEqual(t, ddlIndex, 0)
	assert.Equal(t, ddlIndex+1, dataIndex)
}

func TestInformationSchemaDefaultCollationsMatchCanonicalDefinitions(t *testing.T) {
	assert.Empty(t, DefaultCollationForCharset("unknown_charset"))
	for _, charset := range []string{"binary", "utf8", "utf8mb4"} {
		defaultCollation := DefaultCollationForCharset(charset)
		assert.NotEmpty(t, defaultCollation, "missing canonical default for %s", charset)
		assert.Contains(t, InformationSchemaCharacterSetsData,
			fmt.Sprintf("('%s','%s'", charset, defaultCollation))
	}
	assert.Contains(t, InformationSchemaSchemataDDL,
		"'"+DefaultCollationForCharset("utf8mb4")+"' AS DEFAULT_COLLATION_NAME")
	assert.Contains(t, InformationSchemaTablesDDL,
		"'"+DefaultCollationForCharset("utf8mb4")+"' AS TABLE_COLLATION")
	assert.Contains(t, InformationSchemaViewsDDL,
		"'"+DefaultCollationForCharset("utf8mb4")+"' AS `COLLATION_CONNECTION`")
}

func TestSupportedCollationDefinitionsHaveOneDefaultPerCharset(t *testing.T) {
	defaults := make(map[string]int)
	for _, definition := range SupportedCollationDefinitions {
		if definition.IsDefault == "YES" {
			defaults[definition.Charset]++
		}
	}
	for _, charset := range []string{"binary", "utf8", "utf8mb4"} {
		assert.Equal(t, 1, defaults[charset], "expected exactly one default collation for %s", charset)
	}
}

func TestInformationSchemaCollationsData(t *testing.T) {
	for _, collation := range SupportedCollationDefinitions {
		assert.Contains(t, InformationSchemaCollationsData,
			fmt.Sprintf("('%s', '%s', %d, '%s', '%s', %d, '%s')",
				collation.Name,
				collation.Charset,
				collation.ID,
				collation.IsDefault,
				collation.IsCompiled,
				collation.SortLen,
				collation.PadAttribute,
			))
	}

	ddlIndex := -1
	dataIndex := -1
	for i, sql := range InitInformationSchemaSysTables {
		switch sql {
		case InformationSchemaCollationsDDL:
			ddlIndex = i
		case InformationSchemaCollationsData:
			dataIndex = i
		}
	}
	assert.GreaterOrEqual(t, ddlIndex, 0)
	assert.Equal(t, ddlIndex+1, dataIndex)
}

func TestInformationSchemaCollationCharacterSetApplicabilityDDL(t *testing.T) {
	assert.Contains(t, InformationSchemaCollationCharacterSetApplicabilityDDL,
		"CREATE VIEW information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS")
	assert.Contains(t, InformationSchemaCollationCharacterSetApplicabilityDDL,
		"SELECT COLLATION_NAME, CHARACTER_SET_NAME")
	assert.Contains(t, InformationSchemaCollationCharacterSetApplicabilityDDL,
		"FROM information_schema.COLLATIONS")

	statements, err := mysql.Parse(context.Background(), InformationSchemaCollationCharacterSetApplicabilityDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}
