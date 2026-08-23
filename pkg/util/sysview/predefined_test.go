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

func TestInformationSchemaStatisticsDDL_ContainsIdxAlgo(t *testing.T) {
	assert.True(t, strings.Contains(InformationSchemaStatisticsDDL, "`idx`.`algo` AS `INDEX_TYPE`"))
	assert.False(t, strings.Contains(InformationSchemaStatisticsDDL, "NULL AS `INDEX_TYPE`"))
}

func TestInformationSchemaTableConstraintsDDL_ContainsCheckConstraints(t *testing.T) {
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "UNION ALL")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "FROM mo_check_constraints() cc")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, "cc.table_name AS TABLE_NAME")
	assert.Contains(t, InformationSchemaTableConstraintsDDL, catalog.NonTemporaryTableSQLPredicate("tbl"))
	statements, err := mysql.Parse(context.Background(), InformationSchemaTableConstraintsDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaTableConstraintsLegacyDDL_DoesNotUseCheckConstraints(t *testing.T) {
	assert.NotContains(t, InformationSchemaTableConstraintsLegacyDDL, "UNION ALL")
	assert.NotContains(t, InformationSchemaTableConstraintsLegacyDDL, "mo_check_constraints()")
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
	assert.Contains(t, legacy, InformationSchemaTableConstraintsLegacyDDL)
	assert.Contains(t, legacy, InformationSchemaCollationCharacterSetApplicabilityDDL)
	for _, sql := range legacy {
		assert.NotContains(t, sql, "mo_check_constraints()")
	}

	latest := InitInformationSchemaSysTablesForProtocol(defines.MORPCVersion16)
	assert.Equal(t, InitInformationSchemaSysTables, latest)
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

func TestInformationSchemaColumnsDDL_HidesInternalColumns(t *testing.T) {
	assert.Contains(t, InformationSchemaColumnsDDL, "mc.att_is_hidden = 0")

	statements, err := mysql.Parse(context.Background(), InformationSchemaColumnsDDL, 1)
	assert.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

func TestInformationSchemaKeyColumnUsageDDL_ProjectsForeignKeyMappings(t *testing.T) {
	assert.True(t, strings.HasPrefix(InformationSchemaKeyColumnUsageDDL, "CREATE VIEW information_schema.KEY_COLUMN_USAGE AS"))
	for _, column := range []string{
		"CAST(fk.column_name AS varchar(64)) AS COLUMN_NAME",
		"CAST(fk.refer_db_name AS varchar(64)) AS REFERENCED_TABLE_SCHEMA",
		"CAST(fk.refer_table_name AS varchar(64)) AS REFERENCED_TABLE_NAME",
		"CAST(fk.refer_column_name AS varchar(64)) AS REFERENCED_COLUMN_NAME",
		"CAST(fk.constraint_id AS int unsigned) AS ORDINAL_POSITION",
	} {
		assert.Contains(t, InformationSchemaKeyColumnUsageDDL, column)
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
