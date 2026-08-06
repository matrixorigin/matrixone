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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

func TestInformationSchemaCharacterSetsData(t *testing.T) {
	for _, expected := range []string{
		"('binary','binary','Binary pseudo charset',1)",
		"('utf8','utf8_bin','UTF-8 Unicode',4)",
		"('utf8mb4','utf8mb4_bin','UTF-8 Unicode',4)",
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
