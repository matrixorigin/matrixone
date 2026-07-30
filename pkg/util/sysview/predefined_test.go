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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestInformationSchemaKeyColumnUsageDDL_ProjectsForeignKeyMappings(t *testing.T) {
	assert.True(t, strings.HasPrefix(InformationSchemaKeyColumnUsageDDL, "CREATE VIEW information_schema.KEY_COLUMN_USAGE AS"))
	for _, column := range []string{
		"CAST(fk.column_name AS varchar(64)) AS COLUMN_NAME",
		"CAST(fk.refer_db_name AS varchar(64)) AS REFERENCED_TABLE_SCHEMA",
		"CAST(fk.refer_table_name AS varchar(64)) AS REFERENCED_TABLE_NAME",
		"CAST(fk.refer_column_name AS varchar(64)) AS REFERENCED_COLUMN_NAME",
		"CAST(CASE WHEN fk.constraint_id = 0 THEN 1 ELSE fk.constraint_id END AS int unsigned) AS ORDINAL_POSITION",
	} {
		assert.Contains(t, InformationSchemaKeyColumnUsageDDL, column)
	}
}

func TestInformationSchemaReferentialConstraintsDDL_UsesMySQLDefaultAction(t *testing.T) {
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL,
		"CASE WHEN upper(fk.on_update) = 'RESTRICT' THEN 'NO ACTION' ELSE fk.on_update END AS UPDATE_RULE")
	assert.Contains(t, InformationSchemaReferentialConstraintsDDL,
		"CASE WHEN upper(fk.on_delete) = 'RESTRICT' THEN 'NO ACTION' ELSE fk.on_delete END AS DELETE_RULE")
}
