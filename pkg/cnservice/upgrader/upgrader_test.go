// Copyright 2021 - 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package upgrader

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/stretchr/testify/assert"
)

// Column definitions
var (
	stmtIDCol  = table.UuidStringColumn("statement_id", "statement uniq id")
	txnIDCol   = table.UuidStringColumn("transaction_id", "txn uniq id")
	sesIDCol   = table.UuidStringColumn("session_id", "session uniq id")
	accountCol = table.StringColumn("account", "account name")
	roleIdCol  = table.Int64Column("role_id", "role id")
	userCol    = table.StringColumn("user", "user name")
)

func TestGenerateDiff(t *testing.T) {
	tests := []struct {
		name           string
		currentSchema  *table.Table
		expectedSchema *table.Table
		expectedDiff   table.SchemaDiff
	}{
		{
			name: "No differences",
			currentSchema: &table.Table{
				Database: "testdb",
				Table:    "testtable",
				Columns:  []table.Column{stmtIDCol, txnIDCol},
			},
			expectedSchema: &table.Table{
				Database: "testdb",
				Table:    "testtable",
				Columns:  []table.Column{stmtIDCol, txnIDCol},
			},
			expectedDiff: table.SchemaDiff{},
		},
		{
			name: "Column added",
			currentSchema: &table.Table{
				Database: "testdb",
				Table:    "testtable",
				Columns:  []table.Column{stmtIDCol, txnIDCol},
			},
			expectedSchema: &table.Table{
				Database: "testdb",
				Table:    "testtable",
				Columns:  []table.Column{stmtIDCol, txnIDCol, sesIDCol},
			},
			expectedDiff: table.SchemaDiff{
				AddedColumns: []table.Column{sesIDCol},
				TableName:    "testtable",
				DatabaseName: "testdb",
			},
		},
		// Add more test cases here as needed
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &Upgrader{}
			diff, err := u.GenerateDiff(tt.currentSchema, tt.expectedSchema)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedDiff, diff)
		})
	}
}

func TestGenerateUpgradeSQL(t *testing.T) {
	upgrader := &Upgrader{}

	testCases := []struct {
		name           string
		currentSchema  *table.Table
		expectedSchema *table.Table
		expectedDiff   table.SchemaDiff
		expectedSQL    string
	}{
		{
			name: "Column added",
			currentSchema: &table.Table{
				Database: "testdb",
				Table:    "testtable",
				Columns:  []table.Column{stmtIDCol, txnIDCol},
			},
			expectedSchema: &table.Table{
				Database: "testdb",
				Table:    "testtable",
				Columns:  []table.Column{stmtIDCol, txnIDCol, sesIDCol},
			},
			expectedDiff: table.SchemaDiff{
				AddedColumns: []table.Column{sesIDCol},
				TableName:    "testtable",
				DatabaseName: "testdb",
			},
			expectedSQL: "ALTER TABLE `testdb`.`testtable` ADD COLUMN `session_id` VARCHAR(36)",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			// Generate the SchemaDiff for current and expected schemas
			diff, err := upgrader.GenerateDiff(tt.currentSchema, tt.expectedSchema)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedDiff, diff)

			// Generate the upgrade SQL
			sql, err := upgrader.GenerateUpgradeSQL(diff)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSQL, sql)
		})
	}
}
