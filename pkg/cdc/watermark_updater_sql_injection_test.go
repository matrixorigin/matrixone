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

package cdc

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

// TestSQLInjectionPrevention verifies that malicious SQL injection attempts are properly escaped
func TestSQLInjectionPrevention(t *testing.T) {
	updater := NewCDCWatermarkUpdater(
		"test-sql-injection",
		nil, // InternalExecutor not needed for SQL construction test
	)

	t.Run("SingleQuoteInTaskId", func(t *testing.T) {
		// Malicious task_id with single quote
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "task'; DROP TABLE users; --",
			DBName:    "test_db",
			TableName: "test_table",
		}
		watermark := types.BuildTS(100, 0)

		keys := map[WatermarkKey]types.TS{key: watermark}
		sql := updater.constructBatchUpdateWMSQL(keys)

		t.Logf("Generated SQL: %s", sql)

		// After fix: single quote should be escaped as double single quote
		assert.Contains(t, sql, "'task''; DROP TABLE users; --'",
			"SQL should escape single quotes as double single quotes")
		// The malicious SQL is now safely inside a string literal
	})

	t.Run("SingleQuoteInDBName", func(t *testing.T) {
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "normal_task",
			DBName:    "db' OR '1'='1",
			TableName: "test_table",
		}
		watermark := types.BuildTS(100, 0)

		keys := map[WatermarkKey]types.TS{key: watermark}
		sql := updater.constructBatchUpdateWMSQL(keys)

		t.Logf("Generated SQL: %s", sql)

		// After fix: quotes are escaped, preventing OR injection
		assert.Contains(t, sql, "'db'' OR ''1''=''1'",
			"SQL should escape all single quotes in db_name")
	})

	t.Run("SQLKeywordsInTableName", func(t *testing.T) {
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "normal_task",
			DBName:    "test_db",
			TableName: "table'); DELETE FROM mo_cdc_watermark WHERE ('1'='1",
		}
		watermark := types.BuildTS(100, 0)

		keys := map[WatermarkKey]types.TS{key: watermark}
		sql := updater.constructBatchUpdateWMSQL(keys)

		t.Logf("Generated SQL: %s", sql)

		// After fix: DELETE statement is escaped and treated as string
		assert.Contains(t, sql, "DELETE FROM mo_cdc_watermark",
			"Malicious DELETE is present but safely escaped inside string")
		assert.Contains(t, sql, "''1''=''1",
			"Quotes in malicious input are properly escaped")
	})

	t.Run("BackslashAndQuoteInTaskId", func(t *testing.T) {
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "task\\' OR ''='",
			DBName:    "test_db",
			TableName: "test_table",
		}
		watermark := types.BuildTS(100, 0)

		keys := map[WatermarkKey]types.TS{key: watermark}
		sql := updater.constructBatchUpdateWMSQL(keys)

		t.Logf("Generated SQL: %s", sql)

		// After fix: backslash and quotes are both escaped
		assert.Contains(t, sql, "\\\\", "Backslashes should be escaped")
		assert.Contains(t, sql, "''", "Single quotes should be escaped")
	})

	t.Run("MaliciousErrMsg", func(t *testing.T) {
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "task",
			DBName:    "db",
			TableName: "table",
		}
		job := NewUpdateWMErrMsgJob(context.Background(), &key, "error'; UPDATE mo_cdc_watermark SET watermark='999999'; --")

		sql := updater.constructBatchUpdateWMErrMsgSQL([]*UpdaterJob{job})

		t.Logf("Generated SQL: %s", sql)

		// After fix: error message is escaped
		assert.Contains(t, sql, "UPDATE mo_cdc_watermark SET watermark",
			"Malicious UPDATE is present but safely escaped")
		assert.Contains(t, sql, "'error''; UPDATE",
			"Error message quote is properly escaped")
	})

	t.Run("ReadWMSQLInjection", func(t *testing.T) {
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "task' OR 1=1 --",
			DBName:    "test_db",
			TableName: "test_table",
		}

		keys := map[WatermarkKey]WatermarkResult{
			key: {Watermark: types.TS{}, Ok: false},
		}
		sql := updater.constructReadWMSQL(keys)

		t.Logf("Generated SQL: %s", sql)

		// After fix: OR clause is escaped and won't bypass WHERE
		assert.Contains(t, sql, "'task'' OR 1=1 --'",
			"Malicious OR clause should be escaped inside string literal")
	})

	t.Run("AddWMSQLInjection", func(t *testing.T) {
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "task'); DROP TABLE mo_cdc_watermark; --",
			DBName:    "test_db",
			TableName: "test_table",
		}
		watermark := types.BuildTS(100, 0)
		job := NewGetOrAddCommittedWMJob(context.Background(), &key, &watermark)

		sql := updater.constructAddWMSQL([]*UpdaterJob{job})

		t.Logf("Generated SQL: %s", sql)

		// After fix: DROP TABLE is escaped
		assert.Contains(t, sql, "DROP TABLE mo_cdc_watermark",
			"Malicious DROP is present but safely escaped")
		assert.Contains(t, sql, "'task''); DROP",
			"Quote before DROP is properly escaped")
	})
}

// TestSQLEscaping tests that SQL escaping properly handles special characters
func TestSQLEscaping(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "SingleQuote",
			input:    "test'value",
			expected: "test''value",
		},
		{
			name:     "DoubleQuote",
			input:    `test"value`,
			expected: `test"value`, // Double quotes don't need escaping in single-quoted strings
		},
		{
			name:     "Backslash",
			input:    `test\value`,
			expected: `test\\value`,
		},
		{
			name:     "BackslashAndQuote",
			input:    `test\'value`,
			expected: `test\\''value`,
		},
		{
			name:     "MultipleQuotes",
			input:    "it's a test's value",
			expected: "it''s a test''s value",
		},
		{
			name:     "SQLComment",
			input:    "test-- comment",
			expected: "test-- comment", // Comments are safe inside quoted strings
		},
		{
			name:     "Semicolon",
			input:    "test;DROP TABLE",
			expected: "test;DROP TABLE", // Semicolons are safe inside quoted strings
		},
		{
			name:     "Newline",
			input:    "test\nvalue",
			expected: "test\nvalue", // Newlines are safe
		},
		{
			name:     "EmptyString",
			input:    "",
			expected: "",
		},
		{
			name:     "OnlyQuotes",
			input:    "'''",
			expected: "''''''",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := escapeSQLString(tc.input)
			assert.Equal(t, tc.expected, result,
				"escapeSQLString should properly escape: %s", tc.input)
		})
	}
}

// TestSQLInjectionFixed verifies that SQL injection is prevented after the fix
func TestSQLInjectionFixed(t *testing.T) {
	updater := NewCDCWatermarkUpdater(
		"test-sql-injection-fixed",
		nil,
	)

	t.Run("MaliciousInputsAreEscaped", func(t *testing.T) {
		maliciousInputs := []WatermarkKey{
			{
				AccountId: 1,
				TaskId:    "task'; DROP TABLE users; --",
				DBName:    "test_db",
				TableName: "test_table",
			},
			{
				AccountId: 1,
				TaskId:    "normal_task",
				DBName:    "db' OR '1'='1",
				TableName: "test_table",
			},
			{
				AccountId: 1,
				TaskId:    "task",
				DBName:    "db",
				TableName: "table'); DELETE FROM mo_cdc_watermark; --",
			},
		}

		for _, key := range maliciousInputs {
			watermark := types.BuildTS(100, 0)
			keys := map[WatermarkKey]types.TS{key: watermark}
			sql := updater.constructBatchUpdateWMSQL(keys)

			t.Logf("Input TaskId: %s", key.TaskId)
			t.Logf("Input DBName: %s", key.DBName)
			t.Logf("Input TableName: %s", key.TableName)
			t.Logf("Generated SQL: %s", sql)

			// After fix: single quotes should be escaped as double single quotes
			// The SQL should NOT contain literal DROP, DELETE, or OR clauses outside of values

			// Check that single quotes are properly escaped (should appear as '')
			if strings.Contains(key.TaskId, "'") ||
				strings.Contains(key.DBName, "'") ||
				strings.Contains(key.TableName, "'") {

				// Count single quotes - after escaping, they should appear in pairs
				// This is a heuristic check
				singleQuoteCount := strings.Count(sql, "'")

				// We should have an even number of single quotes
				// (opening and closing quotes, plus doubled quotes for escaping)
				assert.True(t, singleQuoteCount%2 == 0,
					"After escaping, single quotes should appear in pairs")
			}

			// More importantly: verify the SQL is syntactically safe
			// by checking it doesn't contain unescaped malicious patterns
			// Note: This is a best-effort check; real validation needs SQL parsing

			// The SQL should still be a valid INSERT/UPDATE structure
			assert.True(t,
				strings.Contains(sql, "INSERT") || strings.Contains(sql, "UPDATE"),
				"SQL should be INSERT or UPDATE statement")
		}
	})

	t.Run("ReadWMWithMaliciousInput", func(t *testing.T) {
		key := WatermarkKey{
			AccountId: 1,
			TaskId:    "task' OR 1=1 --",
			DBName:    "test_db",
			TableName: "test_table",
		}

		keys := map[WatermarkKey]WatermarkResult{
			key: {Watermark: types.TS{}, Ok: false},
		}
		sql := updater.constructReadWMSQL(keys)

		t.Logf("Generated SQL: %s", sql)

		// After fix: the SQL should properly escape the single quote
		// so it doesn't create an OR condition outside of the string value

		// The structure should be: WHERE (account_id = X AND task_id = 'escaped_value' ...)
		assert.Contains(t, sql, "WHERE", "SQL should have WHERE clause")
		assert.Contains(t, sql, "account_id", "SQL should check account_id")
	})
}

// TestSQLEscapingEdgeCases tests edge cases for SQL escaping
func TestSQLEscapingEdgeCases(t *testing.T) {
	testCases := []struct {
		name  string
		key   WatermarkKey
		check func(t *testing.T, sql string)
	}{
		{
			name: "UnicodeCharacters",
			key: WatermarkKey{
				AccountId: 1,
				TaskId:    "task_中文_测试",
				DBName:    "数据库",
				TableName: "表名",
			},
			check: func(t *testing.T, sql string) {
				assert.Contains(t, sql, "task_中文_测试", "Unicode should be preserved")
				assert.Contains(t, sql, "数据库", "Unicode should be preserved")
			},
		},
		{
			name: "VeryLongString",
			key: WatermarkKey{
				AccountId: 1,
				TaskId:    strings.Repeat("a", 1000),
				DBName:    "test_db",
				TableName: "test_table",
			},
			check: func(t *testing.T, sql string) {
				assert.Contains(t, sql, strings.Repeat("a", 1000),
					"Long strings should be handled correctly")
			},
		},
		{
			name: "SpecialCharactersCombined",
			key: WatermarkKey{
				AccountId: 1,
				TaskId:    "task_!@#$%^&*()",
				DBName:    "db-test.prod",
				TableName: "table_123",
			},
			check: func(t *testing.T, sql string) {
				assert.Contains(t, sql, "task_!@#$%^&*()",
					"Special characters should be preserved")
			},
		},
	}

	updater := NewCDCWatermarkUpdater("test-edge-cases", nil)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			watermark := types.BuildTS(100, 0)
			keys := map[WatermarkKey]types.TS{tc.key: watermark}
			sql := updater.constructBatchUpdateWMSQL(keys)

			t.Logf("Generated SQL: %s", sql)
			tc.check(t, sql)
		})
	}
}
