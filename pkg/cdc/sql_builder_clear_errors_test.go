package cdc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestClearTaskTableErrorsSQL_Generation verifies SQL generation
func TestClearTaskTableErrorsSQL_Generation(t *testing.T) {
	sql := CDCSQLBuilder.ClearTaskTableErrorsSQL(1, "task123")

	expected := "UPDATE `mo_catalog`.`mo_cdc_watermark` SET err_msg = '' WHERE account_id = 1 AND task_id = 'task123' AND err_msg != ''"
	assert.Equal(t, expected, sql)
}

// TestClearTaskTableErrorsSQL_SQLInjectionProtection verifies SQL injection protection
func TestClearTaskTableErrorsSQL_SQLInjectionProtection(t *testing.T) {
	// Test SQL injection attempt
	sql := CDCSQLBuilder.ClearTaskTableErrorsSQL(2, "task'; DROP TABLE users; --")

	// Should escape the single quote
	expected := "UPDATE `mo_catalog`.`mo_cdc_watermark` SET err_msg = '' WHERE account_id = 2 AND task_id = 'task''; DROP TABLE users; --' AND err_msg != ''"
	assert.Equal(t, expected, sql)

	// Verify components
	assert.Contains(t, sql, "UPDATE `mo_catalog`.`mo_cdc_watermark`")
	assert.Contains(t, sql, "SET err_msg = ''")
	assert.Contains(t, sql, "task''; DROP TABLE users; --") // Escaped quote
}

// TestClearTaskTableErrorsSQL_Components verifies SQL components
func TestClearTaskTableErrorsSQL_Components(t *testing.T) {
	sql := CDCSQLBuilder.ClearTaskTableErrorsSQL(100, "my-task")

	// Verify all required components
	assert.Contains(t, sql, "UPDATE `mo_catalog`.`mo_cdc_watermark`", "Should update watermark table")
	assert.Contains(t, sql, "SET err_msg = ''", "Should clear err_msg column")
	assert.Contains(t, sql, "WHERE account_id = 100", "Should filter by account_id")
	assert.Contains(t, sql, "task_id = 'my-task'", "Should filter by task_id")
	assert.Contains(t, sql, "err_msg != ''", "Should only update non-empty errors")

	// Should NOT update other tasks or accounts
	assert.NotContains(t, sql, "account_id != 100")
	assert.NotContains(t, sql, "other-task")
}

// TestClearTaskTableErrorsSQL_TaskIsolation verifies task isolation
func TestClearTaskTableErrorsSQL_TaskIsolation(t *testing.T) {
	sql1 := CDCSQLBuilder.ClearTaskTableErrorsSQL(1, "task-A")
	sql2 := CDCSQLBuilder.ClearTaskTableErrorsSQL(1, "task-B")
	sql3 := CDCSQLBuilder.ClearTaskTableErrorsSQL(2, "task-A")

	// Different tasks for same account
	assert.Contains(t, sql1, "task_id = 'task-A'")
	assert.Contains(t, sql2, "task_id = 'task-B'")
	assert.NotContains(t, sql1, "task-B")
	assert.NotContains(t, sql2, "task-A")

	// Different accounts for same task name
	assert.Contains(t, sql1, "account_id = 1")
	assert.Contains(t, sql3, "account_id = 2")
}
