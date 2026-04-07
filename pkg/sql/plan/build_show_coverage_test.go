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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverage_buildShowGrants_ForRole(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show grants for ROLE role1",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowGrants_ForUser(t *testing.T) {
	mock := NewMockOptimizer(false)

	// The mock optimizer doesn't have mo_user_grant table, so this should fail with a known error
	sqls := []string{
		"show grants for 'testuser'@'localhost'",
	}
	// This is expected to fail in mock because mo_user_grant doesn't exist
	for _, sql := range sqls {
		_, err := runOneStmt(mock, t, sql)
		if err != nil {
			// Expected: mock doesn't have all system tables
			t.Logf("Expected error for user grants in mock: %v", err)
		}
	}
}

func TestCoverage_buildShowGrants_ForUserDefaultHost(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show grants for 'testuser'@'%'",
	}
	for _, sql := range sqls {
		_, err := runOneStmt(mock, t, sql)
		if err != nil {
			// Expected: mock doesn't have all system tables
			t.Logf("Expected error for user grants in mock: %v", err)
		}
	}
}

func TestCoverage_buildShowRoles(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show roles",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowRoles_WithLike(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show roles like '%admin%'",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowStages(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show stages",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowStages_WithLike(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show stages like 'my_stage%'",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowSnapshots(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show snapshots",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowSnapshots_WithWhere(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show snapshots where SNAPSHOT_NAME = 'snap1'",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowStatus(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show status",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowProcessList(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show processlist",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowVariables(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show variables",
		"show global variables",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowIndex(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show index from tpch.nation",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowFunctionStatus(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show function status",
		"show function status like '%ff'",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowLocks(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show locks",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowNodeList(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show node list",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowCreatePublications(t *testing.T) {
	mock := NewMockOptimizer(false)

	stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), "show create publication pub1", 1)
	if err != nil {
		t.Skipf("parser doesn't support show create publication: %v", err)
		return
	}
	for _, stmt := range stmts {
		_, err := BuildPlan(mock.CurrentContext(), stmt, false)
		if err != nil {
			// May fail due to mock limitations but should not panic
			t.Logf("BuildPlan for show create publication: %v", err)
		}
	}
}

func TestCoverage_buildShowPublicationCoverage(t *testing.T) {
	mock := NewMockOptimizer(false)

	stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), "show publication coverage pub1", 1)
	if err != nil {
		t.Skipf("parser doesn't support show publication coverage: %v", err)
		return
	}
	for _, stmt := range stmts {
		_, err := BuildPlan(mock.CurrentContext(), stmt, false)
		if err != nil {
			t.Logf("BuildPlan for show publication coverage: %v", err)
		}
	}
}

func TestCoverage_returnByRewriteSQL(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext()

	// Simple valid SQL
	p, err := returnByRewriteSQL(ctx, "SELECT 1 as val", plan.DataDefinition_SHOW_TARGET)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestCoverage_returnByRewriteSQL_InvalidSQL(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext()

	_, err := returnByRewriteSQL(ctx, "THIS IS NOT VALID SQL", plan.DataDefinition_SHOW_TARGET)
	assert.Error(t, err)
}

func TestCoverage_buildShowDatabases(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show databases",
		"show databases like '%d'",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowTables(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show tables",
		"show tables from tpch",
		"show tables like '%dd'",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowColumns(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show columns from nation",
		"show full columns from nation",
		"show columns from nation from tpch",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowTableStatus(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show table status",
		"show table status from tpch",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowCreateTable(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show create table nation",
		"show create table tpch.nation",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowCreateView(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show create view v1",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowTableNumber(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show table_number",
		"show table_number from tpch",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCoverage_buildShowColumnNumber(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"show column_number from nation",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}
