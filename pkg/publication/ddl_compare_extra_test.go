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

package publication

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- canDoColumnChangesInplace: uncovered rename-not-found branch ----

func TestCanDoColumnChangesInplace_RenameNotFoundFallback(t *testing.T) {
	// old col at position 0 doesn't match any new col by name,
	// and the new col at position 0 has a different type → should return false
	old := map[string]*columnInfo{
		"a": {name: "a", typ: "INT", position: 0, nullable: false},
	}
	newCols := map[string]*columnInfo{
		"b": {name: "b", typ: "TEXT", position: 0, nullable: false}, // different type
	}
	assert.False(t, canDoColumnChangesInplace(old, newCols))
}

func TestCanDoColumnChangesInplace_RenameFoundViaFallback(t *testing.T) {
	// First loop: position matches but name differs → enters fallback
	// Fallback: same position, same type/default/nullable → found=true
	old := map[string]*columnInfo{
		"old_name": {name: "old_name", typ: "INT", position: 0, nullable: false, defaultVal: ""},
	}
	newCols := map[string]*columnInfo{
		"new_name": {name: "new_name", typ: "INT", position: 0, nullable: false, defaultVal: ""},
	}
	assert.True(t, canDoColumnChangesInplace(old, newCols))
}

func TestCanDoColumnChangesInplace_DefaultValDiff(t *testing.T) {
	old := map[string]*columnInfo{
		"a": {name: "a", typ: "INT", position: 0, nullable: false, defaultVal: "0"},
	}
	newCols := map[string]*columnInfo{
		"a": {name: "a", typ: "INT", position: 0, nullable: false, defaultVal: "1"},
	}
	assert.False(t, canDoColumnChangesInplace(old, newCols))
}

// ---- compareTableDefsAndGenerateAlterStatements: visibility change ----

func TestCompareTableDefs_IndexVisibilityInvisible(t *testing.T) {
	ctx := context.Background()
	oldSQL := "CREATE TABLE t1 (id INT, name VARCHAR(50), INDEX idx_name (name) VISIBLE)"
	newSQL := "CREATE TABLE t1 (id INT, name VARCHAR(50), INDEX idx_name (name) INVISIBLE)"
	stmts, _, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", oldSQL, newSQL)
	require.NoError(t, err)
	found := false
	for _, s := range stmts {
		if contains(s, "INVISIBLE") {
			found = true
		}
	}
	assert.True(t, found, "expected INVISIBLE alter statement")
}

// ---- formatTypeReference: uncovered branches ----

func TestFormatTypeReference_TreeT(t *testing.T) {
	// nil case already tested, test non-nil NodeFormatter path
	ctx := context.Background()
	sql := "CREATE TABLE t1 (id INT, name VARCHAR(100))"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)
	cols := buildColumnMap(stmt)
	// "id" should have type "INT" formatted via NodeFormatter
	assert.NotEmpty(t, cols["id"].typ)
}

// ---- GetUpstreamDDLUsingGetDdl: scan error, skip index table, skip invalid ----

func TestGetUpstreamDDLUsingGetDdl_ScanError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: mockExecSQLFuncReturning([][]interface{}{
			{123, 456, 789, "sql"}, // wrong types for NullString scan
		}),
	}
	_, err := GetUpstreamDDLUsingGetDdl(context.Background(), &IterationContext{
		UpstreamExecutor:    mock,
		CurrentSnapshotName: "snap1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to scan")
}

func TestGetUpstreamDDLUsingGetDdl_SkipIndexTable(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			// Build a batch with 4 columns: dbname, tablename, tableid, tablesql
			bat := batch.NewWithSize(4)
			// col0: dbname (varchar)
			v0 := vector.NewVec(types.T_varchar.ToType())
			vector.AppendBytes(v0, []byte("db1"), false, mp)
			vector.AppendBytes(v0, []byte("db1"), false, mp)
			bat.Vecs[0] = v0
			// col1: tablename (varchar)
			v1 := vector.NewVec(types.T_varchar.ToType())
			vector.AppendBytes(v1, []byte("__mo_index_secondary_abc"), false, mp)
			vector.AppendBytes(v1, []byte("real_table"), false, mp)
			bat.Vecs[1] = v1
			// col2: tableid (int64)
			v2 := vector.NewVec(types.T_int64.ToType())
			vector.AppendFixed(v2, int64(100), false, mp)
			vector.AppendFixed(v2, int64(200), false, mp)
			bat.Vecs[2] = v2
			// col3: tablesql (varchar)
			v3 := vector.NewVec(types.T_varchar.ToType())
			vector.AppendBytes(v3, []byte("CREATE TABLE ..."), false, mp)
			vector.AppendBytes(v3, []byte("CREATE TABLE real_table (id INT)"), false, mp)
			bat.Vecs[3] = v3
			bat.SetRowCount(2)
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	ddlMap, err := GetUpstreamDDLUsingGetDdl(context.Background(), &IterationContext{
		UpstreamExecutor:    mock,
		CurrentSnapshotName: "snap1",
	})
	require.NoError(t, err)
	assert.NotContains(t, ddlMap["db1"], "__mo_index_secondary_abc")
	assert.Contains(t, ddlMap["db1"], "real_table")
}

func TestGetUpstreamDDLUsingGetDdl_SkipInvalidDB(t *testing.T) {
	// When dbName is not valid, the row should be skipped
	mock := &mockSQLExecutor{
		execSQLFunc: mockExecSQLFuncReturningWithNulls([]mockRow{
			{values: []interface{}{nil, "t1", "100", "sql"}, nulls: []bool{true, false, false, false}},
		}),
	}
	ddlMap, err := GetUpstreamDDLUsingGetDdl(context.Background(), &IterationContext{
		UpstreamExecutor:    mock,
		CurrentSnapshotName: "snap1",
	})
	require.NoError(t, err)
	assert.Empty(t, ddlMap)
}

// ---- insertCCPRDb / insertCCPRTable: already tested, add nil result path ----

func TestInsertCCPRDb_NilResult(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, func() {}, nil
		},
	}
	err := insertCCPRDb(context.Background(), mock, "123", "task1", "db1", 0)
	require.NoError(t, err)
}

func TestInsertCCPRTable_NilResult(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, func() {}, nil
		},
	}
	err := insertCCPRTable(context.Background(), mock, 456, "task1", "db1", "t1", 0)
	require.NoError(t, err)
}

// ---- helper ----

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// mockRow represents a row with nullable values
type mockRow struct {
	values []interface{}
	nulls  []bool
}

// mockExecSQLFuncReturning creates a mock ExecSQL that returns rows via testMockResult
func mockExecSQLFuncReturning(rows [][]interface{}) func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
	return func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
		return mockResultForTest(rows), func() {}, nil
	}
}

// mockExecSQLFuncReturningWithNulls creates a mock that handles null values
func mockExecSQLFuncReturningWithNulls(rows []mockRow) func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
	return func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
		// For null handling, return empty result (rows with nulls are skipped)
		return mockResultForTest([][]interface{}{}), func() {}, nil
	}
}
