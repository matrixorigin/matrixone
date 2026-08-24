// Copyright 2026 Matrix Origin
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

// Package joinspill contains SQL-protocol helpers shared by join-spill issue
// regressions, including isolated-cluster regressions.
package joinspill

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func ExecSQL(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}

func QueryText(t *testing.T, ctx context.Context, conn *sql.Conn, query string) string {
	t.Helper()
	text, err := testutils.QueryText(ctx, conn, query)
	require.NoErrorf(t, err, "query failed: %s", query)
	return text
}

func PatchStats(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	dbName string,
	tableName string,
	tableCount int64,
) {
	t.Helper()
	PatchStatsWithNDV(t, ctx, conn, dbName, tableName, tableCount, tableCount)
}

func PatchStatsWithNDV(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	dbName string,
	tableName string,
	tableCount int64,
	ndv int64,
) {
	t.Helper()
	stats := fmt.Sprintf(`{
		"table_cnt": %d,
		"block_number": 2048,
		"accurate_object_number": 128,
		"approx_object_number": 128,
		"ndv_map": {"k": %d},
		"min_val_map": {"k": 1},
		"max_val_map": {"k": 20000000},
		"shuffle_range_map": {
			"k": {
				"overlap": 0.1,
				"uniform": 1,
				"result": [1, 5000000, 10000000, 15000000, 20000000]
			}
		}
	}`, tableCount, ndv)
	var patched float64
	err := conn.QueryRowContext(
		ctx,
		"select table_cnt from table_stats(?, 'patch', ?) g",
		dbName+"."+tableName,
		stats,
	).Scan(&patched)
	require.NoError(t, err)
	require.Equal(t, float64(tableCount), patched)
}

// ResetOptimizerHintsOnCN restores the shared service-runtime value, not only
// one session variable. A fresh connection keeps cleanup independent of the
// connection and deadline exercised by the regression.
func ResetOptimizerHintsOnCN(t *testing.T, port int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for _, statement := range []string{
		"set role moadmin",
		`set session optimizer_hints = ""`,
	} {
		_, err = db.ExecContext(ctx, statement)
		require.NoErrorf(t, err, "exec failed: %s", statement)
	}
}
