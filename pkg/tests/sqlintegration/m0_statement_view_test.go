//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// This exercises ordinary Compile capture, actual readers, later statement
// recapture, and rollback with a real contracted relation. It is local SQL
// evidence, not a multi-CN dispatch claim.
func TestM0StatementViewTransactionSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_statement_view"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "alter table "+table+" add column v int")
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, "update "+table+" set v=10 where c_id=1")
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "delete from "+table+" where c_id=2")
		require.NoError(t, err)
		var count, sum int64
		require.NoError(t, tx.QueryRowContext(ctx, "select count(*),sum(coalesce(v,0)) from "+table).Scan(&count, &sum))
		require.Equal(t, int64(2), count)
		require.Equal(t, int64(10), sum)
		// Every later statement takes a new prefix. Updating a table cannot reread
		// its own newly emitted rows and apply the increment twice.
		_, err = tx.ExecContext(ctx, "update "+table+" set v=coalesce(v,0)+1")
		require.NoError(t, err)
		require.NoError(t, tx.QueryRowContext(ctx, "select count(*),sum(v) from "+table).Scan(&count, &sum))
		require.Equal(t, int64(2), count)
		require.Equal(t, int64(12), sum)
		require.NoError(t, tx.Rollback())
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where v is null"))
	})
}
