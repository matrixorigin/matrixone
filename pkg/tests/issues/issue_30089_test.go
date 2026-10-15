// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue30089TemporaryDeleteAvoidsPersistentLifecycle(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db0.Close()
		db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db1.Close()
		const database = "issue30089_temp_delete"
		execSQLRequire(t, ctx, db0, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, err := db0.ExecContext(cleanup, "drop database if exists "+database)
			require.NoError(t, err)
		}()
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("indexed_%t", indexed), func(t *testing.T) {
				conn, err := db0.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(query string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				exec("use " + database)
				table := fmt.Sprintf("t_%t", indexed)
				exec("create table " + table + "(id int primary key, v int)")
				exec("insert into " + table + " values(99,99)")
				definition := "(id int primary key, v int"
				if indexed {
					definition += ", unique key uk(v)"
				}
				exec("create temporary table " + table + definition + ")")
				defer func() {
					cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
					defer stop()
					_, _ = conn.ExecContext(cleanup, "rollback")
					_, err := conn.ExecContext(cleanup, "drop temporary table if exists "+table)
					require.NoError(t, err)
				}()
				exec("insert into " + table + " values(2,20)")
				exec("begin")
				exec("update " + table + " set id=4, v=40")

				// A real shared owner on the other CN is a deterministic barrier:
				// the old physical TRUNCATE rewrite tries to promote to G X and
				// fails because UPDATE has already retained table locks.
				holder, err := db1.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer holder.Rollback()
				var feature string
				require.NoError(t, holder.QueryRowContext(ctx,
					"select feature_code from mo_catalog.mo_feature_registry where feature_code='SNAPSHOT' for share").Scan(&feature))
				require.Equal(t, "SNAPSHOT", feature)
				deleteCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				_, err = conn.ExecContext(deleteCtx, "delete from "+table)
				stop()
				require.NoError(t, err)
				exec("insert into " + table + " values(5,40)")
				exec("rollback")
				var id, value int
				require.NoError(t, conn.QueryRowContext(ctx, "select id,v from "+table).Scan(&id, &value))
				require.Equal(t, 2, id)
				require.Equal(t, 20, value)
				// Committed DELETE must remove the unique index entry too.
				exec("delete from " + table)
				exec("insert into " + table + " values(3,20)")
				require.NoError(t, conn.QueryRowContext(ctx, "select id from "+table+" where v=20").Scan(&id))
				require.Equal(t, 3, id)
				require.NoError(t, holder.Rollback())
				// The same-name permanent relation is not the delete target.
				require.NoError(t, db1.QueryRowContext(ctx, "select id from "+database+"."+table).Scan(&id))
				require.Equal(t, 99, id)
			})
		}
	})
}
