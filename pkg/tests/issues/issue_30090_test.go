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
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/stretchr/testify/require"
)

func TestIssue30090ReplaceViewLifecycleOrder(t *testing.T) {
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
		t.Run("no_default_database", func(t *testing.T) {
			for _, statement := range []string{
				"create view issue30090_no_db as select 1 as id",
				"create or replace view issue30090_no_db as select 1 as id",
			} {
				_, err := db0.ExecContext(ctx, statement)
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.Equal(t, uint16(1046), mysqlErr.Number, statement)
			}
		})
		const database = "issue30090_replace_view"
		execSQLRequire(t, ctx, db0, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, err := db0.ExecContext(cleanup, "drop database if exists "+database)
			require.NoError(t, err)
		}()
		execSQLRequire(t, ctx, db0, "create table "+database+".t(id int primary key)")
		execSQLRequire(t, ctx, db0, "insert into "+database+".t values(1)")
		var registryID uint64
		require.NoError(t, db0.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&registryID))
		for _, cancelWait := range []bool{false, true} {
			name := "release"
			if cancelWait {
				name = "cancel"
			}
			t.Run(name, func(t *testing.T) {
				execSQLRequire(t, ctx, db0, "create or replace view "+database+".v as select id from "+database+".t")
				holder, err := db1.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer holder.Rollback()
				var feature string
				require.NoError(t, holder.QueryRowContext(ctx,
					"select feature_code from mo_catalog.mo_feature_registry where feature_code='SNAPSHOT' for update").Scan(&feature))
				queued := make(chan []byte, 1)
				restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, waiter []byte, _ [][]byte) {
					if id == registryID {
						select {
						case queued <- bytes.Clone(waiter):
						default:
						}
					}
				})
				defer restore()
				operationCtx, stop := context.WithTimeout(ctx, 15*time.Second)
				done := make(chan error, 1)
				finished := false
				defer func() {
					stop()
					_ = holder.Rollback()
					if !finished {
						select {
						case <-done:
						case <-ctx.Done():
							t.Error("CREATE OR REPLACE VIEW did not terminate")
						}
					}
				}()
				go func() {
					_, err := db0.ExecContext(operationCtx, "create or replace view "+database+".v as select id+1 as id from "+database+".t")
					done <- err
				}()
				var waiterID []byte
				select {
				case waiterID = <-queued:
				case err := <-done:
					finished = true
					t.Fatalf("fresh replacement must wait before acquiring D: %v", err)
				case <-operationCtx.Done():
					t.Fatal("replacement did not enqueue on G")
				}
				if cancelWait {
					stop()
				} else {
					require.NoError(t, holder.Commit())
				}
				select {
				case err = <-done:
					finished = true
				case <-ctx.Done():
					t.Fatal("replacement did not terminate")
				}
				if cancelWait {
					require.ErrorIs(t, err, context.Canceled)
					// Client cancellation can finish before the server rolls back.
					// Keep G held until the observed server transaction disappears,
					// so unlocking it cannot race an uncancelled replacement.
					drainCtx, drainCancel := context.WithTimeout(ctx, 5*time.Second)
					defer drainCancel()
					services := issue27487LockServices(cluster)
					require.Eventually(t, func() bool {
						for _, service := range services {
							present, _, err := service.GetWaitingList(drainCtx, waiterID)
							if err != nil || present {
								return false
							}
						}
						return true
					}, 5*time.Second, 20*time.Millisecond, "cancelled server transaction must release admission locks")
					require.NoError(t, holder.Rollback())
				} else {
					require.NoError(t, err)
				}
				var id int
				require.NoError(t, db0.QueryRowContext(ctx, "select id from "+database+".v").Scan(&id))
				if cancelWait {
					require.Equal(t, 1, id, "cancelled replacement must preserve the old view")
				} else {
					require.Equal(t, 2, id)
				}
				// Following replacement is usable after either terminal path.
				execSQLRequire(t, ctx, db0, "create or replace view "+database+".v as select id+2 as id from "+database+".t")
				require.NoError(t, db0.QueryRowContext(ctx, "select id from "+database+".v").Scan(&id))
				require.Equal(t, 3, id)
			})
		}
		t.Run("prepared_reader_rebinds_after_replace", func(t *testing.T) {
			reader, err := db0.Conn(ctx)
			require.NoError(t, err)
			defer reader.Close()
			stmt, err := reader.PrepareContext(ctx, "select id from "+database+".v where id>=?")
			require.NoError(t, err)
			defer stmt.Close()
			var id int
			require.NoError(t, stmt.QueryRowContext(ctx, 0).Scan(&id))
			require.Equal(t, 3, id)
			execSQLRequire(t, ctx, db1, "create or replace view "+database+".v as select id+4 as id from "+database+".t")
			// The replacement committed on CN1. CN0's read snapshot is bounded by
			// its own applied logtail watermark under sacrificing-freshness
			// consistency, so the reader can still legally observe the
			// pre-replacement view. Establish CN1 -> CN0 visibility through the
			// existing SYNCCOMMIT fence before asserting prepared-plan
			// revalidation, as #30334 does at other cross-CN boundaries.
			execSQLRequire(t, ctx, db0, "select mo_ctl('cn', 'SYNCCOMMIT', '')")
			require.NoError(t, stmt.QueryRowContext(ctx, 1).Scan(&id))
			require.Equal(t, 5, id)
		})
		t.Run("retained_data_locks_still_fast_fail", func(t *testing.T) {
			writer, err := db0.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer writer.Rollback()
			_, err = writer.ExecContext(ctx, "update "+database+".t set id=7")
			require.NoError(t, err)
			holder, err := db1.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer holder.Rollback()
			var feature string
			require.NoError(t, holder.QueryRowContext(ctx,
				"select feature_code from mo_catalog.mo_feature_registry where feature_code='SNAPSHOT' for update").Scan(&feature))
			operationCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			defer stop()
			_, err = writer.ExecContext(operationCtx, "create or replace view "+database+".v as select id from "+database+".t")
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr)
			require.Equal(t, uint16(20706), mysqlErr.Number)
		})
	})
}
