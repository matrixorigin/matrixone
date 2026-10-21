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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInformationSchemaViewsUpgradeIsIdempotent(t *testing.T) {
	// Reuse the SQL fixture: one real catalog view, two upgrade transactions,
	// and no timer or polling are needed to prove the durable repair marker.
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		upgrade := func() {
			t.Helper()
			require.NoError(t, exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				return v4_0_7.UpgradeInformationSchemaViewsAfterProtocolCheck(txn, 0)
			}, executor.Options{}.WithWaitCommittedLogApplied()))
		}
		viewID := func() uint64 {
			t.Helper()
			res, err := exec.Exec(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='information_schema' and lower(relname)='views' and relkind='v'", executor.Options{})
			require.NoError(t, err)
			defer res.Close()
			var ids []uint64
			res.ReadRows(func(_ int, cols []*vector.Vector) bool {
				ids = append(ids, executor.GetFixedRows[uint64](cols[0])...)
				return true
			})
			require.Len(t, ids, 1)
			return ids[0]
		}
		upgrade()
		installedID := viewID()
		upgrade()
		require.Equal(t, installedID, viewID(), "an installed VIEWS definition must not be dropped and recreated")

		t.Run("aggregate_metadata_replay", func(t *testing.T) {
			// Exercise the installed public catalog through MySQL on this same
			// CN: three rows, two views, and no extra cluster or readiness poll.
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			const schema = "view_aggregate_metadata_replay"
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
			execSQL := func(statement string) {
				t.Helper()
				_, err := conn.ExecContext(ctx, statement)
				require.NoError(t, err, statement)
			}
			execSQL("drop database if exists " + schema)
			for round := 0; round < 2; round++ {
				t.Logf("aggregate metadata replay round %d", round+1)
				execSQL("create database " + schema)
				execSQL("use " + schema)
				execSQL("create table t(a int, b int)")
				execSQL("insert into t values (1, 10), (1, 20), (2, 30)")
				execSQL("create view agg_v as select a, count(*) cnt from t group by a")
				var definition, updatable string
				require.NoError(t, conn.QueryRowContext(ctx,
					"select view_definition, is_updatable from information_schema.views where table_schema=? and table_name='agg_v'", schema).Scan(&definition, &updatable))
				require.Equal(t, "select `t`.`a`, count(*) as `cnt` from `t` group by `a`", definition)
				require.Equal(t, "NO", updatable)
				execSQL("create view replay_v as " + definition)
				for _, view := range []string{"agg_v", "replay_v"} {
					func() {
						rows, err := conn.QueryContext(ctx, "select a, cnt from "+view+" order by a")
						require.NoError(t, err)
						defer rows.Close()
						var got [][2]int64
						for rows.Next() {
							var row [2]int64
							require.NoError(t, rows.Scan(&row[0], &row[1]))
							got = append(got, row)
						}
						require.NoError(t, rows.Err())
						require.Equal(t, [][2]int64{{1, 2}, {2, 1}}, got, view)
					}()
				}
				_, err = conn.ExecContext(ctx, "update agg_v set cnt=1")
				require.ErrorContains(t, err, "cannot insert/update/delete from view")
				execSQL("drop database " + schema)
				var remaining int
				require.NoError(t, conn.QueryRowContext(ctx,
					"select count(*) from mo_catalog.mo_tables where reldatabase=? and account_id=0", schema).Scan(&remaining))
				require.Zero(t, remaining, "verify cleanup before reusing the same CN, round %d", round+1)
			}
		})
	})
}

func Test_TxnExecutorExec(t *testing.T) {
	// Reuse the single-CN fixture used by TestPreparedParams; this case only reads the catalog.
	runSQLIntegration(t, func(c embed.Cluster) {
		svc, err := c.GetCNService(0)
		require.NoError(t, err)

		exec := testutils.GetSQLExecutor(svc)
		require.NotNil(t, exec)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
		defer cancel()

		err = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			result, queryErr := txn.Exec("select count(*) from mo_catalog.mo_tables", executor.StatementOption{}.WithAccountID(1).WithUserID(2).WithRoleID(2))
			require.NoError(t, queryErr)
			result.Close()
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
	})
}

func TestCatalogUpgradeUpdatePlanRebuild(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		require.NotNil(t, exec)

		db := strings.ToLower(testutils.GetDatabaseName(t))
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		testutils.CreateTestDatabase(t, db, cn)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err = exec.Exec(ctx, "create table t (v int unsigned)", executor.Options{}.WithDatabase(db))
		require.NoError(t, err)

		predicate := fmt.Sprintf("att_database = '%s' AND att_relname = 't' AND attname = 'v'", db)
		readCount := func(condition string) int {
			result, queryErr := exec.Exec(ctx, "SELECT count(*) FROM mo_catalog.mo_columns WHERE "+predicate+condition, executor.Options{})
			require.NoError(t, queryErr)
			defer result.Close()
			return testutils.ReadCount(result)
		}
		require.Equal(t, 1, readCount(""))
		require.Equal(t, 1, readCount(" AND att_is_unsigned = 1"))

		// Simulate the stale metadata repaired by the upgrade, then verify the
		// forced rebuild updates a real row and the committed value is visible.
		setUnsigned := func(value int8) (executor.Result, error) {
			sql := fmt.Sprintf("UPDATE mo_catalog.mo_columns SET att_is_unsigned = %d WHERE %s", value, predicate)
			return exec.Exec(ctx, sql, executor.Options{}.WithForceRebuildPlan().WithStatementOption(
				executor.StatementOption{}.WithMoColumnsUpdate()))
		}
		result, err := setUnsigned(0)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result.AffectedRows)
		result.Close()
		require.Equal(t, 1, readCount(" AND att_is_unsigned = 0"))

		result, err = setUnsigned(1)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result.AffectedRows)
		result.Close()
		require.Equal(t, 1, readCount(" AND att_is_unsigned = 1"))

		sql := "UPDATE mo_catalog.mo_columns SET att_is_unsigned = 0 WHERE " + predicate
		_, err = exec.Exec(ctx, sql, executor.Options{}.WithForceRebuildPlan())
		require.ErrorContains(t, err, "direct DML on mo_catalog.mo_columns")
		require.Equal(t, 1, readCount(" AND att_is_unsigned = 1"))
	})
}

func TestPreparedParams(t *testing.T) {
	runSQLIntegration(t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn)
			require.NotNil(t, exec)

			db := testutils.GetDatabaseName(t)
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
			testutils.CreateTestDatabase(t, db, cn)

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			err = exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					txn.Use(db)

					_, err := txn.Exec(
						"create table t (id int primary key, name varchar(20))",
						executor.StatementOption{},
					)
					require.NoError(t, err)

					_, err = txn.Exec(
						"insert into t values (?, ?), (?, ?)",
						executor.StatementOption{}.WithParams([]string{"1", "test", "3", "test3"}),
					)
					require.NoError(t, err)

					res, err := txn.Exec(
						"select count(1) from t",
						executor.StatementOption{},
					)
					require.NoError(t, err)
					require.Equal(t, 2, testutils.ReadCount(res))
					res.Close()
					return nil
				},
				executor.Options{},
			)

			require.NoError(t, err)

			_, err = exec.Exec(
				ctx,
				"insert into t values (?, ?)",
				executor.Options{}.WithDatabase(db).WithForceRebuildPlan().WithStatementOption(
					executor.StatementOption{}.WithParams([]string{"2", "test2"}),
				),
			)
			require.NoError(t, err)

			res, err := exec.Exec(
				ctx,
				"select count(1) from t",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			require.Equal(t, 3, testutils.ReadCount(res))
			res.Close()
		},
	)
}
