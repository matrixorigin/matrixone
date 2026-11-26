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

package embed

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
)

func TestMetricBatchReceiptSQL(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		ctx, cancel := context.WithTimeoutCause(context.Background(), 45*time.Second, moerr.CauseWriteRowRecords)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		user, err := dbholder.GetSQLWriterDBUser()
		require.NoError(t, err)
		require.Equal(t, dbholder.MOLoggerUser, user.UserName)
		cfg := mysql.NewConfig()
		cfg.User = user.UserName
		cfg.Passwd = user.Password
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		pool, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		defer pool.Close()

		var receiptTableID uint64
		require.NoError(t, pool.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='system_metrics' and relname='metric_batch_receipt'").Scan(&receiptTableID))
		locks := lockservice.GetLockServiceByServiceID(cn.ServiceID())
		require.NotNil(t, locks)

		for _, fixture := range []struct {
			name string
			csv  func(string) string
		}{
			{"metric", func(node string) string {
				return fmt.Sprintf("test_receipt,2026-11-25 09:36:01.123456,1,%s,CN,sys,0,select\n"+
					"test_receipt,2026-11-25 09:36:01.123456,2,%s,TN,tenant,1,insert\n", node, node)
			}},
			{"sql_statement_cu", func(node string) string {
				return fmt.Sprintf("sys,2026-11-25 09:36:01.123456,1,%s,CN,internal_sql\n"+
					"sys,2026-11-25 09:36:01.123456,2,%s,TN,external_sql\n", node, node)
			}},
		} {
			t.Run(fixture.name, func(t *testing.T) {
				relation := "system_metrics." + fixture.name
				node := uuid.NewString()
				batchID := uuid.New()
				otherID := uuid.New()
				packer := types.NewPacker()
				packer.EncodeStringType([]byte(batchID.String()))
				receiptKey := bytes.Clone(packer.Bytes())
				packer.Close()
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeoutCause(context.Background(), 5*time.Second, moerr.CauseWriteRowRecords)
					defer stop()
					_, err := pool.ExecContext(cleanup, "delete from "+relation+" where node = ?", node)
					require.NoError(t, err)
					for _, id := range []uuid.UUID{batchID, otherID} {
						require.NoError(t, dbholder.DeleteMetricBatchReceipt(cleanup, pool, id))
					}
				})
				loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='%s' INTO TABLE %s FIELDS TERMINATED BY ','", fixture.csv(node), relation)
				load := func(query string) func(*sql.Conn) error {
					return func(conn *sql.Conn) error {
						_, err := conn.ExecContext(ctx, query)
						return err
					}
				}
				count := func(want int) {
					t.Helper()
					var rows int
					require.NoError(t, pool.QueryRowContext(ctx, "select count(*) from "+relation+" where node = ?", node).Scan(&rows))
					require.Equal(t, want, rows)
				}

				// A failed LOAD must roll back its receipt as well.
				require.Error(t, dbholder.CommitMetricBatch(ctx, pool, batchID, load("LOAD DATA bad syntax")))
				count(0)
				var receiptCount int
				require.NoError(t, pool.QueryRowContext(ctx, "select count(*) from system_metrics.metric_batch_receipt where batch_id = ?", batchID.String()).Scan(&receiptCount))
				require.Zero(t, receiptCount)

				// Hold a real first-write transaction while the same batch is retried
				// using a second connection. Retryable conflicts may keep the file.
				first, err := pool.BeginTx(ctx, nil)
				require.NoError(t, err)
				var worker sync.WaitGroup
				defer func() {
					_ = first.Rollback()
					worker.Wait()
				}()
				_, err = first.ExecContext(ctx, "insert into system_metrics.metric_batch_receipt (batch_id) values (?)", batchID.String())
				require.NoError(t, err)
				_, err = first.ExecContext(ctx, loadSQL)
				require.NoError(t, err)
				count(0)
				retry := make(chan error, 1)
				worker.Go(func() {
					retry <- dbholder.CommitMetricBatch(ctx, pool, batchID, load(loadSQL))
				})
				require.Eventually(t, func() bool {
					found := false
					locks.IterLocks(func(tableID uint64, keys [][]byte, held lockservice.Lock) bool {
						if tableID != receiptTableID {
							return true
						}
						for _, key := range keys {
							if bytes.Equal(key, receiptKey) {
								held.IterWaiters(func(lock.WaitTxn) bool {
									found = true
									return false
								})
								break
							}
						}
						return !found
					})
					return found
				}, 10*time.Second, 20*time.Millisecond, "the retry must contend on the outstanding receipt")
				require.NoError(t, first.Commit())
				require.NoError(t, <-retry)
				count(2)
				require.NoError(t, dbholder.CommitMetricBatch(ctx, pool, batchID, load(loadSQL)))
				count(2)
				func() {
					rows, err := pool.QueryContext(ctx, "select role, value from "+relation+" where node = ? order by role", node)
					require.NoError(t, err)
					defer rows.Close()
					for i, want := range []string{"CN", "TN"} {
						require.True(t, rows.Next())
						var role string
						var value float64
						require.NoError(t, rows.Scan(&role, &value))
						require.Equal(t, want, role)
						require.Equal(t, float64(i+1), value)
					}
					require.False(t, rows.Next())
					require.NoError(t, rows.Err())
				}()

				// Independent collections can have identical samples.
				require.NoError(t, dbholder.CommitMetricBatch(ctx, pool, otherID, load(loadSQL)))
				count(4)
			})
		}

		t.Run("packet_limited_replay", func(t *testing.T) {
			// Use the same packet accounting as a production driver
			// that negotiated a 1024-byte server limit.
			limitedCfg := cfg.Clone()
			limitedCfg.MaxAllowedPacket = 1023
			limitedCfg.Params = map[string]string{"max_allowed_packet": "1024"}
			limitedPool, err := sql.Open("mysql", limitedCfg.FormatDSN())
			require.NoError(t, err)
			defer limitedPool.Close()
			var limit int
			require.NoError(t, limitedPool.QueryRowContext(ctx, "SELECT @@session.max_allowed_packet").Scan(&limit))
			require.Equal(t, 1024, limit)

			node := uuid.NewString()
			id := uuid.New()
			const relation = "system_metrics.sql_statement_cu"
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeoutCause(context.Background(), 5*time.Second, moerr.CauseWriteRowRecords)
				defer stop()
				_, err := pool.ExecContext(cleanup, "delete from "+relation+" where node = ?", node)
				require.NoError(t, err)
				require.NoError(t, dbholder.DeleteMetricBatchReceipt(cleanup, pool, id))
			})
			const rowCount = 32
			row := fmt.Sprintf("sys,2026-11-25 09:36:01.123456,2,%s,CN,internal_sql\n", node)
			payload := strings.Repeat(row, rowCount)
			err = dbholder.CommitMetricBatch(ctx, limitedPool, id, func(conn *sql.Conn) error {
				_, err := conn.ExecContext(ctx, "LOAD DATA INLINE FORMAT='csv', DATA='"+payload+"' INTO TABLE "+relation+" FIELDS TERMINATED BY ','")
				return err
			})
			require.ErrorIs(t, err, mysql.ErrPktTooLarge)
			count := func(want int) {
				t.Helper()
				var rows int
				var sum float64
				require.NoError(t, pool.QueryRowContext(ctx, "select count(*), coalesce(sum(value), 0) from "+relation+" where node = ?", node).Scan(&rows, &sum))
				require.Equal(t, want, rows)
				require.Equal(t, float64(2*want), sum)
			}
			count(0)
			dbholder.SetDBConn(limitedPool)
			defer dbholder.SetDBConn(nil)
			for range 2 {
				require.NoError(t, etl.ReplayMetricBatch(ctx, mometric.SqlStatementCUTable, []byte(payload), id))
				count(rowCount)
			}
		})

		t.Run("reselect_after_rolling_restart", func(t *testing.T) {
			oldPool, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer oldPool.Close()
			dbholder.SetDBConn(oldPool)
			defer dbholder.CloseDBConn()
			defer dbholder.SetSQLWriterDBAddressFunc(dbholder.GetSQLWriterDBAddressFunc())
			var reselections int
			dbholder.SetSQLWriterDBAddressFunc(func(_ context.Context, random bool) (string, error) {
				require.True(t, random)
				reselections++
				return cfg.Addr, nil
			})

			node := uuid.NewString()
			id := uuid.New()
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeoutCause(context.Background(), 5*time.Second, moerr.CauseWriteRowRecords)
				defer stop()
				_, err := pool.ExecContext(cleanup, "delete from system_metrics.sql_statement_cu where node = ?", node)
				require.NoError(t, err)
				require.NoError(t, dbholder.DeleteMetricBatchReceipt(cleanup, pool, id))
			})
			row := fmt.Appendf(nil, "sys,2026-11-25 09:36:01.123456,2,%s,CN,internal_sql\n", node)
			restart := &mysql.MySQLError{Number: moerr.ErrNewTxnInCNRollingRestart}
			mock.ExpectExec("^START TRANSACTION$").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(regexp.QuoteMeta("insert into system_metrics.metric_batch_receipt (batch_id) values (?)")).
				WithArgs(id.String()).WillReturnError(restart)
			mock.ExpectExec("^ROLLBACK$").WillReturnResult(sqlmock.NewResult(0, 0))
			require.ErrorIs(t, etl.ReplayMetricBatch(ctx, mometric.SqlStatementCUTable, row, id), restart)
			require.NoError(t, mock.ExpectationsWereMet())
			require.Zero(t, reselections)

			// A sparse retry still selects the healthy embedded CN.
			dbholder.DBConnErrCount.Reset()
			for range 2 {
				require.NoError(t, etl.ReplayMetricBatch(ctx, mometric.SqlStatementCUTable, row, id))
				var count int
				require.NoError(t, pool.QueryRowContext(ctx, "select count(*) from system_metrics.sql_statement_cu where node = ?", node).Scan(&count))
				require.Equal(t, 1, count)
			}
			require.Equal(t, 1, reselections)
		})
	})
}
