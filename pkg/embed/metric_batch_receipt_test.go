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
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
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
				count := func(want int) {
					t.Helper()
					var rows int
					require.NoError(t, pool.QueryRowContext(ctx, "select count(*) from "+relation+" where node = ?", node).Scan(&rows))
					require.Equal(t, want, rows)
				}

				// A failed LOAD must roll back its receipt as well.
				require.Error(t, dbholder.CommitMetricBatch(ctx, pool, batchID, "LOAD DATA bad syntax"))
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
					retry <- dbholder.CommitMetricBatch(ctx, pool, batchID, loadSQL)
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
				require.NoError(t, dbholder.CommitMetricBatch(ctx, pool, batchID, loadSQL))
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
				require.NoError(t, dbholder.CommitMetricBatch(ctx, pool, otherID, loadSQL))
				count(4)
			})
		}
	})
}
