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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path"
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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
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

		t.Run("file_only_progress", func(t *testing.T) {
			testMetricFileOnlyProgress(t, ctx, pool)
		})

		t.Run("no_backslash_escapes", func(t *testing.T) {
			testMetricReplayNoBackslashEscapes(t, ctx, cfg, pool)
		})

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

			// A single CSV record can exceed an inline LOAD, and a
			// character value can exceed the prepared parameter limit.
			wideBatch := uuid.New()
			wideRows := []struct {
				node  string
				value float64
			}{
				{uuid.NewString() + strings.Repeat("n", 891) + `\nX`, 3.25},
				{uuid.NewString() + strings.Repeat("你", 510) + `\nX`, math.Inf(1)},
				{uuid.NewString() + strings.Repeat("n", 894), math.Inf(-1)},
				{uuid.NewString() + strings.Repeat("n", 894), math.NaN()},
			}
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeoutCause(context.Background(), 5*time.Second, moerr.CauseWriteRowRecords)
				defer stop()
				for _, fixture := range wideRows {
					_, err := pool.ExecContext(cleanup, "delete from "+relation+" where node = ?", fixture.node)
					require.NoError(t, err)
				}
				require.NoError(t, dbholder.DeleteMetricBatchReceipt(cleanup, pool, wideBatch))
			})
			var widePayload strings.Builder
			for _, fixture := range wideRows {
				widePayload.WriteString(fmt.Sprintf("\\N,2026-11-25 09:36:01.123456,%v,%s,CN,internal_sql\n", fixture.value, fixture.node))
			}
			checkWideRow := func(node string, value float64) {
				t.Helper()
				rows, err := pool.QueryContext(ctx, "select node, value, account from "+relation+" where node = ?", node)
				require.NoError(t, err)
				defer rows.Close()
				require.True(t, rows.Next())
				var gotNode string
				var gotValue float64
				var account sql.NullString
				require.NoError(t, rows.Scan(&gotNode, &gotValue, &account))
				require.Equal(t, []byte(node), []byte(gotNode))
				if math.IsNaN(value) {
					require.True(t, math.IsNaN(gotValue))
				} else {
					require.Equal(t, math.Float64bits(value), math.Float64bits(gotValue))
				}
				require.False(t, account.Valid)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}
			for range 2 {
				require.NoError(t, etl.ReplayMetricBatch(ctx, mometric.SqlStatementCUTable, []byte(widePayload.String()), wideBatch))
				for _, fixture := range wideRows {
					checkWideRow(fixture.node, fixture.value)
				}
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
			mock.ExpectExec(regexp.QuoteMeta("insert into system_metrics.metric_batch_receipt (batch_id) select ? where not exists(select 1 from system_metrics.metric_batch_receipt where batch_id = ?)")).
				WithArgs(id.String(), id.String()).WillReturnError(restart)
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

func testMetricReplayNoBackslashEscapes(t *testing.T, ctx context.Context, cfg *mysql.Config, queryPool *sql.DB) {
	t.Helper()
	open := func(limit int) *sql.DB {
		t.Helper()
		modeCfg := cfg.Clone()
		modeCfg.MaxAllowedPacket = limit - 1
		modeCfg.Params = map[string]string{
			"max_allowed_packet": fmt.Sprint(limit),
			"sql_mode":           "'STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES'",
		}
		pool, err := sql.Open("mysql", modeCfg.FormatDSN())
		require.NoError(t, err)
		t.Cleanup(func() { _ = pool.Close() })
		var mode string
		var actualLimit int
		require.NoError(t, pool.QueryRowContext(ctx, "SELECT @@session.sql_mode, @@session.max_allowed_packet").Scan(&mode, &actualLimit))
		require.Contains(t, mode, "NO_BACKSLASH_ESCAPES")
		require.Equal(t, limit, actualLimit)
		return pool
	}
	regular, limited := open(1<<20), open(1024)
	defer dbholder.SetDBConn(nil)

	for _, fixture := range []struct {
		tbl *table.Table
		row string
	}{
		{mometric.SingleMetricTable, "test_receipt,2026-11-25 09:36:01.123456,3.25,%s,CN,\\N,0,select\n"},
		{mometric.SqlStatementCUTable, "\\N,2026-11-25 09:36:01.123456,3.25,%s,CN,internal_sql\n"},
	} {
		t.Run(fixture.tbl.Table, func(t *testing.T) {
			nodePrefix := uuid.NewString()
			node := nodePrefix + strings.Repeat("n", 920) + "'\\nX"
			payload := []byte(fmt.Sprintf(fixture.row, node))
			relation := fixture.tbl.GetIdentify()
			deleteRows := func() {
				t.Helper()
				_, err := queryPool.ExecContext(ctx, "delete from "+relation+" where left(node,36) = ?", nodePrefix)
				require.NoError(t, err)
			}
			t.Cleanup(deleteRows)

			fs, err := fileservice.NewLocalETLFS("etl", t.TempDir())
			require.NoError(t, err)
			defer fs.Close(context.Background())
			factory := export.GetWriterFactory(fs, "test-writer", "CN", true)
			now := time.Now()
			dir := fixture.tbl.PathBuilder.Build("sys", table.MergeLogTypeLogs, now, fixture.tbl.Database, fixture.tbl.Table)
			firstWrite := func(pool *sql.DB) {
				t.Helper()
				dbholder.SetDBConn(pool)
				writer := factory.GetRowWriter(ctx, "sys", fixture.tbl, now)
				writer.(table.BufferSettable).SetBuffer(bytes.NewBuffer(payload), nil)
				_, err := writer.FlushAndClose()
				require.NoError(t, err)
			}
			checkRow := func(wantCount int) {
				t.Helper()
				var count, nonNullAccounts int
				var gotNode string
				query := "select count(*), coalesce(max(node),''), count(account) from " + relation + " where left(node,36) = ?"
				require.NoError(t, queryPool.QueryRowContext(ctx, query, nodePrefix).Scan(&count, &gotNode, &nonNullAccounts))
				require.Equal(t, wantCount, count)
				require.Zero(t, nonNullAccounts)
				if wantCount == 1 {
					require.Equal(t, []byte(node), []byte(gotNode))
				}
			}

			firstWrite(regular)
			checkRow(1)
			for _, err := range fs.List(ctx, dir) {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
			}
			deleteRows()

			firstWrite(limited)
			checkRow(0)
			filePaths := make([]string, 0, 1)
			for entry, err := range fs.List(ctx, dir) {
				require.NoError(t, err)
				filePaths = append(filePaths, path.Join(dir, entry.Name))
			}
			require.Len(t, filePaths, 1)
			filePath := filePaths[0]
			vec := fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Size: -1}}}
			require.NoError(t, fs.Read(ctx, &vec))
			uploaded := bytes.Clone(vec.Entries[0].Data)
			vec.Release()
			id, ok := table.MetricBatchID(filePath)
			require.True(t, ok)
			t.Cleanup(func() { require.NoError(t, dbholder.DeleteMetricBatchReceipt(ctx, queryPool, id)) })

			merge, err := export.NewMerge(ctx, "", export.WithFileService(fs), export.WithTable(fixture.tbl))
			require.NoError(t, err)
			defer merge.Stop()
			for attempt := range 2 {
				if attempt > 0 {
					require.NoError(t, fs.Write(ctx, fileservice.IOVector{
						FilePath: filePath, Entries: []fileservice.IOEntry{{Size: int64(len(uploaded)), Data: uploaded}},
					}))
				}
				require.NoError(t, merge.Main(ctx))
				checkRow(1)
				var receipts int
				require.NoError(t, queryPool.QueryRowContext(ctx, "select count(*) from system_metrics.metric_batch_receipt where batch_id = ?", id.String()).Scan(&receipts))
				require.Zero(t, receipts)
				_, err := fs.StatFile(ctx, filePath)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
			}
		})
	}
}

// Delay one importer after it reads the exact bytes, until a second
// importer has committed the data, retired its UUID and deleted the file.
type pausedMetricFileRead struct {
	fileservice.FileService
	path   string
	ready  chan struct{}
	resume chan struct{}
}

func (fs *pausedMetricFileRead) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if err := fs.FileService.Read(ctx, vector); err != nil {
		return err
	}
	if path.Base(vector.FilePath) != path.Base(fs.path) {
		return nil
	}
	close(fs.ready)
	select {
	case <-fs.resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func testMetricFileOnlyProgress(t *testing.T, ctx context.Context, pool *sql.DB) {
	const n = 24
	const timestamp = "2026-11-25 09:36:01.123456"
	tbl := mometric.SingleMetricTable
	node := uuid.NewString()
	fs, err := fileservice.NewLocalETLFS("etl", t.TempDir())
	require.NoError(t, err)
	defer fs.Close(ctx)
	factory := export.GetWriterFactory(fs, "test-writer", "CN", false)
	now := time.Now()
	dir := tbl.PathBuilder.Build("sys", table.MergeLogTypeLogs, now, tbl.Database, tbl.Table)
	row := tbl.GetRow(ctx)
	defer row.Free()
	collected, err := time.Parse("2006-01-02 15:04:05.000000", timestamp)
	require.NoError(t, err)
	row.SetVal("metric_name", table.StringField("receipt_progress"))
	row.SetVal("collecttime", table.TimeField(collected))
	row.SetVal("value", table.Float64Field(1))
	row.SetVal("node", table.StringField(node))
	row.SetVal("role", table.StringField("CN"))
	row.SetVal("account", table.StringField("sys"))
	row.SetVal("type", table.StringField("select"))

	files := make([]string, n)
	ids := make([]uuid.UUID, n)
	var publisher uuid.UUID
	var firstPayload []byte
	publish := func(i int) {
		t.Helper()
		writer := factory.GetRowWriter(ctx, "sys", tbl, now)
		require.NoError(t, writer.WriteRow(row))
		_, err := writer.FlushAndClose()
		require.NoError(t, err)
		for entry, err := range fs.List(ctx, dir) {
			require.NoError(t, err)
			file := path.Join(dir, entry.Name)
			if i > 0 && file == files[0] {
				continue
			}
			files[i] = file
			break
		}
		require.NotEmpty(t, files[i])
		var ok bool
		ids[i], ok = table.MetricBatchID(files[i])
		require.True(t, ok)
		vec := fileservice.IOVector{FilePath: files[i], Entries: []fileservice.IOEntry{{Size: -1}}}
		require.NoError(t, fs.Read(ctx, &vec))
		defer vec.Release()
		payload := vec.Entries[0].Data
		require.Greater(t, len(payload), 24)
		require.Equal(t, uint64(i+1), binary.BigEndian.Uint64(payload[16:24]))
		if i == 0 {
			copy(publisher[:], payload[:16])
			firstPayload = bytes.Clone(payload)
		}
		require.Equal(t, publisher[:], payload[:16])
	}
	publish(0)
	defer func() {
		_, err := pool.ExecContext(ctx, "delete from "+tbl.GetIdentify()+" where node = ?", node)
		require.NoError(t, err)
		_, err = pool.ExecContext(ctx, "delete from system_metrics.metric_file_progress where publisher_id = ?", publisher.String())
		require.NoError(t, err)
	}()
	require.NoError(t, fs.Delete(ctx, files[0]))
	merge, err := export.NewMerge(ctx, "", export.WithFileService(fs), export.WithTable(tbl))
	require.NoError(t, err)
	defer merge.Stop()
	dbholder.SetDBConn(pool)
	defer dbholder.SetDBConn(nil)
	for i := 1; i < n; i++ {
		publish(i)
		require.NoError(t, merge.Main(ctx))
		var receipts int
		require.NoError(t, pool.QueryRowContext(ctx, "select count(*) from system_metrics.metric_batch_receipt where batch_id = ?", ids[i].String()).Scan(&receipts))
		require.Zero(t, receipts)
	}
	var count, high int
	query := "select count(*), max(last_seq) from system_metrics.metric_file_progress where publisher_id = ? and first_seq >= 0"
	require.NoError(t, pool.QueryRowContext(ctx, query, publisher.String()).Scan(&count, &high))
	require.Equal(t, 2, count, "one unresolved gap leaves one extra range")
	require.Equal(t, n, high)

	restore := func() {
		t.Helper()
		require.NoError(t, fs.Write(ctx, fileservice.IOVector{
			FilePath: files[0], Entries: []fileservice.IOEntry{{Size: int64(len(firstPayload)), Data: firstPayload}},
		}))
	}
	restore()
	gate := &pausedMetricFileRead{FileService: fs, path: files[0], ready: make(chan struct{}), resume: make(chan struct{})}
	paused, err := export.NewMerge(ctx, "", export.WithFileService(gate), export.WithTable(tbl))
	require.NoError(t, err)
	defer paused.Stop()
	result := make(chan error, 1)
	go func() { result <- paused.Main(ctx) }()
	var release sync.Once
	defer release.Do(func() { close(gate.resume) })
	select {
	case <-gate.ready:
	case err := <-result:
		t.Fatalf("importer exited before reading the pending file: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, merge.Main(ctx))
	var receiptCount int
	require.NoError(t, pool.QueryRowContext(ctx, "select count(*) from system_metrics.metric_batch_receipt where batch_id = ?", ids[0].String()).Scan(&receiptCount))
	require.Zero(t, receiptCount)
	_, err = fs.StatFile(ctx, files[0])
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	release.Do(func() { close(gate.resume) })
	require.NoError(t, <-result)

	// A storage retry can recreate even an already deleted file.
	restore()
	require.NoError(t, merge.Main(ctx))
	_, err = fs.StatFile(ctx, files[0])
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	require.NoError(t, pool.QueryRowContext(ctx, query, publisher.String()).Scan(&count, &high))
	require.Equal(t, 1, count, "filling the gap coalesces the prefix")
	require.Equal(t, n, high)
	require.NoError(t, pool.QueryRowContext(ctx, "select count(*) from "+tbl.GetIdentify()+" where node = ?", node).Scan(&count))
	require.Equal(t, n, count)
}

func TestMetricReceiptRetirementAcrossCNs(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, err := NewCluster(WithTesting(), WithTestAdmissionContext(t.Context()), WithCNCount(2), WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *ServiceConfig) {
				cfg.CN.Txn.Mode = txn.TxnMode_Optimistic.String()
				cfg.CN.Txn.Isolation = txn.TxnIsolation_SI.String()
			})
		}
	}))
	if c != nil {
		t.Cleanup(func() {
			require.NoError(t, c.Close())
			require.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath))
		})
	}
	require.NoError(t, err)
	require.NoError(t, c.Start())
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	user, err := dbholder.GetSQLWriterDBUser()
	require.NoError(t, err)
	connect := func(index int) *sql.DB {
		t.Helper()
		cn, err := c.GetCNService(index)
		require.NoError(t, err)
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd = user.UserName, user.Password
		cfg.Net, cfg.Addr = "tcp", fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	original, replay := connect(0), connect(1)
	for _, originCommitted := range []bool{true, false} {
		t.Run(fmt.Sprintf("committed=%t", originCommitted), func(t *testing.T) {
			batch := uuid.New()
			publisher := uuid.New()
			node := uuid.NewString()
			loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='sys,2026-11-25 09:36:01.123456,1,%s,CN,internal_sql\n' INTO TABLE system_metrics.sql_statement_cu FIELDS TERMINATED BY ','", node)
			load := func(conn *sql.Conn) error {
				_, err := conn.ExecContext(ctx, loadSQL)
				return err
			}
			const originFence = "insert into system_metrics.metric_file_progress (publisher_id, first_seq, last_seq) values (?, -1, 0) on duplicate key update revision = 1-revision"
			if !originCommitted {
				// A long-running writer already has an origin fence. Retirement
				// must conflict with an older update of that row as well.
				_, err := original.ExecContext(ctx, originFence, publisher.String())
				require.NoError(t, err)
			}
			first, err := original.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer first.Rollback()
			_, err = first.ExecContext(ctx, "insert into system_metrics.metric_batch_receipt (batch_id) values (?)", batch.String())
			require.NoError(t, err)
			_, err = first.ExecContext(ctx, loadSQL)
			require.NoError(t, err)
			_, err = first.ExecContext(ctx, originFence, publisher.String())
			require.NoError(t, err)
			if originCommitted {
				require.NoError(t, first.Commit())
				// Complete later files before the fallback publisher becomes visible.
				require.NoError(t, dbholder.CommitMetricFile(ctx, replay, uuid.New(), dbholder.MetricFilePublication{Publisher: publisher, Sequence: 2}, func(*sql.Conn) error { return nil }))
				var receiptCount int
				require.NoError(t, replay.QueryRowContext(ctx, "select count(*) from system_metrics.metric_batch_receipt where batch_id = ?", batch.String()).Scan(&receiptCount))
				require.Equal(t, 1, receiptCount, "a receipt awaiting fallback cannot be reclaimed by later files")
			}
			publication := dbholder.MetricFilePublication{Publisher: publisher, Sequence: 1}
			require.NoError(t, dbholder.CommitMetricFile(ctx, replay, batch, publication, load))
			if !originCommitted {
				// Replay retired the UUID before the optimistic original reached
				// COMMIT. The original must conflict on the durable origin fence.
				require.Error(t, first.Commit())
			}
			require.NoError(t, dbholder.CommitMetricFile(ctx, original, batch, publication, func(*sql.Conn) error {
				return fmt.Errorf("retired publication reached LOAD")
			}))
			var rows, receipts int
			require.NoError(t, replay.QueryRowContext(ctx, "select count(*) from system_metrics.sql_statement_cu where node = ?", node).Scan(&rows))
			require.Equal(t, 1, rows)
			require.NoError(t, replay.QueryRowContext(ctx, "select count(*) from system_metrics.metric_batch_receipt where batch_id = ?", batch.String()).Scan(&receipts))
			require.Zero(t, receipts)
		})
	}
}
