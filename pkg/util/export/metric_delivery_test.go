// Copyright 2022 Matrix Origin
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

package export

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

const (
	insertReceipt = "insert into system_metrics.metric_batch_receipt (batch_id) values (?)"
	selectReceipt = "select exists(select 1 from system_metrics.metric_batch_receipt where batch_id = ?)"
	sampleTime    = "2026-11-25 09:36:01.123456"
)

var commitUnknown = &mysql.MySQLError{Number: moerr.ErrTxnUnknown, Message: "txn commit status is unknown"}

func metricDeliveryDB(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	provider, backoff := dbholder.GetOrInitDBConn, dbholder.DBConnErrCount
	dbholder.GetOrInitDBConn = func(bool, bool) (*sql.DB, error) { return db, nil }
	dbholder.DBConnErrCount = dbholder.NewReConnectionBackOff(time.Minute, dbholder.DBConnRetryThreshold)
	dbholder.SetDBConn(db)
	t.Cleanup(func() {
		dbholder.SetDBConn(nil)
		dbholder.GetOrInitDBConn, dbholder.DBConnErrCount = provider, backoff
		require.NoError(t, mock.ExpectationsWereMet())
	})
	return mock
}

func expectMetricLoad(mock sqlmock.Sqlmock, tbl *table.Table, id uuid.UUID, csv string, replay bool) {
	mock.ExpectExec("^START TRANSACTION$").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(id.String()).WillReturnResult(sqlmock.NewResult(0, 1))
	if replay {
		expectMetricPacket(mock, 1<<20)
	}
	expectMetricLoadSQL(mock, tbl, csv)
}

func expectMetricPacket(mock sqlmock.Sqlmock, limit int) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT @@session.max_allowed_packet")).
		WillReturnRows(sqlmock.NewRows([]string{"limit"}).AddRow(limit))
}

func expectMetricCommit(mock sqlmock.Sqlmock) {
	mock.ExpectExec("^COMMIT$").WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectMetricRollback(mock sqlmock.Sqlmock) {
	mock.ExpectExec("^ROLLBACK$").WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectMetricLoadSQL(mock sqlmock.Sqlmock, tbl *table.Table, csv string) {
	escaped := strings.NewReplacer("\\", "\\\\", "'", "''").Replace(csv)
	query := "LOAD DATA INLINE FORMAT='csv', DATA='" + escaped + "' INTO TABLE " + tbl.GetIdentify() + " FIELDS TERMINATED BY ','"
	mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectCommittedReceipt(mock sqlmock.Sqlmock, id uuid.UUID) {
	mock.ExpectExec("^START TRANSACTION$").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(id.String()).WillReturnError(&mysql.MySQLError{Number: moerr.ER_DUP_ENTRY})
	mock.ExpectExec("^ROLLBACK$").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(selectReceipt)).WithArgs(id.String()).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
}

func TestMetricFallbackDelivery(t *testing.T) {
	for _, fixture := range []struct {
		tbl *table.Table
		csv string
	}{
		{mometric.SingleMetricTable, "m," + sampleTime + ",1,cn-1,CN,sys,0,\n" + "m," + sampleTime + ",1,tn-1,TN,tenant,1,select\n"},
		{mometric.SqlStatementCUTable, "sys," + sampleTime + ",1,cn-1,CN,internal_sql\n" + "sys," + sampleTime + ",1,cn-1,CN,external_sql\n"},
	} {
		for _, committed := range []bool{false, true} {
			name := "rolled_back"
			if committed {
				name = "committed"
			}
			t.Run(fixture.tbl.GetName()+"/"+name, func(t *testing.T) {
				ctx := context.Background()
				fs := testutil.NewFS(t)
				t.Cleanup(func() { fs.Close(ctx) })
				mock := metricDeliveryDB(t)
				id := uuid.New()
				file := table.MetricBatchPath("etl:sys/logs/metric-delivery.csv", id)
				tbl := fixture.tbl
				writer := etl.NewMetricContentWriter(ctx, tbl, etl.NewCSVWriter(ctx, etl.NewFSWriter(ctx, fs, etl.WithFilePath(file))), id)
				writer.SetBuffer(bytes.NewBufferString(fixture.csv), nil)

				expectMetricLoad(mock, tbl, id, fixture.csv, false)
				if committed {
					mock.ExpectExec("^COMMIT$").WillReturnError(commitUnknown)
					expectMetricRollback(mock)
				} else {
					mock.ExpectExec("^COMMIT$").WillReturnError(&mysql.MySQLError{Number: 1213})
					expectMetricRollback(mock)
				}
				_, err := writer.FlushAndClose()
				require.NoError(t, err)
				info, err := fs.StatFile(ctx, file)
				require.NoError(t, err)
				m, err := NewMerge(ctx, "", WithFileService(fs), WithTable(tbl))
				require.NoError(t, err)
				t.Cleanup(m.Stop)

				if committed {
					expectCommittedReceipt(mock, id)
				} else {
					expectMetricLoad(mock, tbl, id, fixture.csv, true)
					expectMetricCommit(mock)
				}
				require.NoError(t, m.doMergeFiles(ctx, []*FileMeta{{FilePath: file, FileSize: info.Size}}))
				_, err = fs.StatFile(ctx, file)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
			})
		}
	}
}

type metricReceiptArgument struct {
	id uuid.UUID
}

func (arg *metricReceiptArgument) Match(value driver.Value) bool {
	raw, ok := value.(string)
	if !ok {
		return false
	}
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return false
	}
	arg.id = id
	return true
}

func TestMetricFallbackWithLongNodeID(t *testing.T) {
	for _, backend := range []struct {
		name      string
		nodeBytes int
		diskCache bool
	}{
		{name: "local", nodeBytes: 200},
		{name: "s3_disk_cache", nodeBytes: 180, diskCache: true},
	} {
		node := strings.Repeat("c", backend.nodeBytes)
		for _, fixture := range []struct {
			tbl     *table.Table
			payload string
		}{
			{mometric.SingleMetricTable, "m," + sampleTime + ",1," + node + ",CN,sys,0,\n"},
			{mometric.SqlStatementCUTable, "sys," + sampleTime + ",1," + node + ",CN,internal_sql\n"},
		} {
			t.Run(backend.name+"/"+fixture.tbl.Table, func(t *testing.T) {
				ctx := t.Context()
				var fs fileservice.FileService
				var cache *fileservice.S3FS
				var cacheDir string
				var err error
				if backend.diskCache {
					cacheDir = t.TempDir()
					capacity := toml.ByteSize(16 * mpool.MB)
					cache, err = fileservice.NewS3FS(ctx,
						fileservice.ObjectStorageArguments{Name: "etl", Bucket: t.TempDir(), Endpoint: "disk"},
						fileservice.CacheConfig{DiskPath: &cacheDir, DiskCapacity: &capacity}, nil, false, true)
					fs = cache
				} else {
					fs, err = fileservice.NewLocalETLFS("etl", t.TempDir())
				}
				require.NoError(t, err)
				t.Cleanup(func() { fs.Close(context.Background()) })
				mock := metricDeliveryDB(t)
				receipt := &metricReceiptArgument{}
				mock.ExpectExec("^START TRANSACTION$").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(receipt).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("^LOAD DATA INLINE").WillReturnError(&mysql.MySQLError{Number: 1213})
				expectMetricRollback(mock)

				now := time.Unix(1795599361, 0)
				writer := GetWriterFactory(fs, node, "CN", true).GetRowWriter(ctx, "sys", fixture.tbl, now)
				writer.(table.BufferSettable).SetBuffer(bytes.NewBufferString(fixture.payload), nil)
				_, err = writer.FlushAndClose()
				require.NoError(t, err)

				dir := fixture.tbl.PathBuilder.Build("sys", table.MergeLogTypeLogs, now, fixture.tbl.Database, fixture.tbl.Table)
				filePaths := make([]string, 0, 1)
				for file, err := range fs.List(ctx, dir) {
					require.NoError(t, err)
					require.False(t, file.IsDir)
					require.LessOrEqual(t, len(file.Name), 245)
					filePath := path.Join(dir, file.Name)
					id, ok := table.MetricBatchID(filePath)
					require.True(t, ok)
					require.Equal(t, receipt.id, id)
					parsed, err := fixture.tbl.PathBuilder.ParsePath(ctx, filePath)
					require.NoError(t, err)
					require.Equal(t, fixture.tbl.Table, parsed.Table())
					filePaths = append(filePaths, filePath)
				}
				require.Len(t, filePaths, 1)
				var cacheFiles []string
				if cache != nil {
					cache.FlushCache(ctx)
					relative := filepath.FromSlash("full" + filePaths[0] + ".mofscache")
					cacheFiles, err = filepath.Glob(filepath.Join(cacheDir, "*", relative))
					require.NoError(t, err)
				}
				expectMetricLoad(mock, fixture.tbl, receipt.id, fixture.payload, true)
				expectMetricCommit(mock)
				merge, err := NewMerge(ctx, "", WithFileService(fs), WithTable(fixture.tbl))
				require.NoError(t, err)
				t.Cleanup(merge.Stop)
				require.NoError(t, merge.mergeMetricFile(ctx, filePaths[0]))
				_, err = fs.StatFile(ctx, filePaths[0])
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
				if cache != nil {
					require.Len(t, cacheFiles, 1, "the CSV must have been present in the disk cache")
					require.NoFileExists(t, cacheFiles[0])
				}
			})
		}
	}
}

func TestMetricMergeRecoversCommitAndDeleteFailures(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	mock := metricDeliveryDB(t)
	tbl := mometric.SingleMetricTable
	id := uuid.New()
	file := table.MetricBatchPath("etl:sys/logs/retry.csv", id)
	payload := "m," + sampleTime + ",1,cn-1,CN,sys,0,\n"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: file, Entries: []fileservice.IOEntry{{Size: int64(len(payload)), Data: []byte(payload)}},
	}))
	wrapped := &failMetricFileDelete{FileService: fs}
	wrapped.fail.Store(true)
	merge, err := NewMerge(ctx, "", WithTable(tbl), WithFileService(wrapped))
	require.NoError(t, err)
	t.Cleanup(merge.Stop)
	expectMetricLoad(mock, tbl, id, payload, true)
	mock.ExpectExec("^COMMIT$").WillReturnError(commitUnknown)
	expectMetricRollback(mock)
	require.ErrorIs(t, merge.mergeMetricFile(ctx, file), commitUnknown)
	_, err = fs.StatFile(ctx, file)
	require.NoError(t, err)

	expectCommittedReceipt(mock, id)
	require.Error(t, merge.mergeMetricFile(ctx, file))
	_, err = fs.StatFile(ctx, file)
	require.NoError(t, err)

	// Start another merger with no remembered in-memory information.
	next, err := NewMerge(ctx, "", WithTable(tbl), WithFileService(wrapped))
	require.NoError(t, err)
	t.Cleanup(next.Stop)
	expectCommittedReceipt(mock, id)
	require.NoError(t, next.mergeMetricFile(ctx, file))
	_, err = fs.StatFile(ctx, file)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestMetricReplayChunksRollBackTogether(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	mock := metricDeliveryDB(t)
	tbl := mometric.SqlStatementCUTable
	id := uuid.New()
	file := table.MetricBatchPath("etl:sys/logs/chunks.csv", id)
	// A record has an embedded newline and extra SQL escaping. Its
	// encoded length, rather than its line length, controls the split.
	first := "sys," + sampleTime + ",1,\"cn'\\\nnode\",CN,internal_sql\n"
	second := "sys," + sampleTime + ",2,cn,CN,external_sql\n"
	payload := first + second
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: file, Entries: []fileservice.IOEntry{{Size: int64(len(payload)), Data: []byte(payload)}},
	}))
	merge, err := NewMerge(ctx, "", WithTable(tbl), WithFileService(fs))
	require.NoError(t, err)
	t.Cleanup(merge.Stop)

	expectBegin := func() {
		mock.ExpectExec("^START TRANSACTION$").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(id.String()).WillReturnResult(sqlmock.NewResult(0, 1))
		expectMetricPacket(mock, 195)
		expectMetricLoadSQL(mock, tbl, first)
	}
	chunkErr := &mysql.MySQLError{Number: 1213}
	expectBegin()
	mock.ExpectExec("LOAD DATA INLINE.*external_sql").WillReturnError(chunkErr)
	expectMetricRollback(mock)
	require.ErrorIs(t, merge.mergeMetricFile(ctx, file), chunkErr)
	_, err = fs.StatFile(ctx, file)
	require.NoError(t, err)

	expectBegin()
	expectMetricLoadSQL(mock, tbl, second)
	expectMetricCommit(mock)
	require.NoError(t, merge.mergeMetricFile(ctx, file))
	_, err = fs.StatFile(ctx, file)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestMetricReplayRenewsDeadlinePerLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := metricDeliveryDB(t)
		tbl := mometric.SqlStatementCUTable
		id := uuid.New()
		row := "sys," + sampleTime + ",1,cn,CN,internal_sql\n"
		mock.ExpectExec("^START TRANSACTION$").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(id.String()).WillReturnResult(sqlmock.NewResult(0, 1))
		expectMetricPacket(mock, 185)
		for range 3 {
			mock.ExpectExec(regexp.QuoteMeta("LOAD DATA INLINE FORMAT='csv', DATA='" + row + "' INTO TABLE system_metrics.sql_statement_cu FIELDS TERMINATED BY ','")).
				WillDelayFor(4 * time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		expectMetricCommit(mock)

		start := time.Now()
		err := etl.ReplayMetricBatch(t.Context(), tbl, []byte(strings.Repeat(row, 3)), id)
		require.NoError(t, err)
		require.Equal(t, 12*time.Second, time.Since(start))
	})
}

func TestMetricReplayReselectsAfterConnectionError(t *testing.T) {
	mock := metricDeliveryDB(t)
	dbholder.SetSQLWriterDBUser("test", "test")
	address := dbholder.GetSQLWriterDBAddressFunc()
	t.Cleanup(func() { dbholder.SetSQLWriterDBAddressFunc(address) })
	reselected := errors.New("CN selection attempted")
	dbholder.SetSQLWriterDBAddressFunc(func(_ context.Context, random bool) (string, error) {
		require.True(t, random)
		return "", reselected
	})
	id := uuid.New()
	tbl := mometric.SingleMetricTable
	payload := []byte("m," + sampleTime + ",1,cn-1,CN,sys,0,\n")
	mock.ExpectExec("^START TRANSACTION$").WillReturnError(mysql.ErrInvalidConn)
	mock.ExpectClose()
	require.ErrorIs(t, etl.ReplayMetricBatch(t.Context(), tbl, payload, id), mysql.ErrInvalidConn)

	// Reproduce the state of a lone file after the normal retry window.
	dbholder.DBConnErrCount.Reset()
	require.ErrorIs(t, etl.ReplayMetricBatch(t.Context(), tbl, payload, id), reselected)
}

func TestMetricReplayReselectsAfterCNRefusal(t *testing.T) {
	for _, failure := range []struct {
		name string
		code uint16
	}{
		{"new_txn_during_restart", moerr.ErrNewTxnInCNRollingRestart},
		{"retry_after_restart", moerr.ErrRetryForCNRollingRestart},
		{"server_shutdown", moerr.ER_SERVER_SHUTDOWN},
	} {
		t.Run(failure.name, func(t *testing.T) {
			mock := metricDeliveryDB(t)
			dbholder.SetSQLWriterDBUser("test", "test")
			address := dbholder.GetSQLWriterDBAddressFunc()
			t.Cleanup(func() { dbholder.SetSQLWriterDBAddressFunc(address) })
			reselected := errors.New("CN selection attempted")
			dbholder.SetSQLWriterDBAddressFunc(func(_ context.Context, random bool) (string, error) {
				require.True(t, random)
				return "", reselected
			})
			id := uuid.New()
			tbl := mometric.SingleMetricTable
			payload := []byte("m," + sampleTime + ",1,cn-1,CN,sys,0,\n")
			restart := &mysql.MySQLError{Number: failure.code}
			mock.ExpectExec("^START TRANSACTION$").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(id.String()).WillReturnError(restart)
			expectMetricRollback(mock)
			require.ErrorIs(t, etl.ReplayMetricBatch(t.Context(), tbl, payload, id), restart)

			dbholder.DBConnErrCount.Reset()
			require.ErrorIs(t, etl.ReplayMetricBatch(t.Context(), tbl, payload, id), reselected)
		})
	}
}

func TestMetricReplayCanceledDuringCommitRetainsFile(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	mock := metricDeliveryDB(t)
	tbl := mometric.SingleMetricTable
	id := uuid.New()
	file := table.MetricBatchPath("etl:sys/logs/cancel-commit.csv", id)
	payload := "m," + sampleTime + ",1,cn-1,CN,sys,0,\n"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: file, Entries: []fileservice.IOEntry{{Size: int64(len(payload)), Data: []byte(payload)}},
	}))
	merge, err := NewMerge(ctx, "", WithTable(tbl), WithFileService(fs))
	require.NoError(t, err)
	t.Cleanup(merge.Stop)

	expectMetricLoad(mock, tbl, id, payload, true)
	mock.ExpectExec("^COMMIT$").WillDelayFor(10 * time.Second).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectClose()
	start := time.Now()
	taskCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	require.Error(t, merge.mergeMetricFile(taskCtx, file))
	require.ErrorIs(t, taskCtx.Err(), context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second)
	_, err = fs.StatFile(ctx, file)
	require.NoError(t, err)
}

func TestMetricMergeDeliversTAE(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	mock := metricDeliveryDB(t)
	tbl := mometric.SqlStatementCUTable
	file := "etl:sys/logs/2026/11/25/sql_statement_cu/1795599361_cn_CN_111111111.tae"
	writer := etl.NewTAEWriter(ctx, tbl, mpool.MustNewZero(), file, fs)
	require.NoError(t, writer.WriteStrings([]string{"sys", sampleTime, "1", "cn", "CN", "internal_sql"}))
	_, err := writer.FlushAndClose()
	require.NoError(t, err)
	info, err := fs.StatFile(ctx, file)
	require.NoError(t, err)
	merge, err := NewMerge(ctx, "", WithTable(tbl), WithFileService(fs))
	require.NoError(t, err)
	t.Cleanup(merge.Stop)

	mock.ExpectPing()
	mock.ExpectExec(regexp.QuoteMeta("LOAD DATA INLINE FORMAT='csv', DATA='sys," + sampleTime + ",1.000000,cn,CN,internal_sql\n' INTO TABLE system_metrics.sql_statement_cu FIELDS TERMINATED BY ','")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, merge.doMergeFiles(ctx, []*FileMeta{{FilePath: file, FileSize: info.Size}}))
	_, err = fs.StatFile(ctx, file)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

type failMetricFileDelete struct {
	fileservice.FileService
	fail atomic.Bool
}

func (fs *failMetricFileDelete) Delete(ctx context.Context, files ...string) error {
	if fs.fail.Swap(false) {
		return moerr.NewInternalError(ctx, "file deletion failed")
	}
	return fs.FileService.Delete(ctx, files...)
}

func TestMetricMergePreservesIncompatibleFiles(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	mock := metricDeliveryDB(t)
	tbl := mometric.SingleMetricTable
	valid := "m," + sampleTime + ",1,cn-1,CN,sys,0,\n"
	goodID := uuid.New()
	group := make([]*FileMeta, 0, 4)
	for _, fixture := range []struct{ file, payload string }{
		{"etl:sys/logs/legacy.csv", valid},
		{table.MetricBatchPath("etl:sys/logs/truncated.csv", uuid.New()), valid + "m,missing fields\n"},
		{table.MetricBatchPath("etl:sys/logs/invalid.csv", uuid.New()), "m,bad-timestamp,1,cn-1,CN,sys,0,\n"},
		{table.MetricBatchPath("etl:sys/logs/good.csv", goodID), valid},
	} {
		size := int64(len(fixture.payload))
		require.NoError(t, fs.Write(ctx, fileservice.IOVector{
			FilePath: fixture.file, Entries: []fileservice.IOEntry{{Size: size, Data: []byte(fixture.payload)}},
		}))
		group = append(group, &FileMeta{FilePath: fixture.file, FileSize: size})
	}
	merge, err := NewMerge(ctx, "", WithTable(tbl), WithFileService(fs))
	require.NoError(t, err)
	t.Cleanup(merge.Stop)
	expectMetricLoad(mock, tbl, goodID, valid, true)
	expectMetricCommit(mock)
	require.NoError(t, merge.doMergeFiles(ctx, group))
	for _, file := range group[:3] {
		_, err := fs.StatFile(ctx, file.FilePath)
		require.NoError(t, err)
	}
	_, err = fs.StatFile(ctx, group[3].FilePath)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))

	// Once the original batch is verified undelivered, an operator can
	// publish the same bytes under a stable, versioned filename.
	recoveryID := uuid.New()
	recovered := table.MetricBatchPath(group[0].FilePath, recoveryID)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: recovered, Entries: []fileservice.IOEntry{{Size: int64(len(valid)), Data: []byte(valid)}},
	}))
	expectMetricLoad(mock, tbl, recoveryID, valid, true)
	expectMetricCommit(mock)
	require.NoError(t, merge.doMergeFiles(ctx, []*FileMeta{{FilePath: recovered, FileSize: int64(len(valid))}}))
	_, err = fs.StatFile(ctx, recovered)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	_, err = fs.StatFile(ctx, group[0].FilePath)
	require.NoError(t, err)
}
