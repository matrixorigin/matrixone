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
	"regexp"
	"sync/atomic"
	"testing"
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
	t.Cleanup(func() {
		dbholder.GetOrInitDBConn, dbholder.DBConnErrCount = provider, backoff
		require.NoError(t, mock.ExpectationsWereMet())
	})
	return mock
}

func expectMetricLoad(mock sqlmock.Sqlmock, tbl *table.Table, id uuid.UUID, csv string) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(id.String()).WillReturnResult(sqlmock.NewResult(0, 1))
	query := "LOAD DATA INLINE FORMAT='csv', DATA='" + csv + "' INTO TABLE " + tbl.GetIdentify() + " FIELDS TERMINATED BY ','"
	mock.ExpectExec(regexp.QuoteMeta(query)).WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectCommittedReceipt(mock sqlmock.Sqlmock, id uuid.UUID) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertReceipt)).WithArgs(id.String()).WillReturnError(&mysql.MySQLError{Number: moerr.ER_DUP_ENTRY})
	mock.ExpectRollback()
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

				expectMetricLoad(mock, tbl, id, fixture.csv)
				if committed {
					mock.ExpectCommit().WillReturnError(commitUnknown)
				} else {
					mock.ExpectCommit().WillReturnError(&mysql.MySQLError{Number: 1213})
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
					expectMetricLoad(mock, tbl, id, fixture.csv)
					mock.ExpectCommit()
				}
				require.NoError(t, m.doMergeFiles(ctx, []*FileMeta{{FilePath: file, FileSize: info.Size}}))
				_, err = fs.StatFile(ctx, file)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
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
	expectMetricLoad(mock, tbl, id, payload)
	mock.ExpectCommit().WillReturnError(commitUnknown)
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

func TestMetricReplaySwitchesCNAfterDeadline(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	healthy := metricDeliveryDB(t)
	getHealthy := dbholder.GetOrInitDBConn
	stalledDB, stalled, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stalledDB.Close() })

	forced := 0
	dbholder.GetOrInitDBConn = func(forceNew, randomCN bool) (*sql.DB, error) {
		if forceNew {
			require.True(t, randomCN)
			forced++
			dbholder.DBConnErrCount.Reset()
		}
		if forced == 0 {
			return stalledDB, nil
		}
		return getHealthy(forceNew, randomCN)
	}
	tbl := mometric.SingleMetricTable
	id := uuid.New()
	file := table.MetricBatchPath("etl:sys/logs/reconnect.csv", id)
	payload := "m," + sampleTime + ",1,cn-1,CN,sys,0,\n"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: file, Entries: []fileservice.IOEntry{{Size: int64(len(payload)), Data: []byte(payload)}},
	}))
	merge, err := NewMerge(ctx, "", WithTable(tbl), WithFileService(fs))
	require.NoError(t, err)
	t.Cleanup(merge.Stop)

	// A stopped task should leave the connection failure count unchanged.
	canceled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, etl.ReplayMetricBatch(canceled, tbl, []byte(payload), id), context.Canceled)
	require.True(t, dbholder.DBConnErrCount.Check())

	// Simulate the transaction deadline used by the merge path, without
	// spending ten seconds waiting for each attempt against the stalled CN.
	for i := 0; i <= dbholder.DBConnRetryThreshold; i++ {
		stalled.ExpectBegin().WillDelayFor(time.Second)
		attempt, cancel := context.WithTimeoutCause(ctx, 50*time.Millisecond, moerr.CauseWriteRowRecords)
		require.Error(t, etl.ReplayMetricBatch(attempt, tbl, []byte(payload), id))
		require.ErrorIs(t, attempt.Err(), context.DeadlineExceeded)
		cancel()
	}
	require.NoError(t, stalled.ExpectationsWereMet())
	_, err = fs.StatFile(ctx, file)
	require.NoError(t, err)
	require.Zero(t, forced)
	require.False(t, dbholder.DBConnErrCount.Check())

	expectMetricLoad(healthy, tbl, id, payload)
	healthy.ExpectCommit()
	require.NoError(t, merge.mergeMetricFile(ctx, file))
	require.Equal(t, 1, forced)
	_, err = fs.StatFile(ctx, file)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
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
	expectMetricLoad(mock, tbl, goodID, valid)
	mock.ExpectCommit()
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
	expectMetricLoad(mock, tbl, recoveryID, valid)
	mock.ExpectCommit()
	require.NoError(t, merge.doMergeFiles(ctx, []*FileMeta{{FilePath: recovered, FileSize: int64(len(valid))}}))
	_, err = fs.StatFile(ctx, recovered)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	_, err = fs.StatFile(ctx, group[0].FilePath)
	require.NoError(t, err)
}
