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

package cdc

// indexSyncSinker is to update HNSW index via CDC.
// It will read CDC changes and create JSON as input to function hnsw_cdc_update(dbname, tablename, vector_dimenion, json)
// You can refer the JSON format from vectorindex.VectorIndexCdc
// Single batch will split into multiple json objects and each json has maximum 8192 records (see vectorindex.VectorIndexCdc).
// Transaction function ExecTxn() used to make sure single batch (multiple json objects) can be updated in single transaction.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"go.uber.org/zap"
)

var _ Sinker = &indexSyncSinker{}

var sqlExecutorFactory = _sqlExecutorFactory

type indexSyncSinker struct {
	cnUUID           string
	accountId        uint64
	taskId           string
	dbTblInfo        *DbTableInfo
	watermarkUpdater *CDCWatermarkUpdater
	ar               *ActiveRoutine
	tableDef         *plan.TableDef
	err              atomic.Value
	sqlWriters       []IndexSqlWriter
	sqlBufSendCh     chan []byte
	exec             executor.SQLExecutor
	rowdata          []any
	rowdelete        []any
}

type IndexEntry struct {
	algo    string
	indexes []*plan.IndexDef
}

func _sqlExecutorFactory(cnUUID string) (executor.SQLExecutor, error) {
	// sql executor
	v, ok := runtime.ServiceRuntime(cnUUID).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		//os.Stderr.WriteString(fmt.Sprintf("sql executor create failed. cnUUID = %s\n", cnUUID))
		return nil, moerr.NewNotSupportedNoCtx("no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)
	return exec, nil
}

var NewIndexSyncSinker = func(
	cnUUID string,
	sinkUri UriInfo,
	accountId uint64,
	taskId string,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
	ar *ActiveRoutine,
	maxSqlLength uint64,
	sendSqlTimeout string,
) (Sinker, error) {

	// sql executor
	exec, err := sqlExecutorFactory(cnUUID)
	if err != nil {
		return nil, err
	}

	sqlwriters := make([]IndexSqlWriter, 0, 5)
	indexmap := make(map[string]*IndexEntry)

	for _, idx := range tableDef.Indexes {
		if idx.TableExist && (catalog.IsHnswIndexAlgo(idx.IndexAlgo) || catalog.IsIvfIndexAlgo(idx.IndexAlgo) || catalog.IsFullTextIndexAlgo(idx.IndexAlgo)) {
			key := idx.IndexName
			sidx, ok := indexmap[key]
			if ok {
				sidx.indexes = append(sidx.indexes, idx)
			} else {
				ie := &IndexEntry{algo: idx.IndexAlgo, indexes: make([]*plan.IndexDef, 0, 3)}
				ie.indexes = append(ie.indexes, idx)
				indexmap[key] = ie
			}
		}

	}

	for _, ie := range indexmap {
		sqlwriter, err := NewIndexSqlWriter(ie.algo, dbTblInfo, tableDef, ie.indexes)
		if err != nil {
			return nil, err
		}
		sqlwriters = append(sqlwriters, sqlwriter)
		os.Stderr.WriteString(fmt.Sprintf("sql writer %T\n", sqlwriter))
	}

	s := &indexSyncSinker{
		cnUUID:           cnUUID,
		dbTblInfo:        dbTblInfo,
		accountId:        accountId,
		taskId:           taskId,
		watermarkUpdater: watermarkUpdater,
		ar:               ar,
		tableDef:         tableDef,
		sqlBufSendCh:     make(chan []byte),
		err:              atomic.Value{},
		exec:             exec,
		sqlWriters:       sqlwriters,
		rowdata:          make([]any, len(tableDef.Cols)),
		rowdelete:        make([]any, 1), // delete row only have one column pk
	}
	return s, nil

}

func (s *indexSyncSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	logutil.Infof("cdc indexSyncSinker(%v).Run: start", s.dbTblInfo)
	defer func() {
		logutil.Infof("cdc indexSyncSinker(%v).Run: end", s.dbTblInfo)
	}()

	closed := false
	for !closed {

		txnbegin := false
		// make sure there is a BEGIN before start transaction
		for !txnbegin {

			select {
			case <-ctx.Done():
				return
			case sqlBuf, ok := <-s.sqlBufSendCh:
				if !ok {
					closed = true
					return
				}
				if bytes.Equal(sqlBuf, begin) {
					txnbegin = true
				} else if bytes.Equal(sqlBuf, commit) {
					// pass
				} else if bytes.Equal(sqlBuf, rollback) {
					// pass
				} else if bytes.Equal(sqlBuf, dummy) {
					// pass
				} else {
					func() {
						newctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
						defer cancel()
						//os.Stderr.WriteString("Wait for BEGIN but sql. execute anyway\n")
						opts := executor.Options{}
						res, err := s.exec.Exec(newctx, string(sqlBuf), opts)
						if err != nil {
							logutil.Errorf("cdc indexSyncSinker(%v) send sql failed, err: %v, sql: %s", s.dbTblInfo, err, sqlBuf[sqlBufReserved:])
							os.Stderr.WriteString(fmt.Sprintf("sql  executor run failed. %s\n", string(sqlBuf)))
							os.Stderr.WriteString(fmt.Sprintf("err :%v\n", err))
							s.SetError(err)
						}
						res.Close()
					}()
				}
			}
		}

		func() {
			newctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
			defer cancel()
			opts := executor.Options{}
			err := s.exec.ExecTxn(newctx,
				func(exec executor.TxnExecutor) error {

					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case sqlBuf, ok := <-s.sqlBufSendCh:
							if !ok {
								// channel closed
								closed = true
								return nil
							}

							if bytes.Equal(sqlBuf, dummy) {

							} else if bytes.Equal(sqlBuf, begin) {
								// BEGIN
							} else if bytes.Equal(sqlBuf, commit) {
								// COMMIT - end of data
								return nil
							} else if bytes.Equal(sqlBuf, rollback) {
								// ROLLBACK
								return moerr.NewQueryInterrupted(ctx)
							} else {
								res, err := exec.Exec(string(sqlBuf), opts.StatementOption())
								if err != nil {
									logutil.Errorf("cdc indexSyncSinker(%v) send sql failed, err: %v, sql: %s", s.dbTblInfo, err, sqlBuf[sqlBufReserved:])
									os.Stderr.WriteString(fmt.Sprintf("sql  executor run failed. %s\n", string(sqlBuf)))
									os.Stderr.WriteString(fmt.Sprintf("err :%v\n", err))
									return err
								}
								res.Close()
							}
						}
					}

				},
				opts)
			if err != nil {
				moe, ok := err.(*moerr.Error)
				if ok {
					if moe.ErrorCode() == moerr.ErrQueryInterrupted {
						// skip rollback error
						//os.Stderr.WriteString("error QueryInterrupted....rollback\n")
						logutil.Errorf("cdc indexSyncSinker(%v) parent rollback", s.dbTblInfo)
					} else {
						s.SetError(err)
					}
				} else if uw, ok := err.(interface{ Unwrap() []error }); ok {
					rollbackfound := false
					for _, e := range uw.Unwrap() {
						//os.Stderr.WriteString(fmt.Sprintf("errors... %v\n", e))
						moe, ok := e.(*moerr.Error)
						if ok && moe.ErrorCode() == moerr.ErrQueryInterrupted {
							rollbackfound = true
						}
					}

					//os.Stderr.WriteString(fmt.Sprintf("rollback found %v\n", rollbackfound))
					if !rollbackfound {
						s.SetError(err)
					}
				} else {
					s.SetError(err)
				}
			}
		}()
	}
}

func (s *indexSyncSinker) Sink(ctx context.Context, data *DecoderOutput) {
	// TODO: IMPORTANT: check the indexdef here so that Add/Drop index can be reflected here

	key := WatermarkKey{
		AccountId: s.accountId,
		TaskId:    s.taskId,
		DBName:    s.dbTblInfo.SourceDbName,
		TableName: s.dbTblInfo.SourceTblName,
	}
	watermark, err := s.watermarkUpdater.GetFromCache(ctx, &key)
	if err != nil {
		logutil.Error(
			"CDC-MySQLSinker-GetWatermarkFailed",
			zap.String("info", s.dbTblInfo.String()),
			zap.String("key", key.String()),
			zap.Error(err),
		)
		return
	}

	if data.toTs.LE(&watermark) {
		logutil.Errorf("cdc indexSyncSinker(%v): unexpected watermark: %s, current watermark: %s",
			s.dbTblInfo, data.toTs.ToString(), watermark.ToString())
		return
	}

	if data.noMoreData {
		// complete sql statement
		err := s.flushCdc()
		if err != nil {
			s.SetError(err)
		}
		return
	}

	start := time.Now()
	defer func() {
		v2.CdcSinkDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if data.outputTyp == OutputTypeSnapshot {
		//os.Stderr.WriteString(fmt.Sprintf("sinkSnapshot batlen %d\n", batchRowCount(data.checkpointBat)))
		s.sinkSnapshot(ctx, data.checkpointBat)
	} else if data.outputTyp == OutputTypeTail {
		//os.Stderr.WriteString(fmt.Sprintf("sinkTail insertBat %d, deletBat = %d\n", data.insertAtmBatch.RowCount(), data.deleteAtmBatch.RowCount()))
		s.sinkTail(ctx, data.insertAtmBatch, data.deleteAtmBatch)
	} else {
		s.SetError(moerr.NewInternalError(ctx, fmt.Sprintf("cdc indexSyncSinker unexpected output type: %v", data.outputTyp)))
	}
}

func (s *indexSyncSinker) SendBegin() {
	s.sqlBufSendCh <- begin
}

func (s *indexSyncSinker) SendCommit() {
	s.sqlBufSendCh <- commit
}

func (s *indexSyncSinker) SendRollback() {
	s.sqlBufSendCh <- rollback
}

func (s *indexSyncSinker) SendDummy() {
	s.sqlBufSendCh <- dummy
}

func (s *indexSyncSinker) Error() error {
	if ptr := s.err.Load(); ptr != nil {
		errPtr := ptr.(*error)
		if errPtr != nil {
			if moErr, ok := (*errPtr).(*moerr.Error); !ok {
				return moerr.ConvertGoError(context.Background(), *errPtr)
			} else {
				if moErr == nil {
					return nil
				}
				return moErr
			}
		}
	}
	return nil
}

func (s *indexSyncSinker) SetError(err error) {
	s.err.Store(&err)
}

func (s *indexSyncSinker) ClearError() {
	var err *moerr.Error
	s.SetError(err)
}

func (s *indexSyncSinker) Reset() {
	for _, writer := range s.sqlWriters {
		writer.Reset()
	}
	s.err = atomic.Value{}
}

func (s *indexSyncSinker) Close() {
	// stop Run goroutine
	close(s.sqlBufSendCh)
}

func (s *indexSyncSinker) sinkSnapshot(ctx context.Context, bat *batch.Batch) {
	var err error

	for i := 0; i < batchRowCount(bat); i++ {
		if err = extractRowFromEveryVector(ctx, bat, i, s.rowdata); err != nil {
			s.SetError(err)
			return
		}

		for _, writer := range s.sqlWriters {
			err = writer.Upsert(ctx, s.rowdata)
			if err != nil {
				s.SetError(err)
				return
			}

			if writer.Full() {
				err = s.sendSql(writer)
				if err != nil {
					s.SetError(err)
					return
				}
			}
		}
	}
}

// upsertBatch and deleteBatch is sorted by ts
// for the same ts, delete first, then upsert
func (s *indexSyncSinker) sinkTail(ctx context.Context, upsertBatch, deleteBatch *AtomicBatch) {
	var err error

	upsertIter := upsertBatch.GetRowIterator().(*atomicBatchRowIter)
	deleteIter := deleteBatch.GetRowIterator().(*atomicBatchRowIter)
	defer func() {
		upsertIter.Close()
		deleteIter.Close()
	}()

	// output sql until one iterator reach the end
	upsertIterHasNext, deleteIterHasNext := upsertIter.Next(), deleteIter.Next()
	for upsertIterHasNext && deleteIterHasNext {
		upsertItem, deleteItem := upsertIter.Item(), deleteIter.Item()
		// compare ts, ignore pk
		if upsertItem.Ts.LT(&deleteItem.Ts) {
			if err = s.sinkUpsert(ctx, upsertIter); err != nil {
				s.SetError(err)
				return
			}
			// get next item
			upsertIterHasNext = upsertIter.Next()
		} else {
			if err = s.sinkDelete(ctx, deleteIter); err != nil {
				s.SetError(err)
				return
			}
			// get next item
			deleteIterHasNext = deleteIter.Next()
		}
	}

	// output the rest of upsert iterator
	for upsertIterHasNext {
		if err = s.sinkUpsert(ctx, upsertIter); err != nil {
			s.SetError(err)
			return
		}
		// get next item
		upsertIterHasNext = upsertIter.Next()
	}

	// output the rest of delete iterator
	for deleteIterHasNext {
		if err = s.sinkDelete(ctx, deleteIter); err != nil {
			s.SetError(err)
			return
		}
		// get next item
		deleteIterHasNext = deleteIter.Next()
	}
	s.flushCdc()
}

func (s *indexSyncSinker) sinkUpsert(ctx context.Context, upsertIter *atomicBatchRowIter) (err error) {

	// get row from the batch
	if err = upsertIter.Row(ctx, s.rowdata); err != nil {
		return err
	}

	for _, writer := range s.sqlWriters {
		if !writer.CheckLastOp(vectorindex.CDC_UPSERT) {
			// last op is not UPSERT, sendSql first
			// send SQL
			err = s.sendSql(writer)
			if err != nil {
				return err
			}

		}

		writer.Upsert(ctx, s.rowdata)

		if writer.Full() {
			// send SQL
			err = s.sendSql(writer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *indexSyncSinker) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter) (err error) {

	// get row from the batch
	if err = deleteIter.Row(ctx, s.rowdelete); err != nil {
		return err
	}

	for _, writer := range s.sqlWriters {
		if !writer.CheckLastOp(vectorindex.CDC_DELETE) {
			// last op is not DELETE, sendSql first
			// send SQL
			err = s.sendSql(writer)
			if err != nil {
				return err
			}

		}

		writer.Delete(ctx, s.rowdelete)

		if writer.Full() {
			// send SQL
			err = s.sendSql(writer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *indexSyncSinker) flushCdc() (err error) {
	for _, writer := range s.sqlWriters {
		err = s.sendSql(writer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *indexSyncSinker) sendSql(writer IndexSqlWriter) error {
	if writer.Empty() {
		return nil
	}

	// generate sql from cdc
	sql, err := writer.ToSql()
	if err != nil {
		return err
	}

	s.sqlBufSendCh <- sql
	os.Stderr.WriteString(string(sql))
	os.Stderr.WriteString("\n")

	// reset
	writer.Reset()

	return nil
}
