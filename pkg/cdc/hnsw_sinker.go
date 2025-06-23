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

// hnswSyncSinker is to update HNSW index via CDC.
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
)

var _ Sinker = &hnswSyncSinker{}

var sqlExecutorFactory = _sqlExecutorFactory

type hnswSyncSinker struct {
	cnUUID           string
	dbTblInfo        *DbTableInfo
	watermarkUpdater IWatermarkUpdater
	ar               *ActiveRoutine
	tableDef         *plan.TableDef
	err              atomic.Value
	sqlWriters       []IndexSqlWriter
	sqlBufSendCh     chan []byte
	exec             executor.SQLExecutor
	rowdata          []any
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

var NewHnswSyncSinker = func(
	cnUUID string,
	sinkUri UriInfo,
	dbTblInfo *DbTableInfo,
	watermarkUpdater IWatermarkUpdater,
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

	// check the tabledef and indexdef
	if len(tableDef.Pkey.Names) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table only have one primary key")
	}

	hnswindexes := make([]*plan.IndexDef, 0, 2)

	for _, idx := range tableDef.Indexes {
		if idx.TableExist && catalog.IsHnswIndexAlgo(idx.IndexAlgo) {
			hnswindexes = append(hnswindexes, idx)
		}

	}

	if len(hnswindexes) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table without index definition")
	}

	indexdef := hnswindexes[0]

	if len(indexdef.Parts) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table only have one vector part")
	}

	sqlwriter, err := NewIndexSqlWriter("hnsw", dbTblInfo, tableDef, hnswindexes)
	if err != nil {
		return nil, err
	}

	// create sinker
	var maxAllowedPacket uint64
	maxAllowedPacket = min(maxAllowedPacket, maxSqlLength)

	s := &hnswSyncSinker{
		cnUUID:           cnUUID,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
		ar:               ar,
		tableDef:         tableDef,
		sqlBufSendCh:     make(chan []byte),
		err:              atomic.Value{},
		exec:             exec,
		sqlWriters:       []IndexSqlWriter{sqlwriter},
		rowdata:          make([]any, len(tableDef.Cols)),
	}
	logutil.Infof("cdc hnswSyncSinker(%v) maxAllowedPacket = %d", s.dbTblInfo, maxAllowedPacket)
	return s, nil

}

func (s *hnswSyncSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	logutil.Infof("cdc hnswSyncSinker(%v).Run: start", s.dbTblInfo)
	defer func() {
		logutil.Infof("cdc hnswSyncSinker(%v).Run: end", s.dbTblInfo)
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
							logutil.Errorf("cdc hnswSyncSinker(%v) send sql failed, err: %v, sql: %s", s.dbTblInfo, err, sqlBuf[sqlBufReserved:])
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
									logutil.Errorf("cdc hnswSyncSinker(%v) send sql failed, err: %v, sql: %s", s.dbTblInfo, err, sqlBuf[sqlBufReserved:])
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
						logutil.Errorf("cdc hnswSyncSinker(%v) parent rollback", s.dbTblInfo)
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

func (s *hnswSyncSinker) Sink(ctx context.Context, data *DecoderOutput) {
	watermark := s.watermarkUpdater.GetFromMem(s.dbTblInfo.SourceDbName, s.dbTblInfo.SourceTblName)
	if data.toTs.LE(&watermark) {
		logutil.Errorf("cdc hnswSyncSinker(%v): unexpected watermark: %s, current watermark: %s",
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
		s.SetError(moerr.NewInternalError(ctx, fmt.Sprintf("cdc hnswSyncSinker unexpected output type: %v", data.outputTyp)))
	}
}

func (s *hnswSyncSinker) SendBegin() {
	s.sqlBufSendCh <- begin
}

func (s *hnswSyncSinker) SendCommit() {
	s.sqlBufSendCh <- commit
}

func (s *hnswSyncSinker) SendRollback() {
	s.sqlBufSendCh <- rollback
}

func (s *hnswSyncSinker) SendDummy() {
	s.sqlBufSendCh <- dummy
}

func (s *hnswSyncSinker) Error() error {
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

func (s *hnswSyncSinker) SetError(err error) {
	s.err.Store(&err)
}

func (s *hnswSyncSinker) ClearError() {
	var err *moerr.Error
	s.SetError(err)
}

func (s *hnswSyncSinker) Reset() {
	for _, writer := range s.sqlWriters {
		writer.Reset()
	}
	s.err = atomic.Value{}
}

func (s *hnswSyncSinker) Close() {
	// stop Run goroutine
	close(s.sqlBufSendCh)
}

func (s *hnswSyncSinker) sinkSnapshot(ctx context.Context, bat *batch.Batch) {
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
func (s *hnswSyncSinker) sinkTail(ctx context.Context, upsertBatch, deleteBatch *AtomicBatch) {
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

func (s *hnswSyncSinker) sinkUpsert(ctx context.Context, upsertIter *atomicBatchRowIter) (err error) {

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

func (s *hnswSyncSinker) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter) (err error) {

	// get row from the batch
	if err = deleteIter.Row(ctx, s.rowdata); err != nil {
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

		writer.Delete(ctx, s.rowdata)

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

func (s *hnswSyncSinker) flushCdc() (err error) {
	for _, writer := range s.sqlWriters {
		err = s.sendSql(writer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *hnswSyncSinker) sendSql(writer IndexSqlWriter) error {
	if writer.Empty() {
		return nil
	}

	// generate sql from cdc
	sql, err := writer.ToSql()
	if err != nil {
		return err
	}

	s.sqlBufSendCh <- sql

	// reset
	writer.Reset()

	return nil
}
