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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

var _ Sinker = &hnswSyncSinker[float32]{}

var sqlExecutorFactory = _sqlExecutorFactory

type hnswSyncSinker[T types.RealNumbers] struct {
	cnUUID           string
	dbTblInfo        *DbTableInfo
	watermarkUpdater IWatermarkUpdater
	ar               *ActiveRoutine
	tableDef         *plan.TableDef
	cdc              *vectorindex.VectorIndexCdc[T]
	param            vectorindex.HnswCdcParam
	err              atomic.Value

	sqlBufSendCh chan []byte
	pkcol        int32
	veccol       int32
	exec         executor.SQLExecutor
}

func _sqlExecutorFactory(cnUUID string) (executor.SQLExecutor, error) {
	// sql executor
	v, ok := runtime.ServiceRuntime(cnUUID).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		os.Stderr.WriteString(fmt.Sprintf("sql executor create failed. cnUUID = %s\n", cnUUID))
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

	pkColName := tableDef.Pkey.PkeyColName

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

	pkcol := tableDef.Name2ColIndex[pkColName]
	veccol := tableDef.Name2ColIndex[indexdef.Parts[0]]

	if tableDef.Cols[pkcol].Typ.Id != int32(types.T_int64) {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table primary key is not int64")

	}

	// get param and index table name
	paramstr := indexdef.IndexAlgoParams
	var meta, storage string
	for _, idx := range hnswindexes {
		if idx.IndexAlgoTableType == catalog.Hnsw_TblType_Metadata {
			meta = idx.IndexTableName
		}
		if idx.IndexAlgoTableType == catalog.Hnsw_TblType_Storage {
			storage = idx.IndexTableName
		}
	}

	if len(meta) == 0 || len(storage) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table either meta or storage hidden index table not exist")
	}

	var hnswparam vectorindex.HnswParam
	if len(paramstr) > 0 {
		err := json.Unmarshal([]byte(paramstr), &hnswparam)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtx("hnsw sync sinker. failed to convert hnsw param json")
		}
	}

	param := vectorindex.HnswCdcParam{
		MetaTbl:   meta,
		IndexTbl:  storage,
		DbName:    dbTblInfo.SinkDbName,
		Table:     dbTblInfo.SinkTblName,
		Params:    hnswparam,
		Dimension: tableDef.Cols[veccol].Typ.Width,
	}

	// create sinker
	var maxAllowedPacket uint64
	maxAllowedPacket = min(maxAllowedPacket, maxSqlLength)

	if tableDef.Cols[veccol].Typ.Id == int32(types.T_array_float32) {
		s := &hnswSyncSinker[float32]{
			cnUUID:           cnUUID,
			dbTblInfo:        dbTblInfo,
			watermarkUpdater: watermarkUpdater,
			ar:               ar,
			tableDef:         tableDef,
			cdc:              vectorindex.NewVectorIndexCdc[float32](),
			sqlBufSendCh:     make(chan []byte),
			pkcol:            pkcol,
			veccol:           veccol,
			err:              atomic.Value{},
			param:            param,
			exec:             exec,
		}
		logutil.Infof("cdc hnswSyncSinker(%v) maxAllowedPacket = %d", s.dbTblInfo, maxAllowedPacket)
		return s, nil

	} else if tableDef.Cols[veccol].Typ.Id == int32(types.T_array_float64) {
		s := &hnswSyncSinker[float64]{
			cnUUID:           cnUUID,
			dbTblInfo:        dbTblInfo,
			watermarkUpdater: watermarkUpdater,
			ar:               ar,
			tableDef:         tableDef,
			cdc:              vectorindex.NewVectorIndexCdc[float64](),
			sqlBufSendCh:     make(chan []byte),
			pkcol:            pkcol,
			veccol:           veccol,
			err:              atomic.Value{},
			param:            param,
			exec:             exec,
		}
		logutil.Infof("cdc hnswSyncSinker(%v) maxAllowedPacket = %d", s.dbTblInfo, maxAllowedPacket)
		return s, nil

	} else {
		return nil, moerr.NewInternalErrorNoCtx("hnsw index table part is not []float32 or []float64")
	}

}

func (s *hnswSyncSinker[T]) Run(ctx context.Context, ar *ActiveRoutine) {
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
					os.Stderr.WriteString("Run: wait for begin but commit\n")
				} else if bytes.Equal(sqlBuf, rollback) {
					os.Stderr.WriteString("Run: wait for begin but rollback\n")
				} else if bytes.Equal(sqlBuf, dummy) {
					// pass
				} else {
					func() {
						newctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
						defer cancel()
						//os.Stderr.WriteString(fmt.Sprintf("Wait for BEGIN.... %s\n", string(sqlBuf)))
						os.Stderr.WriteString(fmt.Sprintf("Wait for BEGIN but sql. execute anyway\n"))
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
				//os.Stderr.WriteString(fmt.Sprintf("error from txn %v, ok %v\n", err, ok))
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

func (s *hnswSyncSinker[T]) Sink(ctx context.Context, data *DecoderOutput) {
	watermark := s.watermarkUpdater.GetFromMem(s.dbTblInfo.SourceDbName, s.dbTblInfo.SourceTblName)
	if data.toTs.LE(&watermark) {
		os.Stderr.WriteString("unexpected watermark\n")
		logutil.Errorf("cdc hnswSyncSinker(%v): unexpected watermark: %s, current watermark: %s",
			s.dbTblInfo, data.toTs.ToString(), watermark.ToString())
		return
	}
	s.cdc.Start = data.fromTs.ToString()
	s.cdc.End = data.toTs.ToString()

	if data.noMoreData {

		os.Stderr.WriteString("no more data\n")
		if data.checkpointBat != nil {
			os.Stderr.WriteString(fmt.Sprintf("no more data sinkSnapshot batlen %d\n", batchRowCount(data.checkpointBat)))
		}
		if data.insertAtmBatch != nil {
			os.Stderr.WriteString(fmt.Sprintf("no more data sinkTail insertBat %d, deletBat = %d\n", data.insertAtmBatch.RowCount(), data.deleteAtmBatch.RowCount()))
		}
		// complete sql statement
		err := s.sendSql()
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
		os.Stderr.WriteString(fmt.Sprintf("sinkSnapshot batlen %d\n", batchRowCount(data.checkpointBat)))
		s.sinkSnapshot(ctx, data.checkpointBat)
	} else if data.outputTyp == OutputTypeTail {
		os.Stderr.WriteString(fmt.Sprintf("sinkTail insertBat %d, deletBat = %d\n", data.insertAtmBatch.RowCount(), data.deleteAtmBatch.RowCount()))
		s.sinkTail(ctx, data.insertAtmBatch, data.deleteAtmBatch)
	} else {
		s.SetError(moerr.NewInternalError(ctx, fmt.Sprintf("cdc hnswSyncSinker unexpected output type: %v", data.outputTyp)))
	}
}

func (s *hnswSyncSinker[T]) SendBegin() {
	s.sqlBufSendCh <- begin
}

func (s *hnswSyncSinker[T]) SendCommit() {
	s.sqlBufSendCh <- commit
}

func (s *hnswSyncSinker[T]) SendRollback() {
	s.sqlBufSendCh <- rollback
}

func (s *hnswSyncSinker[T]) SendDummy() {
	s.sqlBufSendCh <- dummy
}

func (s *hnswSyncSinker[T]) Error() error {
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

func (s *hnswSyncSinker[T]) SetError(err error) {
	s.err.Store(&err)
}

func (s *hnswSyncSinker[T]) ClearError() {
	var err *moerr.Error
	s.SetError(err)
}

func (s *hnswSyncSinker[T]) Reset() {
	s.cdc.Reset()
	s.err = atomic.Value{}
}

func (s *hnswSyncSinker[T]) Close() {
	// stop Run goroutine
	close(s.sqlBufSendCh)
}

func (s *hnswSyncSinker[T]) sinkSnapshot(ctx context.Context, bat *batch.Batch) {
	pkvec := bat.Vecs[s.pkcol]
	vecvec := bat.Vecs[s.veccol]
	for i := 0; i < batchRowCount(bat); i++ {
		pk := vector.GetFixedAtWithTypeCheck[int64](pkvec, i)

		// check null
		if vecvec.IsNull(uint64(i)) {
			// nil vector means delete
			s.cdc.Delete(pk)
		} else {
			v := vector.GetArrayAt[T](vecvec, i)

			s.cdc.Upsert(pk, v)
		}

		// check full
		if s.cdc.Full() {
			// send sql
			err := s.sendSql()
			if err != nil {
				s.SetError(err)
				return
			}
		}
	}
}

// upsertBatch and deleteBatch is sorted by ts
// for the same ts, delete first, then upsert
func (s *hnswSyncSinker[T]) sinkTail(ctx context.Context, upsertBatch, deleteBatch *AtomicBatch) {
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

func (s *hnswSyncSinker[T]) sinkUpsert(ctx context.Context, upsertIter *atomicBatchRowIter) (err error) {

	// get row from the batch
	row := upsertIter.Item()
	bat := row.Src

	pkvec := bat.Vecs[s.pkcol]
	vecvec := bat.Vecs[s.veccol]
	pk := vector.GetFixedAtWithTypeCheck[int64](pkvec, row.Offset)

	// check null
	if vecvec.IsNull(uint64(row.Offset)) {
		// nil vector means delete
		s.cdc.Delete(pk)
	} else {
		v := vector.GetArrayAt[T](vecvec, row.Offset)

		s.cdc.Upsert(pk, v)
	}

	if s.cdc.Full() {
		// send SQL
		return s.sendSql()
	}

	return nil
}

func (s *hnswSyncSinker[T]) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter) (err error) {

	// get row from the batch
	row := deleteIter.Item()
	bat := row.Src

	pkvec := bat.Vecs[s.pkcol]
	pk := vector.GetFixedAtWithTypeCheck[int64](pkvec, row.Offset)

	s.cdc.Delete(pk)
	if s.cdc.Full() {
		return s.sendSql()
	}

	return nil
}

func (s *hnswSyncSinker[T]) flushCdc() (err error) {
	return s.sendSql()
}

func (s *hnswSyncSinker[T]) sendSql() error {
	if s.cdc.Empty() {
		return nil
	}

	// generate sql from cdc
	js, err := s.cdc.ToJson()
	if err != nil {
		return err
	}
	// pad extra space at the front and send SQL
	padding := strings.Repeat(" ", sqlBufReserved)
	sql := fmt.Sprintf("%s SELECT hnsw_cdc_update('%s', '%s', %d, '%s');", padding, s.param.DbName, s.param.Table, s.param.Dimension, js)

	s.sqlBufSendCh <- []byte(sql)

	// reset
	s.cdc.Reset()

	return nil
}
