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

// to retrieve the index table from mo_catalog
// select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_tables where relname = "tbl" and reldatabase = "db");

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

var _ Sinker = &hnswSyncSinker[float32]{}

type HnswCdcParam struct {
	DbName   string                `json:"db"`
	Table    string                `json:"table"`
	MetaTbl  string                `json:"meta"`
	IndexTbl string                `json:"index"`
	Params   vectorindex.HnswParam `json:"params"`
}

type hnswSyncSinker[T types.RealNumbers] struct {
	mysql            Sink
	dbTblInfo        *DbTableInfo
	watermarkUpdater IWatermarkUpdater
	ar               *ActiveRoutine
	tableDef         *plan.TableDef
	cdc              *vectorindex.HnswCdc[T]
	param            HnswCdcParam
	err              atomic.Value

	sqlBufSendCh chan []byte
	// only contains user defined column types, no mo meta cols
	upsertTypes []*types.Type
	// only contains pk columns
	deleteTypes []*types.Type
	pkColNames  []string
	pkcol       int32
	veccol      int32
}

var NewHnswSyncSinker = func(
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

	sink, err := NewMysqlSink(sinkUri.User, sinkUri.Password, sinkUri.Ip, sinkUri.Port, retryTimes, retryDuration, sendSqlTimeout)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	padding := strings.Repeat(" ", sqlBufReserved)
	// use db
	err = sink.Send(ctx, ar, []byte(padding+fmt.Sprintf("use `%s`", dbTblInfo.SinkDbName)), false)
	if err != nil {
		return nil, err
	}

	// TODO: check the tabledef and indexdef
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

	param := HnswCdcParam{
		MetaTbl:  meta,
		IndexTbl: storage,
		DbName:   dbTblInfo.SinkDbName,
		Table:    dbTblInfo.SinkTblName,
		Params:   hnswparam,
	}

	// create sinker
	var maxAllowedPacket uint64
	_ = sink.(*mysqlSink).conn.QueryRow("SELECT @@max_allowed_packet").Scan(&maxAllowedPacket)
	maxAllowedPacket = min(maxAllowedPacket, maxSqlLength)

	if tableDef.Cols[veccol].Typ.Id == int32(types.T_array_float32) {
		s := &hnswSyncSinker[float32]{
			mysql:            sink,
			dbTblInfo:        dbTblInfo,
			watermarkUpdater: watermarkUpdater,
			ar:               ar,
			tableDef:         tableDef,
			cdc:              vectorindex.NewHnswCdc[float32](),
			sqlBufSendCh:     make(chan []byte),
			pkcol:            pkcol,
			veccol:           veccol,
			err:              atomic.Value{},
		}
		logutil.Infof("cdc hnswSyncSinker(%v) maxAllowedPacket = %d", s.dbTblInfo, maxAllowedPacket)
		return s, nil

	} else if tableDef.Cols[veccol].Typ.Id == int32(types.T_array_float64) {
		s := &hnswSyncSinker[float64]{
			mysql:            sink,
			dbTblInfo:        dbTblInfo,
			watermarkUpdater: watermarkUpdater,
			ar:               ar,
			tableDef:         tableDef,
			cdc:              vectorindex.NewHnswCdc[float64](),
			sqlBufSendCh:     make(chan []byte),
			pkcol:            pkcol,
			veccol:           veccol,
			err:              atomic.Value{},
			param:            param,
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

	for sqlBuf := range s.sqlBufSendCh {
		// have error, skip
		if s.err.Load() != nil {
			continue
		}

		if bytes.Equal(sqlBuf, dummy) {
			// dummy sql, do nothing
		} else if bytes.Equal(sqlBuf, begin) {
			if err := s.mysql.SendBegin(ctx); err != nil {
				logutil.Errorf("cdc hnswSyncSinker(%v) SendBegin, err: %v", s.dbTblInfo, err)
				// record error
				s.err.Store(err)
			}
		} else if bytes.Equal(sqlBuf, commit) {
			if err := s.mysql.SendCommit(ctx); err != nil {
				logutil.Errorf("cdc hnswSyncSinker(%v) SendCommit, err: %v", s.dbTblInfo, err)
				// record error
				s.err.Store(err)
			}
		} else if bytes.Equal(sqlBuf, rollback) {
			if err := s.mysql.SendRollback(ctx); err != nil {
				logutil.Errorf("cdc hnswSyncSinker(%v) SendRollback, err: %v", s.dbTblInfo, err)
				// record error
				s.err.Store(err)
			}
		} else {
			if err := s.mysql.Send(ctx, ar, sqlBuf, true); err != nil {
				logutil.Errorf("cdc hnswSyncSinker(%v) send sql failed, err: %v, sql: %s", s.dbTblInfo, err, sqlBuf[sqlBufReserved:])
				// record error
				s.err.Store(err)
			}
		}
	}
}

func (s *hnswSyncSinker[T]) Sink(ctx context.Context, data *DecoderOutput) {
	watermark := s.watermarkUpdater.GetFromMem(s.dbTblInfo.SourceDbName, s.dbTblInfo.SourceTblName)
	if data.toTs.LE(&watermark) {
		logutil.Errorf("cdc hnswSyncSinker(%v): unexpected watermark: %s, current watermark: %s",
			s.dbTblInfo, data.toTs.ToString(), watermark.ToString())
		return
	}
	s.cdc.Start = data.fromTs.ToString()
	s.cdc.End = data.toTs.ToString()

	if data.noMoreData {
		// complete sql statement
		err := s.sendSql()
		if err != nil {
			s.err.Store(err)
		}
		return
	}

	start := time.Now()
	defer func() {
		v2.CdcSinkDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if data.outputTyp == OutputTypeSnapshot {
		s.sinkSnapshot(ctx, data.checkpointBat)
	} else if data.outputTyp == OutputTypeTail {
		s.sinkTail(ctx, data.insertAtmBatch, data.deleteAtmBatch)
	} else {
		s.err.Store(moerr.NewInternalError(ctx, fmt.Sprintf("cdc hnswSyncSinker unexpected output type: %v", data.outputTyp)))
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
	if val := s.err.Load(); val == nil {
		return nil
	} else {
		return val.(error)
	}
}

func (s *hnswSyncSinker[T]) Reset() {
	s.cdc.Reset()
	s.err = atomic.Value{}
}

func (s *hnswSyncSinker[T]) Close() {
	// stop Run goroutine
	close(s.sqlBufSendCh)
	s.mysql.Close()
}

func (s *hnswSyncSinker[T]) sinkSnapshot(ctx context.Context, bat *batch.Batch) {
	pkvec := bat.Vecs[s.pkcol]
	vecvec := bat.Vecs[s.veccol]
	for i := 0; i < batchRowCount(bat); i++ {
		pk := vector.GetFixedAtWithTypeCheck[int64](pkvec, i)
		v := vector.GetArrayAt[T](vecvec, i)
		// TODO: check null

		s.cdc.Upsert(pk, v)

		// check full
		if s.cdc.Full() {
			// send sql
			err := s.sendSql()
			if err != nil {
				s.err.Store(err)
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
				s.err.Store(err)
				return
			}
			// get next item
			upsertIterHasNext = upsertIter.Next()
		} else {
			if err = s.sinkDelete(ctx, deleteIter); err != nil {
				s.err.Store(err)
				return
			}
			// get next item
			deleteIterHasNext = deleteIter.Next()
		}
	}

	// output the rest of upsert iterator
	for upsertIterHasNext {
		if err = s.sinkUpsert(ctx, upsertIter); err != nil {
			s.err.Store(err)
			return
		}
		// get next item
		upsertIterHasNext = upsertIter.Next()
	}

	// output the rest of delete iterator
	for deleteIterHasNext {
		if err = s.sinkDelete(ctx, deleteIter); err != nil {
			s.err.Store(err)
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
	if err != nil {
		return err
	}

	pkvec := bat.Vecs[s.pkcol]
	vecvec := bat.Vecs[s.veccol]
	pk := vector.GetFixedAtWithTypeCheck[int64](pkvec, row.Offset)
	v := vector.GetArrayAt[T](vecvec, row.Offset)

	s.cdc.Upsert(pk, v)

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
	if err != nil {
		return err
	}
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
	sql := fmt.Sprintf("%s SELECT hnsw_cdc_update('%s', '%s', '%s');", padding, "db", "table", js)

	s.sqlBufSendCh <- []byte(sql)

	// reset
	s.cdc.Reset()

	return nil
}
