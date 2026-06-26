// Copyright 2025 Matrix Origin
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

package disttae

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

var _ engine.Reader = new(TableMetaReader)

const (
	dataMetaState = iota
	tombstoneMetaState
	endState
)

type cloneObjectSource int

const (
	cloneObjectFromCommittedState cloneObjectSource = iota
	cloneObjectFromTxnWorkspace
)

type TableMetaReader struct {
	table     *txnTable
	fs        fileservice.FileService
	snapshot  types.TS
	txnOffset int
	state     int
	//tblDef   *plan.TableDef
	pState *logtailreplay.PartitionState
}

func (r *TableMetaReader) GetTableDef() *plan.TableDef {
	return r.table.tableDef
}

func (r *TableMetaReader) GetTxnInfo() string {
	return r.table.getTxn().op.Txn().DebugString()
}

func (r *TableMetaReader) Close() error {
	//r.tblDef = nil
	r.table = nil
	r.pState = nil
	r.state = endState
	return nil
}

func (r *TableMetaReader) addCloneSharedFile(
	stats *objectio.ObjectStats,
	source cloneObjectSource,
) {
	txnId := r.table.db.getTxn().op.Txn().ID
	name := stats.ObjectName().String()
	if source == cloneObjectFromCommittedState {
		r.table.db.getEng().cloneTxnCache.AddSharedFile(txnId, name)
		return
	}
	r.table.db.getEng().cloneTxnCache.AddTxnLocalSharedFile(txnId, name)
}

func NewTableMetaReader(
	ctx context.Context,
	rel engine.Relation,
) (engine.Reader, error) {

	var (
		ok  bool
		err error

		fs        fileservice.FileService
		snapshot  types.TS
		txnOffset int
		//tblDef   *plan.TableDef
		pState *logtailreplay.PartitionState

		table *txnTable
	)

	if table, ok = rel.(*txnTable); !ok {
		table = rel.(*txnTableDelegate).origin
	}

	//tblDef = table.GetTableDef(ctx)
	fs = table.getTxn().proc.GetFileService()
	snapshot = types.TimestampToTS(table.getTxn().op.SnapshotTS())
	// A clone source may itself be created by an earlier ALTER in the same
	// transaction. Keep the visible write boundary so metadata and row scans
	// include those txn-local objects without seeing later writes.
	txnOffset = len(table.getTxn().writes)
	if table.db.op.IsSnapOp() {
		txnOffset = table.getTxn().GetSnapshotWriteOffset()
	}

	if pState, err = table.getPartitionState(ctx); err != nil {
		return nil, err
	}

	return &TableMetaReader{
		fs:        fs,
		snapshot:  snapshot,
		txnOffset: txnOffset,
		//tblDef:   tblDef,
		table:  table,
		pState: pState,
	}, nil
}

func (r *TableMetaReader) Read(
	ctx context.Context,
	_ []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	outBatch *batch.Batch,
) (end bool, err error) {

	if r.state == endState {
		return true, nil
	}

	var (
		attrs    []string
		seqnums  []uint16
		colTypes []types.Type

		logs []zap.Field

		isTombstone = r.state == tombstoneMetaState
	)

	defer func() {
		stateStr := "data"
		if r.state == tombstoneMetaState {
			stateStr = "tombstone"
		}

		if r.state == dataMetaState {
			r.state = tombstoneMetaState
		} else {
			r.state = endState
		}

		logs = append(logs, zap.String("table",
			fmt.Sprintf("%s(%d)-%s(%d)-%s",
				r.table.db.databaseName, r.table.db.databaseId,
				r.table.tableName, r.table.tableId,
				r.GetTxnInfo())))

		logs = append(logs, zap.String("state", stateStr))
		logs = append(logs, zap.Error(err))

		logutil.Info("TableMetaReader", logs...)
	}()

	outBatch.CleanOnlyData()

	if isTombstone {
		pkCol := plan2.PkColByTableDef(r.table.tableDef)
		pkType := plan2.ExprType2Type(&pkCol.Typ)

		seqnums = []uint16{0, 1}
		colTypes = []types.Type{types.T_Rowid.ToType(), pkType}
		attrs = objectio.TombstoneAttrs_CN_Created
	} else {
		seqnums, colTypes, attrs, _, _ = colexec.GetSequmsAttrsSortKeyIdxFromTableDef(r.table.tableDef)
	}

	if logs, err = r.collect(
		ctx, mp, outBatch, isTombstone, seqnums, attrs, colTypes,
	); err != nil {
		return false, err
	}

	return false, err
}

func (r *TableMetaReader) SetOrderBy(specs []*plan.OrderBySpec) {
}

func (r *TableMetaReader) SetIndexParam(param *plan.IndexReaderParam) {
}

func (r *TableMetaReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (r *TableMetaReader) SetFilterZM(zoneMap objectio.ZoneMap) {
}

func (r *TableMetaReader) collect(
	ctx context.Context,
	mp *mpool.MPool,
	outBatch *batch.Batch,
	isTombstone bool,
	seqnums []uint16,
	attrs []string,
	colTypes []types.Type,
) (logs []zap.Field, err error) {

	var (
		iter       objectio.ObjectIter
		objRelData readutil.ObjListRelData

		objCnt, blkCnt, rowCnt int

		log1, log2, log3 zap.Field
	)

	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if iter, err = r.pState.NewObjectsIter(
		r.snapshot, true, isTombstone,
	); err != nil {
		return nil, err
	}

	appendObjectStats := func(stats *objectio.ObjectStats, source cloneObjectSource) error {
		if stats.GetCNCreated() || !stats.GetAppendable() {
			objCnt++
			blkCnt += int(stats.BlkCnt())
			rowCnt += int(stats.Rows())

			if err = colexec.ExpandObjectStatsToBatch(mp, isTombstone, outBatch, false, *stats); err != nil {
				return err
			}

			// Committed pState objects are owned outside this transaction and
			// must survive any clone rollback. Txn-local objects can be shared
			// by later ALTER/clone writes in this transaction, so intermediate
			// workspace GC must keep them, but full transaction rollback must
			// still delete them.
			r.addCloneSharedFile(stats, source)

		} else {
			// we can see an appendable object, if the snapshot falls into [createTS, deleteTS).
			// so there may exist rows which commitTS > snapshot, we need to scan all rows to filter them out.
			objRelData.AppendObj(stats)
		}
		return nil
	}

	for iter.Next() {
		obj := iter.Entry()
		if err = appendObjectStats(&obj.ObjectStats, cloneObjectFromCommittedState); err != nil {
			return nil, err
		}
	}

	// pState only contains committed objects. Repeated ALTER statements in one
	// transaction can clone from objects produced by an earlier ALTER, so include
	// txn-local object stats up to the reader's write boundary.
	var uncommittedObjs []objectio.ObjectStats
	if isTombstone {
		uncommittedObjs, _ = r.table.collectUnCommittedTombstoneObjs(r.txnOffset)
	} else {
		uncommittedObjs, _ = r.table.collectUnCommittedDataObjs(r.txnOffset)
	}
	for i := range uncommittedObjs {
		if err = appendObjectStats(&uncommittedObjs[i], cloneObjectFromTxnWorkspace); err != nil {
			return nil, err
		}
	}

	log1 = zap.String("collect-naobjs",
		fmt.Sprintf("%d-%d-%d", objCnt, blkCnt, rowCnt),
	)
	logs = append(logs, log1)

	if isTombstone {
		if log2, err = r.collectTombstoneOfAObjsAndInMem(
			ctx, mp, outBatch, seqnums, attrs, colTypes, objRelData,
		); err != nil {
			return nil, err
		}

		logs = append(logs, log2)
	} else {
		if log3, err = r.collectDataOfAObjsAndInMem(
			ctx, mp, outBatch, seqnums, attrs, colTypes, objRelData,
		); err != nil {
			return nil, err
		}

		logs = append(logs, log3)
	}

	outBatch.SetRowCount(outBatch.Vecs[0].Length())

	return logs, nil
}

func (r *TableMetaReader) collectDataOfAObjsAndInMem(
	ctx context.Context,
	mp *mpool.MPool,
	outBatch *batch.Batch,
	seqnums []uint16,
	attrs []string,
	colTypes []types.Type,
	objRelData readutil.ObjListRelData,
) (log zap.Field, err error) {

	var (
		dataReader engine.Reader
		s3Writer   *colexec.CNS3Writer

		objCnt, blkCnt, rowCnt int
	)

	s3Writer = colexec.NewCNS3DataWriter(mp, r.fs, r.table.tableDef, -1, false)
	defer func() {
		if dataReader != nil {
			dataReader.Close()
		}
		s3Writer.Close()
	}()

	source, err := NewLocalDataSource(
		ctx,
		r.table,
		r.txnOffset,
		r.pState,
		objRelData.GetBlockInfoSlice(),
		nil,
		false,
		engine.Policy_SkipUncommitedInMemory,
		engine.GeneralLocalDataSource,
	)
	if err != nil {
		return zap.Skip(), err
	}
	// Use the normal local data source so appendable object scans share the same
	// txn visibility rules as regular table reads.
	source.snapshotTS = r.snapshot

	dataReader = readutil.SimpleReaderWithDataSource(
		ctx, r.fs,
		source, r.snapshot.ToTimestamp(),
		readutil.WithColumns(seqnums, colTypes),
	)

	if objCnt, blkCnt, rowCnt, err = readWriteHelper(
		ctx, dataReader, outBatch, mp, attrs, colTypes, s3Writer,
	); err != nil {
		return zap.Skip(), err
	}

	log = zap.String("collect-aobj-inmem",
		fmt.Sprintf("%d-%d-%d", objCnt, blkCnt, rowCnt),
	)

	return log, nil
}

func (r *TableMetaReader) collectTombstoneOfAObjsAndInMem(
	ctx context.Context,
	mp *mpool.MPool,
	outBatch *batch.Batch,
	seqnums []uint16,
	attrs []string,
	colTypes []types.Type,
	objRelData readutil.ObjListRelData,
) (log zap.Field, err error) {

	var (
		rowsBatch *batch.Batch
		s3Writer  *colexec.CNS3Writer

		iter            logtailreplay.RowsIter
		tombstoneReader engine.Reader

		objCnt, blkCnt, rowCnt int
	)

	s3Writer = colexec.NewCNS3TombstoneWriter(mp, r.fs, colTypes[1], -1)

	defer func() {
		if iter != nil {
			iter.Close()
		}
		s3Writer.Close()

		if tombstoneReader != nil {
			tombstoneReader.Close()
		}

		if rowsBatch != nil {
			rowsBatch.Clean(mp)
		}
	}()

	appendTombstoneRow := func(rowidVec, pkVec *vector.Vector, offset int64) error {
		if rowsBatch == nil {
			rowsBatch = batch.New(attrs)
			rowsBatch.Attrs = attrs
			for i := 0; i < len(rowsBatch.Attrs); i++ {
				rowsBatch.Vecs[i] = vector.NewVec(colTypes[i])
			}
		}
		if err = rowsBatch.Vecs[0].UnionOne(rowidVec, offset, mp); err != nil {
			return err
		}
		return rowsBatch.Vecs[1].UnionOne(pkVec, offset, mp)
	}

	var appendErr error
	// Row-level tombstones created earlier in this transaction are not in pState
	// yet, but clone must carry them into the new tombstone object.
	r.table.getTxn().ForEachTableWrites(
		r.table.db.databaseId,
		r.table.tableId,
		r.txnOffset,
		func(entry Entry) {
			if appendErr != nil ||
				entry.typ != DELETE ||
				entry.bat == nil ||
				entry.bat.IsEmpty() ||
				len(entry.bat.Vecs) < 2 ||
				entry.bat.Vecs[0].GetType().Oid != types.T_Rowid {
				return
			}
			for i := 0; i < entry.bat.RowCount(); i++ {
				if appendErr = appendTombstoneRow(entry.bat.Vecs[0], entry.bat.Vecs[1], int64(i)); appendErr != nil {
					return
				}
			}
		})
	if appendErr != nil {
		return zap.Skip(), appendErr
	}

	iter = r.pState.NewRowsIter(r.snapshot, nil, true)
	for iter.Next() {
		entry := iter.Entry()

		// tombstones in the mem rows: del_row_id, commit_ts, pk, row_id.
		// expected: del_row_id and pk.
		if err = appendTombstoneRow(entry.Batch.Vecs[0], entry.Batch.Vecs[2], entry.Offset); err != nil {
			return zap.Skip(), err
		}
	}

	if rowsBatch != nil {
		rowsBatch.SetRowCount(rowsBatch.Vecs[0].Length())
		if err = s3Writer.Write(ctx, rowsBatch); err != nil {
			return zap.Skip(), err
		}
	}

	if objRelData.DataCnt() != 0 {
		tombstoneReader = readutil.SimpleMultiObjectsReader(
			ctx, r.fs, objRelData.Objlist, r.snapshot.ToTimestamp(),
			readutil.WithColumns(seqnums, colTypes),
		)
	}

	if objCnt, blkCnt, rowCnt, err = readWriteHelper(
		ctx, tombstoneReader, outBatch, mp, attrs, colTypes, s3Writer,
	); err != nil {
		return zap.Skip(), err
	}

	log = zap.String("collect-aobj-inmem",
		fmt.Sprintf("%d-%d-%d", objCnt, blkCnt, rowCnt),
	)

	return log, nil
}

func readWriteHelper(
	ctx context.Context,
	reader engine.Reader,
	outBatch *batch.Batch,
	mp *mpool.MPool,
	attrs []string,
	colTypes []types.Type,
	s3Writer *colexec.CNS3Writer,
) (objCnt, blkCnt, rowCnt int, err error) {

	var (
		stop      bool
		sl        []objectio.ObjectStats
		tmpBat    *batch.Batch
		dataBatch *batch.Batch
	)

	defer func() {
		if dataBatch != nil {
			dataBatch.Clean(mp)
		}
	}()

	if reader != nil {
		dataBatch = batch.New(attrs)
		dataBatch.Attrs = attrs
		for i := 0; i < len(dataBatch.Attrs); i++ {
			dataBatch.Vecs[i] = vector.NewVec(colTypes[i])
		}

		for {
			dataBatch.CleanOnlyData()
			if stop, err = reader.Read(ctx, attrs, nil, mp, dataBatch); err != nil {
				return
			}

			if stop {
				break
			}

			if dataBatch.RowCount() > 0 {
				if err = s3Writer.Write(ctx, dataBatch); err != nil {
					return
				}
			}
		}
	}

	if sl, err = s3Writer.Sync(ctx); err != nil {
		return
	}

	if tmpBat, err = s3Writer.FillBlockInfoBat(); err != nil {
		return
	}

	if _, err = outBatch.Append(ctx, mp, tmpBat); err != nil {
		return
	}

	for _, s := range sl {
		objCnt++
		blkCnt += int(s.BlkCnt())
		rowCnt += int(s.Rows())
	}

	return
}
