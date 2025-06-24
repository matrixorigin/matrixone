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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"
)

var _ engine.Reader = new(TableMetaReader)

const (
	dataMetaState = iota
	tombstoneMetaState
	endState
)

type TableMetaReader struct {
	table    *txnTable
	fs       fileservice.FileService
	snapshot types.TS
	state    int
	//tblDef   *plan.TableDef
	pState *logtailreplay.PartitionState
}

func (r *TableMetaReader) Close() error {
	//r.tblDef = nil
	r.table = nil
	r.pState = nil
	r.state = endState
	return nil
}

func NewTableMetaReader(
	ctx context.Context,
	rel engine.Relation,
) (engine.Reader, error) {

	var (
		ok  bool
		err error

		fs       fileservice.FileService
		snapshot types.TS
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

	if pState, err = table.getPartitionState(ctx); err != nil {
		return nil, err
	}

	return &TableMetaReader{
		fs:       fs,
		snapshot: snapshot,
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

		logs1, logs2 = zap.Skip(), zap.Skip()

		isTombstone = r.state == tombstoneMetaState
	)

	defer func() {
		if r.state == dataMetaState {
			r.state = tombstoneMetaState
		} else {
			r.state = endState
		}

		stateStr := "data"
		if r.state == tombstoneMetaState {
			stateStr = "tombstone"
		}

		logutil.Info("TableMetaReader",
			zap.String("state", stateStr),
			zap.Error(err), logs1, logs2)
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

	// step1
	if logs1, err = r.collectVisibleObjs(
		ctx, mp, outBatch, isTombstone, seqnums, attrs, colTypes,
	); err != nil {
		return false, err
	}

	// step2
	logs2, err = r.collectVisibleInMemRows(
		ctx, mp, outBatch, isTombstone, seqnums, attrs, colTypes)

	return false, err
}

func (r *TableMetaReader) SetOrderBy(specs []*plan.OrderBySpec) {
}

func (r *TableMetaReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (r *TableMetaReader) SetFilterZM(zoneMap objectio.ZoneMap) {
}

func (r *TableMetaReader) collectVisibleInMemRows(
	ctx context.Context,
	mp *mpool.MPool,
	outBatch *batch.Batch,
	isTombstone bool,
	seqnums []uint16,
	attrs []string,
	colTypes []types.Type,
) (log zap.Field, err error) {

	var (
		iter logtailreplay.RowsIter
		sl   []objectio.ObjectStats

		s3Writer  *colexec.CNS3Writer
		rowsBatch *batch.Batch

		tmpBat *batch.Batch

		objCnt, blkCnt, rowCnt int
	)

	defer func() {
		if iter != nil {
			iter.Close()
		}

		if rowsBatch != nil {
			rowsBatch.Clean(mp)
		}

		if s3Writer != nil {
			s3Writer.Close(mp)
		}
	}()

	writeS3 := func() error {
		if s3Writer == nil {
			if isTombstone {
				s3Writer = colexec.NewCNS3TombstoneWriter(mp, r.fs, colTypes[1])
			} else {
				s3Writer = colexec.NewCNS3DataWriter(mp, r.fs, r.table.tableDef, false)
			}
		}

		return s3Writer.Write(ctx, mp, rowsBatch)
	}

	iter = r.pState.NewRowsIter(r.snapshot, nil, isTombstone)
	for iter.Next() {
		if rowsBatch == nil {
			rowsBatch = batch.New(attrs)
			rowsBatch.Attrs = attrs
			for i := 0; i < len(rowsBatch.Attrs); i++ {
				rowsBatch.Vecs[i] = vector.NewVec(colTypes[i])
			}
		}

		entry := iter.Entry()

		for i := range rowsBatch.Attrs {
			idx := 2 + seqnums[i]
			if int(idx) >= len(entry.Batch.Vecs) /*add column*/ ||
				entry.Batch.Attrs[idx] == "" /*drop column*/ {
				err = vector.AppendAny(
					rowsBatch.Vecs[i],
					nil,
					true,
					mp)
			} else {
				if isTombstone {
					// for tombstones in the mem rows: del_row_id, commit_ts, pk, row_id.
					// expected: del_row_id and pk.
					idx -= 2
					if idx == 1 {
						idx = 2
					}
				}

				err = rowsBatch.Vecs[i].UnionOne(
					entry.Batch.Vecs[int(idx)],
					entry.Offset,
					mp,
				)
			}
			if err != nil {
				return zap.Skip(), err
			}
		}

		rowsBatch.SetRowCount(rowsBatch.Vecs[0].Length())

		if rowsBatch.RowCount() >= options.DefaultBlockMaxRows {
			if err = writeS3(); err != nil {
				return zap.Skip(), err
			}

			rowsBatch.CleanOnlyData()
		}
	}

	if rowsBatch == nil {
		return zap.Skip(), nil
	}

	if rowsBatch.RowCount() > 0 {
		if err = writeS3(); err != nil {
			return zap.Skip(), err
		}
	}

	if sl, err = s3Writer.Sync(ctx, mp); err != nil {
		return zap.Skip(), err
	}

	if tmpBat, err = s3Writer.FillBlockInfoBat(mp); err != nil {
		return zap.Skip(), err
	}

	if _, err = outBatch.Append(ctx, mp, tmpBat); err != nil {
		return zap.Skip(), err
	}

	for _, s := range sl {
		objCnt++
		blkCnt += int(s.BlkCnt())
		rowCnt += int(s.Rows())
	}

	log = zap.String("collectVisibleInMemRows",
		fmt.Sprintf("%d-%d-%d", objCnt, blkCnt, rowCnt))

	return log, nil
}

func (r *TableMetaReader) collectVisibleObjs(
	ctx context.Context,
	mp *mpool.MPool,
	outBatch *batch.Batch,
	isTombstone bool,
	seqnums []uint16,
	attrs []string,
	colTypes []types.Type,
) (log zap.Field, err error) {

	var (
		stop bool
		sl   []objectio.ObjectStats

		iter       objectio.ObjectIter
		dataReader engine.Reader

		s3Writer  *colexec.CNS3Writer
		dataBatch *batch.Batch

		tmpBat *batch.Batch

		objRelData readutil.ObjListRelData

		newObjCnt, newBlkCnt, newRowCnt    int
		copyObjCnt, copyBlkCnt, copyRowCnt int
	)

	defer func() {
		if dataBatch != nil {
			dataBatch.Clean(mp)
		}

		if s3Writer != nil {
			s3Writer.Close(mp)
		}

		if iter != nil {
			iter.Close()
		}
	}()

	if iter, err = r.pState.NewObjectsIter(
		r.snapshot, true, isTombstone,
	); err != nil {
		return zap.Skip(), err
	}

	for iter.Next() {
		obj := iter.Entry()

		// if the obj is created by CN, the data commit time equals to the obj.CreateTime
		if obj.GetCNCreated() || !obj.GetAppendable() {
			copyObjCnt++
			copyBlkCnt += int(obj.ObjectStats.BlkCnt())
			copyRowCnt += int(obj.ObjectStats.Rows())

			if err = colexec.ExpandObjectStatsToBatch(
				mp, isTombstone, outBatch, true, obj.ObjectStats); err != nil {
				return zap.Skip(), err
			}
		} else {
			objRelData.AppendObj(&obj.ObjectStats)
		}
	}

	if objRelData.DataCnt() > 0 {
		if isTombstone {
			s3Writer = colexec.NewCNS3TombstoneWriter(mp, r.fs, colTypes[1])
		} else {
			s3Writer = colexec.NewCNS3DataWriter(mp, r.fs, r.table.tableDef, false)
		}

		source := &LocalDisttaeDataSource{
			table:           r.table,
			pState:          r.pState,
			fs:              r.fs,
			ctx:             ctx,
			mp:              mp,
			rangeSlice:      objRelData.GetBlockInfoSlice(),
			tombstonePolicy: engine.Policy_SkipUncommitedInMemory,
		}

		dataReader = readutil.SimpleReaderWithDataSource(
			ctx, r.fs,
			source, r.snapshot.ToTimestamp(),
			readutil.WithColumns(seqnums, colTypes),
		)

		dataBatch = batch.New(attrs)
		dataBatch.Attrs = attrs
		for i := 0; i < len(dataBatch.Attrs); i++ {
			dataBatch.Vecs[i] = vector.NewVec(colTypes[i])
		}

		for {
			stop, err = dataReader.Read(ctx, attrs, nil, mp, dataBatch)
			if err != nil {
				return zap.Skip(), err
			}

			if stop {
				break
			}

			if err = s3Writer.Write(ctx, mp, dataBatch); err != nil {
				return zap.Skip(), err
			}
		}

		if sl, err = s3Writer.Sync(ctx, mp); err != nil {
			return zap.Skip(), err
		}

		if tmpBat, err = s3Writer.FillBlockInfoBat(mp); err != nil {
			return zap.Skip(), err
		}

		if _, err = outBatch.Append(ctx, mp, tmpBat); err != nil {
			return zap.Skip(), err
		}

		for _, s := range sl {
			newObjCnt++
			newBlkCnt += int(s.BlkCnt())
			newRowCnt += int(s.Rows())
		}
	}

	outBatch.SetRowCount(outBatch.Vecs[0].Length())

	log = zap.String("collectVisibleObjs",
		fmt.Sprintf("copy(%d-%d-%d), new(%d-%d-%d)",
			copyObjCnt, copyBlkCnt, copyRowCnt, newObjCnt, newBlkCnt, newRowCnt))

	return log, nil
}
