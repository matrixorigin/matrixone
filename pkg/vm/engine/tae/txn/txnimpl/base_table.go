// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/tidwall/btree"
)

var (
	ErrDuplicateNode = moerr.NewInternalErrorNoCtx("tae: duplicate node")
)

type baseTable struct {
	txnTable    *txnTable
	schema      *catalog.Schema
	isTombstone bool

	tableSpace *tableSpace
}

func newBaseTable(schema *catalog.Schema, isTombstone bool, txnTable *txnTable) *baseTable {
	return &baseTable{
		schema:      schema,
		txnTable:    txnTable,
		isTombstone: isTombstone,
	}
}
func (tbl *baseTable) collectCmd(cmdMgr *commandManager) (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.CollectCmd(cmdMgr)
	}
	return
}
func (tbl *baseTable) Close() error {
	if tbl.tableSpace != nil {
		err := tbl.tableSpace.Close()
		if err != nil {
			return err
		}
		tbl.tableSpace = nil
	}
	return nil
}
func (tbl *baseTable) DedupWorkSpace(key containers.Vector) (err error) {
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	return
}
func (tbl *baseTable) approxSize() int {
	if tbl == nil || tbl.tableSpace == nil || tbl.tableSpace.node == nil {
		return 0
	}
	return tbl.tableSpace.node.data.ApproxSize()
}
func (tbl *baseTable) BatchDedupLocal(bat *containers.Batch) error {
	if tbl.tableSpace == nil || !tbl.schema.HasPK() {
		return nil
	}
	return tbl.DedupWorkSpace(bat.GetVectorByName(tbl.schema.GetPrimaryKey().Name))
}

func (tbl *baseTable) addObjsWithMetaLoc(ctx context.Context, stats objectio.ObjectStats) (err error) {
	var pkVecs []containers.Vector
	var closeFuncs []func()
	defer func() {
		for _, v := range pkVecs {
			v.Close()
		}
		for _, f := range closeFuncs {
			f()
		}
	}()
	if tbl.tableSpace != nil && tbl.tableSpace.isStatsExisted(stats) {
		return nil
	}
	metaLocs := make([]objectio.Location, 0)
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := tbl.schema.Extra.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		metaLocs = append(metaLocs, metaloc)
	}
	schema := tbl.schema
	if schema.HasPK() && !tbl.schema.IsSecondaryIndexTable() {
		dedupType := tbl.txnTable.store.txn.GetDedupType()
		if !dedupType.SkipSourcePersisted() {
			for _, loc := range metaLocs {
				var vectors []containers.Vector
				var closeFunc func()
				vectors, closeFunc, err = ioutil.LoadColumns2(
					ctx,
					[]uint16{uint16(schema.GetSingleSortKeyIdx())},
					nil,
					tbl.txnTable.store.rt.Fs.Service,
					loc,
					fileservice.Policy(0),
					false,
					nil,
				)
				if err != nil {
					return err
				}
				closeFuncs = append(closeFuncs, closeFunc)
				pkVecs = append(pkVecs, vectors[0])
				err = tbl.txnTable.dedup(ctx, vectors[0], tbl.isTombstone)
				if err != nil {
					return
				}
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl.txnTable, tbl.isTombstone)
	}
	return tbl.tableSpace.AddDataFiles(pkVecs, stats)
}
func (tbl *baseTable) getRowsByPK(ctx context.Context, pks containers.Vector) (rowIDs containers.Vector, err error) {
	var it *catalog.VisibleCommittedObjectIt
	if tbl.isTombstone {
		it = tbl.txnTable.entry.MakeTombstoneVisibleObjectIt(tbl.txnTable.store.txn)
	} else {
		it = tbl.txnTable.entry.MakeDataVisibleObjectIt(tbl.txnTable.store.txn)
	}
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	pkType := pks.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, pks.GetDownstreamVector()); err != nil {
		return
	}
	if err = vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	); err != nil {
		return
	}
	for it.Next() {
		obj := it.Item()
		objData := obj.GetObjectData()
		if objData == nil {
			continue
		}
		if obj.HasCommittedPersistedData() {
			var skip bool
			if skip, err = quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				continue
			}
		}
		err = obj.GetObjectData().GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			types.TS{}, types.MaxTs(),
			rowIDs,
			common.WorkspaceAllocator,
		)
		if err != nil {
			logutil.Infof("getRowsByPK failed GetDuplicate: %v", err)
			return
		}
	}
	return
}

/*
similar to findDeletes
*/
func (tbl *baseTable) incrementalGetRowsByPK(ctx context.Context, pks containers.Vector, from, to types.TS, inQueue bool) (rowIDs containers.Vector, err error) {
	// TODO(aptend): handle the iterator correctly
	var objIt btree.IterG[*catalog.ObjectEntry]
	if tbl.isTombstone {
		tbl.txnTable.entry.WaitTombstoneObjectCommitted(to)
		objIt = tbl.txnTable.entry.MakeTombstoneObjectIt()
	} else {
		tbl.txnTable.entry.WaitDataObjectCommitted(to)
		objIt = tbl.txnTable.entry.MakeDataObjectIt()
	}
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	)

	var earlybreak bool
	for ok := objIt.Last(); ok; ok = objIt.Prev() {
		if earlybreak {
			break
		}
		obj := objIt.Item()

		if obj.CreatedAt.GT(&to) {
			continue
		}

		if obj.IsAppendable() {
			if !obj.HasDropIntent() && obj.CreatedAt.LT(&from) {
				earlybreak = true
			}
		} else if obj.CreatedAt.LT(&from) {
			continue
		}

		// only keep the category-a + category-c for candidates.
		if obj.GetPrevVersion() == nil && obj.GetNextVersion() != nil {
			continue
		}

		if !obj.VisibleByTS(to) {
			continue
		}
		objData := obj.GetObjectData()
		err = objData.GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			from, to,
			rowIDs,
			common.WorkspaceAllocator,
		)
		if err != nil {
			return
		}
	}
	// s := ""
	// for _, v := range candidates {
	// 	s += v.StringWithLevel(2) + ","
	// }
	// logutil.Info("incr",
	// 	zap.Bool("inQueue", inQueue),
	// 	zap.String("table", tbl.txnTable.store.txn.Repr()),
	// 	zap.String("from", from.ToString()),
	// 	zap.String("to", to.ToString()),
	// 	zap.String("rowIDs", rowIDs.String()),
	// 	zap.String("pks", pks.String()),
	// 	zap.String("candidates", s),
	// )
	return
}

func (tbl *baseTable) CleanUp() {
	if tbl.tableSpace != nil {
		tbl.tableSpace.CloseAppends()
	}
}

func (tbl *baseTable) PrePrepare() error {
	if tbl.tableSpace != nil {
		return tbl.tableSpace.PrepareApply()
	}
	return nil
}

func quickSkipThisObject(
	ctx context.Context,
	keysZM index.ZM,
	meta *catalog.ObjectEntry,
) (ok bool, err error) {
	ok = !meta.SortKeyZoneMap().FastIntersect(keysZM)
	return
}
