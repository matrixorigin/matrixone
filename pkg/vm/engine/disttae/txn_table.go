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

package disttae

import (
	"context"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	AllColumns = "*"
)

var _ engine.Relation = new(txnTable)

func (tbl *txnTable) Stats(ctx context.Context, partitionTables []any, statsInfoMap any) bool {
	s, ok := statsInfoMap.(*plan2.StatsInfoMap)
	if !ok {
		return false
	}

	if partitionTables != nil {
		sumBlockInfos := make([]catalog.BlockInfo, 0)
		for _, partitionTable := range partitionTables {
			if ptable, ok2 := partitionTable.(*txnTable); ok2 {
				if len(ptable.blockInfos) == 0 || !ptable.blockInfosUpdated {
					err := ptable.updateBlockInfos(ctx)
					if err != nil {
						return false
					}
				}

				if len(ptable.blockInfos) > 0 {
					sumBlockInfos = append(sumBlockInfos, ptable.blockInfos...)
				}
			} else {
				panic("partition Table type is not txnTable")
			}
		}

		if len(sumBlockInfos) > 0 {
			return CalcStats(ctx, sumBlockInfos, tbl.getTableDef(), tbl.db.txn.proc, s)
		} else {
			// no meta means not flushed yet, very small table
			return false
		}
	} else {
		if len(tbl.blockInfos) == 0 || !tbl.blockInfosUpdated {
			err := tbl.updateBlockInfos(ctx)
			if err != nil {
				return false
			}
		}
		if len(tbl.blockInfos) > 0 {
			return CalcStats(ctx, tbl.blockInfos, tbl.getTableDef(), tbl.db.txn.proc, s)
		} else {
			// no meta means not flushed yet, very small table
			return false
		}
	}
}

func (tbl *txnTable) Rows(ctx context.Context) (rows int64, err error) {
	writes := make([]Entry, 0, len(tbl.db.txn.writes))
	writes = tbl.db.txn.getTableWrites(tbl.db.databaseId, tbl.tableId, writes)

	deletes := make(map[types.Rowid]struct{})
	for _, entry := range writes {
		if entry.typ == INSERT {
			rows = rows + int64(entry.bat.Length())
		} else {
			if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
				/*
					CASE:
					create table t1(a int);
					begin;
					truncate t1; //txnDatabase.Truncate will DELETE mo_tables
					show tables; // t1 must be shown
				*/
				if entry.databaseId == catalog.MO_CATALOG_ID &&
					entry.tableId == catalog.MO_TABLES_ID &&
					entry.truncate {
					continue
				}
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					deletes[v] = struct{}{}
				}
			}
		}
	}

	ts := types.TimestampToTS(tbl.db.txn.meta.SnapshotTS)
	partition, err := tbl.getPartitionState(ctx)
	if err != nil {
		return 0, err
	}
	iter := partition.NewRowsIter(ts, nil, false)
	for iter.Next() {
		entry := iter.Entry()
		if _, ok := deletes[entry.RowID]; ok {
			continue
		}
		rows++
	}
	iter.Close()

	if len(tbl.blockInfos) > 0 {
		for _, blk := range tbl.blockInfos {
			rows += int64(blk.MetaLocation().Rows())
		}
	}

	return rows, nil
}

func (tbl *txnTable) ForeachBlock(
	state *logtailreplay.PartitionState,
	fn func(block logtailreplay.BlockEntry) error,
) (err error) {
	ts := types.TimestampToTS(tbl.db.txn.meta.SnapshotTS)
	iter := state.NewBlocksIter(ts)
	for iter.Next() {
		entry := iter.Entry()
		if err = fn(entry); err != nil {
			break
		}
	}
	iter.Close()
	return
}

func (tbl *txnTable) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	var (
		err  error
		part *logtailreplay.PartitionState
	)
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return nil, nil, err
	}

	var inited bool
	cols := tbl.getTableDef().GetCols()
	dataLength := len(cols) - 1

	tableVal := make([][2]any, dataLength)
	tableTypes := make([]uint8, dataLength)
	zms := make([]objectio.ZoneMap, dataLength)

	var meta objectio.ObjectMeta
	fs, err := fileservice.Get[fileservice.FileService](tbl.db.txn.proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, nil, err
	}
	onBlkFn := func(blk logtailreplay.BlockEntry) error {
		var err error
		location := blk.MetaLocation()
		if objectio.IsSameObjectLocVsMeta(location, meta) {
			return nil
		}
		if meta, err = objectio.FastLoadObjectMeta(ctx, &location, fs); err != nil {
			return err
		}
		if inited {
			for idx := range zms {
				zm := meta.MustGetColumn(uint16(cols[idx].Seqnum)).ZoneMap()
				if !zm.IsInited() {
					continue
				}
				index.UpdateZM(zms[idx], zm.GetMaxBuf())
				index.UpdateZM(zms[idx], zm.GetMinBuf())
			}
		} else {
			for idx := range zms {
				zms[idx] = meta.MustGetColumn(uint16(cols[idx].Seqnum)).ZoneMap()
				tableTypes[idx] = uint8(cols[idx].Typ.Id)
			}
			inited = true
		}

		return nil
	}

	if err = tbl.ForeachBlock(part, onBlkFn); err != nil {
		return nil, nil, err
	}

	if !inited {
		return nil, nil, moerr.NewInvalidInputNoCtx("table meta is nil")
	}

	for idx, zm := range zms {
		tableVal[idx] = [2]any{zm.GetMin(), zm.GetMax()}
	}

	return tableVal, tableTypes, nil
}

// Get all neede column exclude the hidden size
func (tbl *txnTable) Size(ctx context.Context, name string) (int64, error) {
	ts := types.TimestampToTS(tbl.db.txn.meta.SnapshotTS)
	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return 0, err
	}

	ret := int64(0)
	neededCols := make(map[string]*plan.ColDef)
	cols := tbl.getTableDef().Cols
	found := false

	for i := range cols {
		if !cols[i].Hidden && (name == AllColumns || cols[i].Name == name) {
			neededCols[cols[i].Name] = cols[i]
			found = true
		}
	}

	if !found {
		return 0, moerr.NewInvalidInput(ctx, "bad input column name %v", name)
	}

	writes := make([]Entry, 0, len(tbl.db.txn.writes))
	writes = tbl.db.txn.getTableWrites(tbl.db.databaseId, tbl.tableId, writes)

	deletes := make(map[types.Rowid]struct{})
	for _, entry := range writes {
		if entry.typ == INSERT {
			for i, s := range entry.bat.Attrs {
				if _, ok := neededCols[s]; ok {
					ret += int64(entry.bat.Vecs[i].Size())
				}
			}
		} else {
			if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
				/*
					CASE:
					create table t1(a int);
					begin;
					truncate t1; //txnDatabase.Truncate will DELETE mo_tables
					show tables; // t1 must be shown
				*/
				if entry.databaseId == catalog.MO_CATALOG_ID &&
					entry.tableId == catalog.MO_TABLES_ID &&
					entry.truncate {
					continue
				}
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					deletes[v] = struct{}{}
				}
			}
		}
	}

	// Different rows may belong to same batch. So we have
	// to record the batch which we have already handled to avoid
	// repetitive computation
	handled := make(map[*batch.Batch]struct{})
	var meta objectio.ObjectMeta
	// Calculate the in mem size
	// TODO: It might includ some deleted row size
	iter := part.NewRowsIter(ts, nil, false)
	for iter.Next() {
		entry := iter.Entry()
		if _, ok := deletes[entry.RowID]; ok {
			continue
		}
		if _, ok := handled[entry.Batch]; ok {
			continue
		}

		for i, s := range entry.Batch.Attrs {
			if _, ok := neededCols[s]; ok {
				ret += int64(entry.Batch.Vecs[i].Size())
			}
		}

		handled[entry.Batch] = struct{}{}
	}
	iter.Close()

	fs, err := fileservice.Get[fileservice.FileService](tbl.db.txn.proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return -1, err
	}
	// Calculate the block size
	biter := part.NewBlocksIter(ts)
	for biter.Next() {
		blk := biter.Entry()
		location := blk.MetaLocation()
		if objectio.IsSameObjectLocVsMeta(location, meta) {
			continue
		}

		if meta, err = objectio.FastLoadObjectMeta(ctx, &location, fs); err != nil {
			biter.Close()
			return 0, err
		}

		for _, col := range neededCols {
			colmata := meta.MustGetColumn(uint16(col.Seqnum))
			ret += int64(colmata.Location().Length())
		}
	}
	biter.Close()

	return ret, nil
}

func (tbl *txnTable) GetColumMetadataScanInfo(ctx context.Context, name string) ([]*plan.MetadataScanInfo, error) {
	var (
		err  error
		part *logtailreplay.PartitionState
	)
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return nil, err
	}

	var needCols []*plan.ColDef
	found := false
	cols := tbl.getTableDef().GetCols()
	for _, c := range cols {
		// TODO: We can keep hidden column but need a better way to know
		// whether it has the colmeta or not
		if !c.Hidden && (c.Name == name || name == AllColumns) {
			needCols = append(needCols, c)
			found = true
		}
	}

	if !found {
		return nil, moerr.NewInvalidInput(ctx, "bad input column name %v", name)
	}
	fs, err := fileservice.Get[fileservice.FileService](tbl.db.txn.proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	var meta objectio.ObjectMeta
	infoList := make([]*plan.MetadataScanInfo, 0, len(tbl.blockInfos))
	eachBlkFn := func(blk logtailreplay.BlockEntry) error {
		var err error
		location := blk.MetaLocation()
		if !objectio.IsSameObjectLocVsMeta(location, meta) {
			if meta, err = objectio.FastLoadObjectMeta(ctx, &location, fs); err != nil {
				return err
			}
		}

		blkmeta := meta.GetBlockMeta(uint32(location.ID()))
		maxSeq := uint32(blkmeta.GetMaxSeqnum())
		for _, col := range needCols {
			// Blocks will have different col info becuase of ddl
			// So we have to check whether the block includes needed col
			// or not first.
			if col.Seqnum > maxSeq {
				continue
			}

			newInfo := &plan.MetadataScanInfo{
				ColName:    col.Name,
				EntryState: blk.EntryState,
				Sorted:     blk.Sorted,
				IsHidden:   col.Hidden,
			}
			FillByteFamilyTypeForBlockInfo(newInfo, blk)
			colmeta := blkmeta.ColumnMeta(uint16(col.Seqnum))

			newInfo.RowCnt = int64(blkmeta.GetRows())
			newInfo.NullCnt = int64(colmeta.NullCnt())
			newInfo.CompressSize = int64(colmeta.Location().Length())
			newInfo.OriginSize = int64(colmeta.Location().OriginSize())

			zm := colmeta.ZoneMap()
			newInfo.Max = zm.GetMaxBuf()
			newInfo.Min = zm.GetMinBuf()

			infoList = append(infoList, newInfo)
		}

		return nil
	}

	if err = tbl.ForeachBlock(part, eachBlkFn); err != nil {
		return nil, err
	}

	return infoList, nil
}

func FillByteFamilyTypeForBlockInfo(info *plan.MetadataScanInfo, blk logtailreplay.BlockEntry) error {
	// It is better to use the Marshal() method
	info.BlockId = blk.BlockID[:]
	info.ObjectName = blk.MetaLocation().Name().String()
	info.MetaLoc = blk.MetaLoc[:]
	info.DelLoc = blk.DeltaLoc[:]
	info.SegId = blk.SegmentID[:]
	info.CommitTs = blk.CommitTs[:]
	info.CreateTs = blk.CreateTime[:]
	info.DeleteTs = blk.DeleteTime[:]
	return nil
}

func (tbl *txnTable) GetDirtyBlksIn(state *logtailreplay.PartitionState) []types.Blockid {
	dirtyBlks := make([]types.Blockid, 0)
	for blk := range tbl.db.txn.blockId_dn_delete_metaLoc_batch {
		if !state.BlockVisible(
			blk, types.TimestampToTS(tbl.db.txn.meta.SnapshotTS)) {
			continue
		}
		dirtyBlks = append(dirtyBlks, blk)
	}
	return dirtyBlks
}

func (tbl *txnTable) LoadDeletesForBlock(bid types.Blockid, offsets *[]int64) (err error) {

	for blk, bats := range tbl.db.txn.blockId_dn_delete_metaLoc_batch {
		if blk != bid {
			continue
		}
		for _, bat := range bats {
			vs := vector.MustStrCol(bat.GetVector(0))
			for _, metalLoc := range vs {
				location, err := blockio.EncodeLocationFromString(metalLoc)
				if err != nil {
					return err
				}
				rowIdBat, err := blockio.LoadColumns(
					tbl.db.txn.proc.Ctx,
					[]uint16{0},
					nil,
					tbl.db.txn.engine.fs,
					location,
					tbl.db.txn.proc.GetMPool())
				if err != nil {
					return err
				}
				rowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
				for _, rowId := range rowIds {
					_, offset := rowId.Decode()
					*offsets = append(*offsets, int64(offset))
				}
			}
		}

	}
	return
}

// LoadDeletesForBlockIn loads deletes for blocks in PartitionState.
func (tbl *txnTable) LoadDeletesForBlockIn(
	state *logtailreplay.PartitionState,
	in bool,
	deleteBlockId map[types.Blockid][]int64,
	deletesRowId map[types.Rowid]uint8) error {

	for blk, bats := range tbl.db.txn.blockId_dn_delete_metaLoc_batch {
		if in != state.BlockVisible(
			blk, types.TimestampToTS(tbl.db.txn.meta.SnapshotTS)) {
			continue
		}
		for _, bat := range bats {
			vs := vector.MustStrCol(bat.GetVector(0))
			for _, metalLoc := range vs {
				location, err := blockio.EncodeLocationFromString(metalLoc)
				if err != nil {
					return err
				}
				rowIdBat, err := blockio.LoadColumns(
					tbl.db.txn.proc.Ctx,
					[]uint16{0},
					nil,
					tbl.db.txn.engine.fs,
					location,
					tbl.db.txn.proc.GetMPool())
				if err != nil {
					return err
				}
				rowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
				for _, rowId := range rowIds {
					if deleteBlockId != nil {
						id, offset := rowId.Decode()
						deleteBlockId[id] = append(deleteBlockId[id], int64(offset))
					} else if deletesRowId != nil {
						deletesRowId[rowId] = 0
					} else {
						panic("Load Block Deletes Error")
					}
				}
				rowIdBat.Clean(tbl.db.txn.proc.GetMPool())
			}
		}

	}
	return nil
}

func (tbl *txnTable) GetEngineType() engine.EngineType {
	return engine.Disttae
}

func (tbl *txnTable) reset(newId uint64) {
	//if the table is truncated first time, the table id is saved into the oldTableId
	if tbl.oldTableId == 0 {
		tbl.oldTableId = tbl.tableId
	}
	tbl.tableId = newId
	tbl._partState = nil
	tbl.blockInfos = nil
	tbl.blockInfosUpdated = false
}

func (tbl *txnTable) resetSnapshot() {
	tbl._partState = nil
	tbl.blockInfos = nil
	tbl.blockInfosUpdated = false
}

// return all unmodified blocks
func (tbl *txnTable) Ranges(ctx context.Context, exprs []*plan.Expr) (ranges [][]byte, err error) {
	tbl.writes = tbl.writes[:0]
	tbl.writesOffset = tbl.db.txn.statements[tbl.db.txn.statementID-1]

	tbl.writes = tbl.db.txn.getTableWrites(tbl.db.databaseId, tbl.tableId, tbl.writes)

	// make sure we have the block infos snapshot
	if err = tbl.updateBlockInfos(ctx); err != nil {
		return
	}

	// get the table's snapshot
	var part *logtailreplay.PartitionState
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return
	}

	ranges = make([][]byte, 0, 1)
	ranges = append(ranges, []byte{})

	if len(tbl.blockInfos) == 0 {
		return
	}

	// for dynamic parameter, sustitute param ref and const fold cast expression here to improve performance
	// temporary solution, will fix it in the future
	newExprs := make([]*plan.Expr, len(exprs))
	bat := batch.EmptyForConstFoldBatch
	for i := range exprs {
		newExprs[i] = plan2.DeepCopyExpr(exprs[i])
		// newExprs[i] = plan2.SubstitueParam(newExprs[i], tbl.proc)
		foldedExpr, _ := plan2.ConstantFold(bat, newExprs[i], tbl.proc, true)
		if foldedExpr != nil {
			newExprs[i] = foldedExpr
		}
	}

	err = tbl.rangesOnePart(
		ctx,
		part,
		tbl.getTableDef(),
		newExprs,
		tbl.blockInfos,
		&ranges,
		tbl.proc,
	)
	return
}

func (tbl *txnTable) ApplyRuntimeFilters(ctx context.Context, blocks [][]byte, exprs []*plan.Expr, filters []*pipeline.RuntimeFilter) ([][]byte, error) {
	var err error
	evaluators := make([]RuntimeFilterEvaluator, len(filters))

	for i, filter := range filters {
		switch filter.Typ {
		case pipeline.RuntimeFilter_IN:
			vec := vector.NewVec(types.T_any.ToType())
			err = vec.UnmarshalBinary(filter.Data)
			if err != nil {
				return nil, err
			}
			evaluators[i] = &RuntimeInFilter{
				InList: vec,
			}

		case pipeline.RuntimeFilter_MIN_MAX:
			evaluators[i] = &RuntimeZonemapFilter{
				Zm: objectio.ZoneMap(filter.Data),
			}
		}
	}

	proc := tbl.db.txn.proc

	var (
		objMeta  objectio.ObjectMeta
		skipObj  bool
		auxIdCnt int32
	)

	for _, expr := range exprs {
		auxIdCnt = plan2.AssignAuxIdForExpr(expr, auxIdCnt)
	}

	columnMap := make(map[int]int)
	zms := make([]objectio.ZoneMap, auxIdCnt)
	vecs := make([]*vector.Vector, auxIdCnt)
	plan2.GetColumnMapByExprs(exprs, tbl.getTableDef(), &columnMap)

	defer func() {
		for i := range vecs {
			if vecs[i] != nil {
				vecs[i].Free(proc.Mp())
			}
		}
	}()

	errCtx := errutil.ContextWithNoReport(ctx, true)
	fs, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	curr := 1 // Skip the first block which is always the memtable
	for i := 1; i < len(blocks); i++ {
		blk := catalog.DecodeBlockInfo(blocks[i])
		location := blk.MetaLocation()

		if !objectio.IsSameObjectLocVsMeta(location, objMeta) {
			if objMeta, err = objectio.FastLoadObjectMeta(errCtx, &location, fs); err != nil {
				return nil, err
			}

			skipObj = false
			// here we only eval expr on the object meta if it has more than 2 blocks
			if objMeta.BlockCount() > 2 {
				for i, expr := range exprs {
					zm := colexec.GetExprZoneMap(errCtx, proc, expr, objMeta, columnMap, zms, vecs)
					if zm.IsInited() && !evaluators[i].Evaluate(zm) {
						skipObj = true
						break
					}
				}
			}
		}

		if skipObj {
			continue
		}

		var skipBlk bool

		// eval filter expr on the block
		blkMeta := objMeta.GetBlockMeta(uint32(location.ID()))
		for i, expr := range exprs {
			zm := colexec.GetExprZoneMap(errCtx, proc, expr, blkMeta, columnMap, zms, vecs)
			if zm.IsInited() && !evaluators[i].Evaluate(zm) {
				skipBlk = true
				break
			}
		}

		if skipBlk {
			continue
		}

		// store the block in ranges
		blocks[curr] = blocks[i]
		curr++
	}

	return blocks[:curr], nil
}

// txn can read :
//  1. snapshot data:
//      1>. committed block data resides in S3.
//      2>. partition state data resides in memory. read by partitionReader.

//      deletes(rowids) for committed block exist in the following four places:
//      1. in delta location formed by DN writing S3. read by blockReader.
//      2. in CN's partition state, read by partitionReader.
//  	3. in txn's workspace(txn.writes) being deleted by txn, read by partitionReader.
//  	4. in delta location being deleted through CN writing S3, read by blockMergeReader.

//  2. data in txn's workspace:
//     1>.Raw batch data resides in txn.writes,read by partitionReader.
//     2>.CN blocks resides in S3, read by blockReader.

// rangesOnePart collect blocks which are visible to this txn,
// include committed blocks and uncommitted blocks by CN writing S3.
// notice that only clean blocks can be distributed into remote CNs.
func (tbl *txnTable) rangesOnePart(
	ctx context.Context,
	state *logtailreplay.PartitionState, // snapshot state of this transaction
	tableDef *plan.TableDef, // table definition (schema)
	exprs []*plan.Expr, // filter expression
	committedblocks []catalog.BlockInfo, // whole block list
	ranges *[][]byte, // output marshaled block list after filtering
	proc *process.Process, // process of this transaction
) (err error) {
	dirtyBlks := make(map[types.Blockid]struct{})

	//blks contains all visible blocks to this txn, namely
	//includes committed blocks and uncommitted blocks by CN writing S3.
	blks := make([]catalog.BlockInfo, 0, len(committedblocks))
	blks = append(blks, committedblocks...)

	//collect partitionState.dirtyBlocks which may be invisible to this txn into dirtyBlks.
	{
		iter := state.NewDirtyBlocksIter()
		for iter.Next() {
			entry := iter.Entry()
			//lazy load deletes for block.
			dirtyBlks[entry.BlockID] = struct{}{}
		}
		iter.Close()

	}

	//only collect dirty blocks in PartitionState.blocks into dirtyBlks.
	for _, bid := range tbl.GetDirtyBlksIn(state) {
		dirtyBlks[bid] = struct{}{}
	}
	txn := tbl.db.txn
	for _, entry := range tbl.writes {
		if entry.typ == INSERT {
			if entry.bat == nil || entry.bat.Length() == 0 {
				continue
			}
			if entry.bat.Attrs[0] != catalog.BlockMeta_MetaLoc {
				continue
			}
			//load uncommitted blocks from txn's workspace.
			metaLocs := vector.MustStrCol(entry.bat.Vecs[0])
			for _, metaLoc := range metaLocs {
				location, err := blockio.EncodeLocationFromString(metaLoc)
				if err != nil {
					return err
				}
				sid := location.Name().SegmentId()
				blkid := objectio.NewBlockid(
					&sid,
					location.Name().Num(),
					location.ID())
				pos, ok := txn.cnBlkId_Pos[*blkid]
				if !ok {
					panic(fmt.Sprintf("blkid %s not found", blkid.String()))
				}
				blks = append(blks, pos.blkInfo)
				//blkInfo := pos.blkInfo
				//blkInfo.PartitionNum = -1
				var offsets []int64
				txn.deletedBlocks.getDeletedOffsetsByBlock(blkid, &offsets)
				if len(offsets) != 0 {
					dirtyBlks[*blkid] = struct{}{}
				}
				//*ranges = append(*ranges, catalog.EncodeBlockInfo(blkInfo))
			}
			continue
		}
		// entry.typ == DELETE
		if entry.isGeneratedByTruncate() {
			continue
		}
		//deletes in tbl.writes maybe comes from PartitionState.rows or PartitionState.blocks.
		if entry.fileName == "" {
			vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
			for _, v := range vs {
				id, _ := v.Decode()
				dirtyBlks[id] = struct{}{}
			}
		}
	}

	var (
		objMeta  objectio.ObjectMeta
		zms      []objectio.ZoneMap
		vecs     []*vector.Vector
		skipObj  bool
		auxIdCnt int32
	)

	defer func() {
		for i := range vecs {
			if vecs[i] != nil {
				vecs[i].Free(proc.Mp())
			}
		}
	}()

	// check if expr is monotonic, if not, we can skip evaluating expr for each block
	for _, expr := range exprs {
		auxIdCnt += plan2.AssignAuxIdForExpr(expr, auxIdCnt)
	}

	columnMap := make(map[int]int)
	if auxIdCnt > 0 {
		zms = make([]objectio.ZoneMap, auxIdCnt)
		vecs = make([]*vector.Vector, auxIdCnt)
		plan2.GetColumnMapByExprs(exprs, tableDef, &columnMap)
	}

	errCtx := errutil.ContextWithNoReport(ctx, true)

	fs, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	hasDeletes := len(dirtyBlks) > 0
	for _, blk := range blks {
		// if expr is monotonic, we need evaluating expr for each block
		if auxIdCnt > 0 {
			location := blk.MetaLocation()

			// check whether the block belongs to a new object
			// yes:
			//     1. load object meta
			//     2. eval expr on object meta
			//     3. if the expr is false, skip eval expr on the blocks of the same object
			// no:
			//     1. check whether the object is skipped
			//     2. if skipped, skip this block
			//     3. if not skipped, eval expr on the block
			if !objectio.IsSameObjectLocVsMeta(location, objMeta) {
				if objMeta, err = objectio.FastLoadObjectMeta(ctx, &location, fs); err != nil {
					return
				}

				skipObj = false
				// here we only eval expr on the object meta if it has more than 2 blocks
				if objMeta.BlockCount() > 2 {
					for _, expr := range exprs {
						if !colexec.EvaluateFilterByZoneMap(errCtx, proc, expr, objMeta, columnMap, zms, vecs) {
							skipObj = true
							break
						}
					}
				}
			}

			if skipObj {
				continue
			}

			var skipBlk bool

			// eval filter expr on the block
			blkMeta := objMeta.GetBlockMeta(uint32(location.ID()))
			for _, expr := range exprs {
				if !colexec.EvaluateFilterByZoneMap(errCtx, proc, expr, blkMeta, columnMap, zms, vecs) {
					skipBlk = true
					break
				}
			}

			// if the block is not needed, skip it
			if skipBlk {
				continue
			}
		}

		if hasDeletes {
			if _, ok := dirtyBlks[blk.BlockID]; !ok {
				blk.CanRemote = true
			}
			blk.PartitionNum = -1
			*ranges = append(*ranges, catalog.EncodeBlockInfo(blk))
			continue
		}
		// store the block in ranges
		blk.CanRemote = true
		blk.PartitionNum = -1
		*ranges = append(*ranges, catalog.EncodeBlockInfo(blk))
	}
	blockio.RecordBlockSelectivity(len(*ranges)-1, len(blks))
	return
}

// getTableDef only return all cols and their index.
func (tbl *txnTable) getTableDef() *plan.TableDef {
	if tbl.tableDef == nil {
		var cols []*plan.ColDef
		i := int32(0)
		name2index := make(map[string]int32)
		for _, def := range tbl.defs {
			if attr, ok := def.(*engine.AttributeDef); ok {
				name2index[attr.Attr.Name] = i
				cols = append(cols, &plan.ColDef{
					Name:  attr.Attr.Name,
					ColId: attr.Attr.ID,
					Typ: &plan.Type{
						Id:       int32(attr.Attr.Type.Oid),
						Width:    attr.Attr.Type.Width,
						Scale:    attr.Attr.Type.Scale,
						AutoIncr: attr.Attr.AutoIncrement,
					},
					Primary:   attr.Attr.Primary,
					Default:   attr.Attr.Default,
					OnUpdate:  attr.Attr.OnUpdate,
					Comment:   attr.Attr.Comment,
					ClusterBy: attr.Attr.ClusterBy,
					Seqnum:    uint32(attr.Attr.Seqnum),
				})
				i++
			}
		}
		tbl.tableDef = &plan.TableDef{
			Name:          tbl.tableName,
			Cols:          cols,
			Name2ColIndex: name2index,
		}
		tbl.tableDef.Version = tbl.version
		// add Constraint
		if len(tbl.constraint) != 0 {
			c := new(engine.ConstraintDef)
			err := c.UnmarshalBinary(tbl.constraint)
			if err != nil {
				return nil
			}
			for _, ct := range c.Cts {
				switch k := ct.(type) {
				case *engine.PrimaryKeyDef:
					tbl.tableDef.Pkey = k.Pkey
				}
			}
		}
	}

	return tbl.tableDef
}

func (tbl *txnTable) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	//return tbl.defs, nil
	// I don't understand why the logic now is not to get all the tableDef. Don't understand.
	// copy from tae's logic
	defs := make([]engine.TableDef, 0, len(tbl.defs))
	defs = append(defs, &engine.VersionDef{Version: tbl.version})
	if tbl.comment != "" {
		commentDef := new(engine.CommentDef)
		commentDef.Comment = tbl.comment
		defs = append(defs, commentDef)
	}
	if tbl.partitioned > 0 || tbl.partition != "" {
		partitionDef := new(engine.PartitionDef)
		partitionDef.Partitioned = tbl.partitioned
		partitionDef.Partition = tbl.partition
		defs = append(defs, partitionDef)
	}

	if tbl.viewdef != "" {
		viewDef := new(engine.ViewDef)
		viewDef.View = tbl.viewdef
		defs = append(defs, viewDef)
	}
	if len(tbl.constraint) > 0 {
		c := &engine.ConstraintDef{}
		err := c.UnmarshalBinary(tbl.constraint)
		if err != nil {
			return nil, err
		}
		defs = append(defs, c)
	}
	for i, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Name != catalog.Row_ID {
				defs = append(defs, tbl.defs[i])
			}
		}
	}
	pro := new(engine.PropertiesDef)
	pro.Properties = append(pro.Properties, engine.Property{
		Key:   catalog.SystemRelAttr_Kind,
		Value: string(tbl.relKind),
	})
	if tbl.createSql != "" {
		pro.Properties = append(pro.Properties, engine.Property{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: tbl.createSql,
		})
	}
	defs = append(defs, pro)
	return defs, nil

}

func (tbl *txnTable) UpdateConstraint(ctx context.Context, c *engine.ConstraintDef) error {
	ct, err := c.MarshalBinary()
	if err != nil {
		return err
	}
	bat, err := genTableConstraintTuple(tbl.tableId, tbl.db.databaseId, tbl.tableName, tbl.db.databaseName, ct, tbl.db.txn.proc.Mp())
	if err != nil {
		return err
	}
	if err = tbl.db.txn.WriteBatch(UPDATE, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, tbl.db.txn.dnStores[0], -1, false, false); err != nil {
		return err
	}
	tbl.constraint = ct
	return nil
}

func (tbl *txnTable) AlterTable(ctx context.Context, c *engine.ConstraintDef, constraint [][]byte) error {
	ct, err := c.MarshalBinary()
	if err != nil {
		return err
	}
	bat, err := genTableAlterTuple(constraint, tbl.db.txn.proc.Mp())
	if err != nil {
		return err
	}
	if err = tbl.db.txn.WriteBatch(ALTER, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, tbl.db.txn.dnStores[0], -1, false, false); err != nil {
		return err
	}
	tbl.constraint = ct
	return nil
}

func (tbl *txnTable) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	var attrs []*engine.Attribute
	for _, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			attrs = append(attrs, &attr.Attr)
		}
	}
	return attrs, nil
}

func (tbl *txnTable) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	attrs := make([]*engine.Attribute, 0, 1)
	for _, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Primary {
				attrs = append(attrs, &attr.Attr)
			}
		}
	}
	return attrs, nil
}

func (tbl *txnTable) GetHideKeys(ctx context.Context) ([]*engine.Attribute, error) {
	attrs := make([]*engine.Attribute, 0, 1)
	attrs = append(attrs, &engine.Attribute{
		IsHidden: true,
		IsRowId:  true,
		Name:     catalog.Row_ID,
		Type:     types.New(types.T_Rowid, 0, 0),
		Primary:  true,
	})
	return attrs, nil
}

func (tbl *txnTable) Write(ctx context.Context, bat *batch.Batch) error {
	if bat == nil || bat.Length() == 0 {
		return nil
	}
	// for writing S3 Block
	if bat.Attrs[0] == catalog.BlockMeta_BlockInfo {
		blkInfo := catalog.DecodeBlockInfo(bat.Vecs[0].GetBytesAt(0))
		fileName := blkInfo.MetaLocation().Name().String()
		ibat, err := util.CopyBatch(bat, tbl.db.txn.proc)
		if err != nil {
			return err
		}
		return tbl.db.txn.WriteFile(
			INSERT,
			tbl.db.databaseId,
			tbl.tableId,
			tbl.db.databaseName,
			tbl.tableName,
			fileName,
			ibat,
			tbl.db.txn.dnStores[0])
	}
	ibat, err := util.CopyBatch(bat, tbl.db.txn.proc)
	if err != nil {
		return err
	}
	if err := tbl.db.txn.WriteBatch(
		INSERT,
		tbl.db.databaseId,
		tbl.tableId,
		tbl.db.databaseName,
		tbl.tableName,
		ibat,
		tbl.db.txn.dnStores[0],
		tbl.primaryIdx,
		false,
		false); err != nil {
		return err
	}
	return tbl.db.txn.dumpBatch(tbl.writesOffset)
}

func (tbl *txnTable) Update(ctx context.Context, bat *batch.Batch) error {
	return nil
}

//	blkId(string)     deltaLoc(string)                   type(int)
//
// |-----------|-----------------------------------|----------------|
// |  blk_id   |   batch.Marshal(deltaLoc)         |  FlushDeltaLoc | DN Block
// |  blk_id   |   batch.Marshal(uint32 offset)    |  CNBlockOffset | CN Block
// |  blk_id   |   batch.Marshal(rowId)            |  RawRowIdBatch | DN Blcok
// |  blk_id   |   batch.Marshal(uint32 offset)    | RawBatchOffset | RawBatch (in txn workspace)
func (tbl *txnTable) EnhanceDelete(bat *batch.Batch, name string) error {
	blkId, typ_str := objectio.Str2Blockid(name[:len(name)-2]), string(name[len(name)-1])
	typ, err := strconv.ParseInt(typ_str, 10, 64)
	if err != nil {
		return err
	}
	switch typ {
	case deletion.FlushDeltaLoc:
		location, err := blockio.EncodeLocationFromString(bat.Vecs[0].GetStringAt(0))
		if err != nil {
			return err
		}
		fileName := location.Name().String()
		copBat, err := util.CopyBatch(bat, tbl.db.txn.proc)
		if err != nil {
			return err
		}
		if err := tbl.db.txn.WriteFile(DELETE, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, fileName, copBat, tbl.db.txn.dnStores[0]); err != nil {
			return err
		}
		tbl.db.txn.blockId_dn_delete_metaLoc_batch[*blkId] = append(tbl.db.txn.blockId_dn_delete_metaLoc_batch[*blkId], copBat)
	case deletion.CNBlockOffset:
		vs := vector.MustFixedCol[int64](bat.GetVector(0))
		tbl.db.txn.PutCnBlockDeletes(blkId, vs)
	case deletion.RawRowIdBatch:
		tbl.writeDnPartition(tbl.db.txn.proc.Ctx, bat)
	case deletion.RawBatchOffset:
		vs := vector.MustFixedCol[int64](bat.GetVector(0))
		entry_bat := tbl.db.txn.blockId_raw_batch[*blkId]
		entry_bat.AntiShrink(vs)
		// reset rowId offset
		rowIds := vector.MustFixedCol[types.Rowid](entry_bat.GetVector(0))
		for i := range rowIds {
			(&rowIds[i]).SetRowOffset(uint32(i))
		}
	}
	return nil
}

// CN Block Compaction
func (tbl *txnTable) compaction() error {
	mp := make(map[int][]int64)
	s3writer := &colexec.S3Writer{}
	s3writer.SetTableName(tbl.tableName)
	s3writer.SetSchemaVer(tbl.version)
	batchNums := 0
	name, err := s3writer.GenerateWriter(tbl.db.txn.proc)
	if err != nil {
		return err
	}
	var deletedIDs []*types.Blockid
	defer func() {
		tbl.db.txn.deletedBlocks.removeBlockDeletedInfos(deletedIDs)
	}()
	tbl.db.txn.deletedBlocks.iter(func(id *types.Blockid, deleteOffsets []int64) bool {
		pos := tbl.db.txn.cnBlkId_Pos[*id]
		// just do compaction for current txnTable
		entry := tbl.db.txn.writes[pos.idx]
		if !(entry.databaseId == tbl.db.databaseId && entry.tableId == tbl.tableId) {
			return true
		}
		delete(tbl.db.txn.cnBlkId_Pos, *id)
		deletedIDs = append(deletedIDs, id)
		if len(deleteOffsets) == 0 {
			return true
		}
		mp[pos.idx] = append(mp[pos.idx], pos.offset)
		// start compaction
		metaLoc := tbl.db.txn.writes[pos.idx].bat.GetVector(0).GetStringAt(int(pos.offset))
		location, e := blockio.EncodeLocationFromString(metaLoc)
		if e != nil {
			err = e
			return false
		}
		if tbl.seqnums == nil {
			n := len(tbl.tableDef.Cols) - 1
			idxs := make([]uint16, 0, n)
			typs := make([]types.Type, 0, n)
			for i := 0; i < len(tbl.tableDef.Cols)-1; i++ {
				col := tbl.tableDef.Cols[i]
				idxs = append(idxs, uint16(col.Seqnum))
				typs = append(typs, vector.ProtoTypeToType(col.Typ))
			}
			tbl.seqnums = idxs
			tbl.typs = typs
		}
		s3writer.SetSeqnums(tbl.seqnums)
		bat, e := blockio.BlockCompactionRead(
			tbl.db.txn.proc.Ctx,
			location,
			deleteOffsets,
			tbl.seqnums,
			tbl.typs,
			tbl.db.txn.engine.fs,
			tbl.db.txn.proc.GetMPool())
		if e != nil {
			err = e
			return false
		}
		if bat.Length() == 0 {
			return true
		}
		// ToDo: Optimize this logic, we need to control blocks num in one file
		// and make sure one block has as close as possible to 8192 rows
		// if the batch is little we should not flush, improve this in next pr.
		s3writer.WriteBlock(bat)
		batchNums++
		if len(deleteOffsets) > 0 {
			bat.Clean(tbl.db.txn.proc.GetMPool())
		}
		return true
	})
	if err != nil {
		return err
	}

	if batchNums > 0 {
		blkInfos, err := s3writer.WriteEndBlocks(tbl.db.txn.proc)
		if err != nil {
			return err
		}
		new_bat := batch.NewWithSize(1)
		new_bat.Attrs = []string{catalog.BlockMeta_BlockInfo}
		new_bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
		for _, blkInfo := range blkInfos {
			vector.AppendBytes(
				new_bat.GetVector(0),
				catalog.EncodeBlockInfo(blkInfo),
				false,
				tbl.db.txn.proc.GetMPool())
		}
		new_bat.SetZs(len(blkInfos), tbl.db.txn.proc.GetMPool())
		err = tbl.db.txn.WriteFile(
			INSERT,
			tbl.db.databaseId,
			tbl.tableId,
			tbl.db.databaseName,
			tbl.tableName,
			name.String(),
			new_bat,
			tbl.db.txn.dnStores[0])
		if err != nil {
			return err
		}
	}
	remove_batch := make(map[*batch.Batch]bool)
	// delete old block info
	for idx, offsets := range mp {
		bat := tbl.db.txn.writes[idx].bat
		tbl.db.txn.delPosForCNBlock(bat.GetVector(0), offsets)
		bat.AntiShrink(offsets)
		// update txn.cnBlkId_Pos
		tbl.db.txn.updatePosForCNBlock(bat.GetVector(0), idx)
		if bat.Length() == 0 {
			remove_batch[bat] = true
		}
	}
	tbl.db.txn.Lock()
	for i := 0; i < len(tbl.db.txn.writes); i++ {
		if remove_batch[tbl.db.txn.writes[i].bat] {
			// DON'T MODIFY THE IDX OF AN ENTRY IN LOG
			// THIS IS VERY IMPORTANT FOR CN BLOCK COMPACTION
			// maybe this will cause that the log imcrements unlimitly.
			// tbl.db.txn.writes = append(tbl.db.txn.writes[:i], tbl.db.txn.writes[i+1:]...)
			// i--
			tbl.db.txn.writes[i].bat.Clean(tbl.db.txn.proc.GetMPool())
			tbl.db.txn.writes[i].bat = nil
		}
	}
	tbl.db.txn.Unlock()
	return nil
}

func (tbl *txnTable) Delete(ctx context.Context, bat *batch.Batch, name string) error {
	if bat == nil {
		// ToDo:
		// start to do compaction for cn blocks
		// there are three strageties:
		// 1.do compaction at deletion operator
		// 2.do compaction here
		// 3.do compaction when read
		// choose which one at last depends on next pr
		// we use 2 now.
		return tbl.compaction()
	}
	//for S3 delete
	if name != catalog.Row_ID {
		return tbl.EnhanceDelete(bat, name)
	}
	bat.SetAttributes([]string{catalog.Row_ID})

	bat = tbl.db.txn.deleteBatch(bat, tbl.db.databaseId, tbl.tableId)
	if bat.Length() == 0 {
		return nil
	}
	return tbl.writeDnPartition(ctx, bat)
}

func (tbl *txnTable) writeDnPartition(ctx context.Context, bat *batch.Batch) error {
	ibat, err := util.CopyBatch(bat, tbl.db.txn.proc)
	if err != nil {
		return err
	}
	if err := tbl.db.txn.WriteBatch(DELETE, tbl.db.databaseId, tbl.tableId,
		tbl.db.databaseName, tbl.tableName, ibat, tbl.db.txn.dnStores[0], tbl.primaryIdx, false, false); err != nil {
		return err
	}
	return nil
}

func (tbl *txnTable) AddTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *txnTable) DelTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *txnTable) GetTableID(ctx context.Context) uint64 {
	return tbl.tableId
}

// GetTableName implements the engine.Relation interface.
func (tbl *txnTable) GetTableName() string {
	return tbl.tableName
}

func (tbl *txnTable) GetDBID(ctx context.Context) uint64 {
	return tbl.db.databaseId
}

func (tbl *txnTable) NewReader(
	ctx context.Context,
	num int,
	expr *plan.Expr,
	ranges [][]byte) ([]engine.Reader, error) {
	if len(ranges) == 0 {
		return tbl.newMergeReader(ctx, num, expr, nil)
	}
	if len(ranges) == 1 && engine.IsMemtable(ranges[0]) {
		return tbl.newMergeReader(ctx, num, expr, nil)
	}
	if len(ranges) > 1 && engine.IsMemtable(ranges[0]) {
		rds := make([]engine.Reader, num)
		mrds := make([]mergeReader, num)
		ranges = ranges[1:]

		var dirtyBlks []*catalog.BlockInfo
		var cleanBlks []*catalog.BlockInfo
		for _, blk := range ranges {
			blkInfo := catalog.DecodeBlockInfo(blk)
			if blkInfo.CanRemote {
				cleanBlks = append(cleanBlks, blkInfo)
				continue
			}
			dirtyBlks = append(dirtyBlks, blkInfo)
		}

		rds0, err := tbl.newMergeReader(ctx, num, expr, dirtyBlks)
		if err != nil {
			return nil, err
		}
		for i, rd := range rds0 {
			mrds[i].rds = append(mrds[i].rds, rd)
		}

		if len(cleanBlks) > 0 {
			rds0, err = tbl.newBlockReader(ctx, num, expr, cleanBlks, tbl.proc)
			if err != nil {
				return nil, err
			}
		}
		for i, rd := range rds0 {
			mrds[i].rds = append(mrds[i].rds, rd)
		}

		for i := range rds {
			rds[i] = &mrds[i]
		}
		return rds, nil
	}
	blkInfos := make([]*catalog.BlockInfo, 0, len(ranges))
	for _, r := range ranges {
		blkInfos = append(blkInfos, catalog.DecodeBlockInfo(r))
	}
	return tbl.newBlockReader(ctx, num, expr, blkInfos, tbl.proc)
}

func (tbl *txnTable) newMergeReader(
	ctx context.Context,
	num int,
	expr *plan.Expr,
	dirtyBlks []*catalog.BlockInfo) ([]engine.Reader, error) {

	var encodedPrimaryKey []byte
	pk := tbl.tableDef.Pkey
	if pk != nil && expr != nil {
		// TODO: workaround for composite primary key
		// right now for composite primary key, the filter expr will not be pushed down
		// here we try to serialize the composite primary key
		if pk.CompPkeyCol != nil {
			pkVals := make([]*plan.Const, len(pk.Names))
			_, hasNull := getCompositPKVals(expr, pk.Names, pkVals, tbl.proc)

			// return empty reader if the composite primary key has null value
			if hasNull {
				return []engine.Reader{
					new(emptyReader),
				}, nil
			}
			cnt := getValidCompositePKCnt(pkVals)
			if cnt != 0 {
				var packer *types.Packer
				put := tbl.db.txn.engine.packerPool.Get(&packer)
				for i := 0; i < cnt; i++ {
					serialTupleByConstExpr(pkVals[i], packer)
				}
				v := packer.Bytes()
				packer.Reset()
				encodedPrimaryKey = logtailreplay.EncodePrimaryKey(v, packer)
				// TODO: hack: remove the last comma, need to fix this in the future
				encodedPrimaryKey = encodedPrimaryKey[0 : len(encodedPrimaryKey)-1]
				put.Put()
			}
		} else {
			pkColumn := tbl.tableDef.Cols[tbl.primaryIdx]
			ok, v := getPkValueByExpr(expr, pkColumn.Name, types.T(pkColumn.Typ.Id), tbl.proc)
			if ok {
				var packer *types.Packer
				put := tbl.db.txn.engine.packerPool.Get(&packer)
				encodedPrimaryKey = logtailreplay.EncodePrimaryKey(v, packer)
				put.Put()
			}
		}
	}
	rds := make([]engine.Reader, num)
	mrds := make([]mergeReader, num)
	rds0, err := tbl.newReader(
		ctx,
		num,
		encodedPrimaryKey,
		expr,
		dirtyBlks,
		tbl.writes)
	if err != nil {
		return nil, err
	}
	mrds[0].rds = append(mrds[0].rds, rds0...)

	for i := range rds {
		rds[i] = &mrds[i]
	}

	return rds, nil
}

func (tbl *txnTable) newBlockReader(
	ctx context.Context,
	num int,
	expr *plan.Expr,
	blkInfos []*catalog.BlockInfo,
	proc *process.Process) ([]engine.Reader, error) {
	rds := make([]engine.Reader, num)
	ts := tbl.db.txn.meta.SnapshotTS
	tableDef := tbl.getTableDef()

	if len(blkInfos) < num || len(blkInfos) == 1 {
		for i, blk := range blkInfos {
			rds[i] = newBlockReader(
				ctx,
				tableDef,
				ts,
				[]*catalog.BlockInfo{blk},
				expr,
				tbl.db.txn.engine.fs,
				proc,
			)
		}
		for j := len(blkInfos); j < num; j++ {
			rds[j] = &emptyReader{}
		}
		return rds, nil
	}

	infos, steps := groupBlocksToObjects(blkInfos, num)
	fs, err := fileservice.Get[fileservice.FileService](
		tbl.db.txn.engine.fs,
		defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	blockReaders := newBlockReaders(
		ctx,
		fs,
		tableDef,
		tbl.primarySeqnum,
		ts,
		num,
		expr,
		proc)
	distributeBlocksToBlockReaders(blockReaders, num, infos, steps)
	for i := 0; i < num; i++ {
		rds[i] = blockReaders[i]
	}
	return rds, nil
}

func (tbl *txnTable) newReader(
	ctx context.Context,
	readerNumber int,
	encodedPrimaryKey []byte,
	expr *plan.Expr,
	dirtyBlks []*catalog.BlockInfo,
	entries []Entry,
) ([]engine.Reader, error) {
	txn := tbl.db.txn
	ts := txn.meta.SnapshotTS
	fs := txn.engine.fs
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	//prepare inserts and deletes for partition reader.
	//TODO:: put this logic into partitionReader.read.
	var inserts []*batch.Batch
	//var blkInfos []*catalog.BlockInfo
	//blkDels := make(map[types.Blockid][]int64)
	var deletes map[types.Rowid]uint8
	if !txn.readOnly.Load() {
		inserts = make([]*batch.Batch, 0, len(entries))
		deletes = make(map[types.Rowid]uint8)
		for _, entry := range entries {
			if entry.typ == INSERT {
				if entry.bat == nil || entry.bat.Length() == 0 {
					continue
				}
				if entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
					continue
				}
				inserts = append(inserts, entry.bat)
				continue
			}
			//entry.typ == DELETE
			if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
				/*
					CASE:
					create table t1(a int);
					begin;
					truncate t1; //txnDatabase.Truncate will DELETE mo_tables
					show tables; // t1 must be shown
				*/
				if entry.isGeneratedByTruncate() {
					continue
				}
				//deletes in txn.Write maybe comes from PartitionState.Rows ,
				// PartitionReader need to skip them.
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					deletes[v] = 0
				}
			}
		}
		//deletes maybe comes from PartitionState.rows, PartitionReader need to skip them;
		// so, here only load deletes which don't belong to PartitionState.blks.
		tbl.LoadDeletesForBlockIn(state, false, nil, deletes)
	}

	readers := make([]engine.Reader, readerNumber)

	seqnumMp := make(map[string]int)
	for _, coldef := range tbl.tableDef.Cols {
		seqnumMp[coldef.Name] = int(coldef.Seqnum)
	}

	mp := make(map[string]types.Type)
	mp[catalog.Row_ID] = types.New(types.T_Rowid, 0, 0)
	for _, def := range tbl.defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		mp[attr.Attr.Name] = attr.Attr.Type
	}

	var iter logtailreplay.RowsIter
	if len(encodedPrimaryKey) > 0 {
		iter = state.NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			logtailreplay.Prefix(encodedPrimaryKey),
		)
	} else {
		iter = state.NewRowsIter(
			types.TimestampToTS(ts),
			nil,
			false,
		)
	}

	partReader := &PartitionReader{
		typsMap:  mp,
		inserts:  inserts,
		deletes:  deletes,
		iter:     iter,
		seqnumMp: seqnumMp,
	}
	readers[0] = partReader

	if readerNumber == 1 {
		for i := range dirtyBlks {
			readers = append(
				readers,
				newBlockMergeReader(
					ctx,
					tbl,
					ts,
					[]*catalog.BlockInfo{dirtyBlks[i]},
					expr,
					fs,
					tbl.proc,
				),
			)
		}
		return []engine.Reader{&mergeReader{readers}}, nil
	}

	if len(dirtyBlks) < readerNumber-1 {
		for i := range dirtyBlks {
			readers[i+1] = newBlockMergeReader(
				ctx,
				tbl,
				ts,
				[]*catalog.BlockInfo{dirtyBlks[i]},
				expr,
				fs,
				tbl.proc,
			)
		}
		for j := len(dirtyBlks) + 1; j < readerNumber; j++ {
			readers[j] = &emptyReader{}
		}
		return readers, nil
	}
	//create readerNumber-1 blockReaders
	blockReaders := newBlockReaders(
		ctx,
		fs,
		tbl.tableDef,
		-1,
		ts,
		readerNumber-1,
		expr,
		tbl.proc)
	objInfos, steps := groupBlocksToObjects(dirtyBlks, readerNumber-1)
	blockReaders = distributeBlocksToBlockReaders(
		blockReaders,
		readerNumber-1,
		objInfos,
		steps)
	for i := range blockReaders {
		bmr := &blockMergeReader{
			blockReader: blockReaders[i],
			table:       tbl,
		}
		readers[i+1] = bmr
	}
	return readers, nil
}

// get the table's snapshot.
// it is only initialized once for a transaction and will not change.
func (tbl *txnTable) getPartitionState(ctx context.Context) (*logtailreplay.PartitionState, error) {
	if tbl._partState == nil {
		if err := tbl.updateLogtail(ctx); err != nil {
			return nil, err
		}
		tbl._partState = tbl.db.txn.engine.getPartition(tbl.db.databaseId, tbl.tableId).Snapshot()
	}
	return tbl._partState, nil
}

func (tbl *txnTable) updateBlockInfos(ctx context.Context) (err error) {
	tbl.dnList = []int{0}

	_, created := tbl.db.txn.createMap.Load(genTableKey(ctx, tbl.tableName, tbl.db.databaseId))
	// check if the table is not created in this txn, and the block infos are not updated, then update:
	// 1. update logtail
	// 2. generate block infos
	// 3. update the blockInfosUpdated and blockInfos fields of the table
	if !created && !tbl.blockInfosUpdated {
		if err = tbl.updateLogtail(ctx); err != nil {
			return
		}
		var blocks []catalog.BlockInfo
		if blocks, err = tbl.db.txn.getBlockInfos(ctx, tbl); err != nil {
			return
		}
		tbl.blockInfos = blocks
		tbl.blockInfosUpdated = true
	}
	return
}

func (tbl *txnTable) updateLogtail(ctx context.Context) (err error) {
	// if the logtail is updated, skip
	if tbl.logtailUpdated {
		return
	}

	// if the table is created in this txn, skip
	if _, created := tbl.db.txn.createMap.Load(genTableKey(ctx, tbl.tableName, tbl.db.databaseId)); created {
		return
	}

	tableId := tbl.tableId
	/*
		if the table is truncated once or more than once,
		it is suitable to use the old table id to sync logtail.

		CORNER CASE 1:
		create table t1(a int);
		begin;
		truncate t1; //table id changed. there is no new table id in DN.
		select count(*) from t1; // sync logtail for the new id failed.

		CORNER CASE 2:
		create table t1(a int);
		begin;
		select count(*) from t1; // sync logtail for the old succeeded.
		truncate t1; //table id changed. there is no new table id in DN.
		select count(*) from t1; // not sync logtail this time.

		CORNER CASE 3:
		create table t1(a int);
		begin;
		truncate t1; //table id changed. there is no new table id in DN.
		truncate t1; //table id changed. there is no new table id in DN.
		select count(*) from t1; // sync logtail for the new id failed.
	*/
	if tbl.oldTableId != 0 {
		tableId = tbl.oldTableId
	}

	if err = tbl.db.txn.engine.UpdateOfPush(ctx, tbl.db.databaseId, tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
		return
	}
	if err = tbl.db.txn.engine.lazyLoad(ctx, tbl); err != nil {
		return
	}

	tbl.logtailUpdated = true
	return nil
}

func (tbl *txnTable) PrimaryKeysMayBeModified(ctx context.Context, from types.TS, to types.TS, keysVector *vector.Vector) (bool, error) {
	var packer *types.Packer
	put := tbl.db.txn.engine.packerPool.Get(&packer)
	defer put.Put()
	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return false, err
	}
	return part.PrimaryKeysMayBeModified(from, to, keysVector, packer), nil
}
