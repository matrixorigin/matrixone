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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ engine.Relation = new(txnTable)

func (tbl *txnTable) Stats(ctx context.Context, expr *plan.Expr, statsInfoMap any) (*plan.Stats, error) {
	if !plan2.NeedStats(tbl.getTableDef()) {
		return plan2.DefaultStats(), nil
	}
	s, ok := statsInfoMap.(*plan2.StatsInfoMap)
	if !ok {
		return plan2.DefaultStats(), nil
	}
	if len(tbl.blockInfos) == 0 || !tbl.blockInfosUpdated {
		err := tbl.updateBlockInfos(ctx, expr)
		if err != nil {
			return plan2.DefaultStats(), err
		}
	}
	if len(tbl.blockInfos) > 0 {
		return CalcStats(ctx, tbl.blockInfos, expr, tbl.getTableDef(), tbl.db.txn.proc, tbl.getCbName(), s)
	} else {
		// no meta means not flushed yet, very small table
		return plan2.DefaultStats(), nil
	}
}

func (tbl *txnTable) Rows(ctx context.Context) (rows int64, err error) {
	writes := make([]Entry, 0, len(tbl.db.txn.writes))
	tbl.db.txn.Lock()
	for _, entry := range tbl.db.txn.writes {
		if entry.databaseId != tbl.db.databaseId {
			continue
		}
		if entry.tableId != tbl.tableId {
			continue
		}
		writes = append(writes, entry)
	}
	tbl.db.txn.Unlock()

	deletes := make(map[types.Rowid]struct{})
	for _, entry := range writes {
		if entry.typ == INSERT {
			rows = rows + int64(entry.bat.Length())
		} else {
			if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					deletes[v] = struct{}{}
				}
			}
		}
	}

	ts := types.TimestampToTS(tbl.db.txn.meta.SnapshotTS)
	parts, err := tbl.getParts(ctx)
	if err != nil {
		return 0, err
	}
	for _, part := range parts {
		iter := part.NewRowsIter(ts, nil, false)
		for iter.Next() {
			entry := iter.Entry()
			if _, ok := deletes[entry.RowID]; ok {
				continue
			}
			rows++
		}
		iter.Close()
	}

	if len(tbl.blockInfos) > 0 {
		for _, blks := range tbl.blockInfos {
			for _, blk := range blks {
				rows += int64(blk.MetaLocation().Rows())
			}
		}
	}

	return rows, nil
}

func (tbl *txnTable) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	if len(tbl.blockInfos) == 0 {
		return nil, nil, moerr.NewInvalidInputNoCtx("table meta is nil")
	}

	cols := tbl.getTableDef().GetCols()
	dataLength := len(cols) - 1

	tableTypes := make([]uint8, dataLength)

	tableVal := make([][2]any, dataLength)
	zms := make([]objectio.ZoneMap, dataLength)

	var (
		err  error
		init bool
		meta objectio.ObjectMeta
	)
	for _, blks := range tbl.blockInfos {
		for _, blk := range blks {
			location := blk.MetaLocation()
			if objectio.IsSameObjectLocVsMeta(location, meta) {
				continue
			} else {
				if meta, err = loadObjectMeta(ctx, location, tbl.db.txn.proc.FileService, tbl.db.txn.proc.Mp()); err != nil {
					return nil, nil, err
				}
			}
			if !init {
				for idx := range zms {
					zms[idx] = meta.ObjectColumnMeta(uint16(idx)).ZoneMap()
					tableTypes[idx] = uint8(cols[idx].Typ.Id)
				}

				init = true
			} else {
				for idx := range zms {
					zm := meta.ObjectColumnMeta(uint16(idx)).ZoneMap()
					index.UpdateZM(&zms[idx], zm.GetMaxBuf())
					index.UpdateZM(&zms[idx], zm.GetMinBuf())
				}
			}
		}
	}

	for idx, zm := range zms {
		tableVal[idx] = [2]any{zm.GetMin(), zm.GetMax()}
	}

	return tableVal, tableTypes, nil
}

func (tbl *txnTable) Size(ctx context.Context, name string) (int64, error) {
	// TODO
	return 0, nil
}

func (tbl *txnTable) LoadDeletesForBlock(blockID string, deleteBlockId map[types.Blockid][]int, deletesRowId map[types.Rowid]uint8) error {
	for _, bat := range tbl.db.txn.blockId_dn_delete_metaLoc_batch[blockID] {
		vs := vector.MustStrCol(bat.GetVector(0))
		for _, metalLoc := range vs {
			location, err := blockio.EncodeLocationFromString(metalLoc)
			if err != nil {
				return err
			}
			s3BlockReader, err := blockio.NewObjectReader(tbl.db.txn.engine.fs, location)
			if err != nil {
				return err
			}
			rowIdBat, err := s3BlockReader.LoadColumns(tbl.db.txn.proc.Ctx, []uint16{0}, location.ID(), tbl.db.txn.proc.GetMPool())
			if err != nil {
				return err
			}
			rowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
			for _, rowId := range rowIds {
				if deleteBlockId != nil {
					id, offset := rowId.Decode()
					deleteBlockId[id] = append(deleteBlockId[id], int(offset))
				} else if deletesRowId != nil {
					deletesRowId[rowId] = 0
				} else {
					panic("Load Block Deletes Error")
				}
			}
		}
	}
	return nil
}

func (tbl *txnTable) GetEngineType() engine.EngineType {
	return engine.Disttae
}

func (tbl *txnTable) reset(newId uint64) {
	tbl.tableId = newId
	tbl._parts = nil
	tbl.blockInfos = nil
	tbl.modifiedBlocks = nil
	tbl.blockInfosUpdated = false
	tbl.localState = NewPartitionState(true)
}

// return all unmodified blocks
func (tbl *txnTable) Ranges(ctx context.Context, expr *plan.Expr) (ranges [][]byte, err error) {
	tbl.db.txn.Lock()
	tbl.writes = tbl.writes[:0]
	tbl.writesOffset = len(tbl.db.txn.writes)
	for i, entry := range tbl.db.txn.writes {
		if entry.databaseId != tbl.db.databaseId {
			continue
		}
		if entry.tableId != tbl.tableId {
			continue
		}
		tbl.writes = append(tbl.writes, tbl.db.txn.writes[i])
	}
	tbl.db.txn.Unlock()

	if err = tbl.updateBlockInfos(ctx, expr); err != nil {
		return
	}
	parts, err := tbl.getParts(ctx)
	if err != nil {
		return
	}

	ranges = make([][]byte, 0, 1)
	ranges = append(ranges, []byte{})
	if len(tbl.blockInfos) == 0 {
		return
	}
	tbl.modifiedBlocks = make([][]ModifyBlockMeta, len(tbl.blockInfos))

	exprMono := plan2.CheckExprIsMonotonic(tbl.db.txn.proc.Ctx, expr)
	columnMap, columns, maxCol := plan2.GetColumnsByExpr(expr, tbl.getTableDef())
	for _, i := range tbl.dnList {
		blocks := tbl.blockInfos[i]
		deletes := make(map[types.Blockid][]int)
		if len(blocks) > 0 {
			ts := tbl.db.txn.meta.SnapshotTS
			ids := make([]types.Blockid, len(blocks))
			for i := range blocks {
				// if cn can see a appendable block, this block must contain all updates
				// in cache, no need to do merge read, BlockRead will filter out
				// invisible and deleted rows with respect to the timestamp
				if !blocks[i].EntryState {
					if blocks[i].CommitTs.ToTimestamp().Less(ts) { // hack
						ids[i] = blocks[i].BlockID
					}
				}
			}

			for _, blockID := range ids {
				ts := types.TimestampToTS(ts)
				iter := parts[i].NewRowsIter(ts, &blockID, true)
				for iter.Next() {
					entry := iter.Entry()
					id, offset := entry.RowID.Decode()
					deletes[id] = append(deletes[id], int(offset))
				}
				iter.Close()
				// DN flush deletes rowids block
				if err = tbl.LoadDeletesForBlock(string(blockID[:]), deletes, nil); err != nil {
					return
				}
			}
			for _, entry := range tbl.writes {
				// rawBatch detele rowId for Dn block
				if entry.typ == DELETE && entry.fileName == "" {
					vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
					for _, v := range vs {
						id, offset := v.Decode()
						deletes[id] = append(deletes[id], int(offset))
					}
				}
			}
		}
		var (
			meta  objectio.ObjectMeta
			mblks []ModifyBlockMeta
		)
		hasDeletes := len(deletes) > 0
		for _, blk := range blocks {
			need := true
			if exprMono {
				location := blk.MetaLocation()
				if !objectio.IsSameObjectLocVsMeta(location, meta) {
					if meta, err = loadObjectMeta(ctx, location, tbl.db.txn.proc.FileService, tbl.db.txn.proc.Mp()); err != nil {
						return
					}
				}
				need = needRead(ctx, expr, meta, blk, tbl.getTableDef(), columnMap, columns, maxCol, tbl.db.txn.proc)
			}

			if !need {
				continue
			}

			if hasDeletes {
				if rows, ok := deletes[blk.BlockID]; ok {
					mblks = append(mblks, ModifyBlockMeta{blk, rows})
				} else {
					ranges = append(ranges, blockInfoMarshal(blk))
				}
			} else {
				ranges = append(ranges, blockInfoMarshal(blk))
			}
		}
		tbl.modifiedBlocks[i] = mblks
	}

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
				})
				i++
			}
		}
		tbl.tableDef = &plan.TableDef{
			Name:          tbl.tableName,
			Cols:          cols,
			Name2ColIndex: name2index,
		}
	}
	return tbl.tableDef
}

func (tbl *txnTable) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	//return tbl.defs, nil
	// I don't understand why the logic now is not to get all the tableDef. Don't understand.
	// copy from tae's logic
	defs := make([]engine.TableDef, 0, len(tbl.defs))
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
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, tbl.db.txn.dnStores[0], -1); err != nil {
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

func (tbl *txnTable) getCbName() string {
	if tbl.clusterByIdx == -1 {
		return ""
	} else {
		return tbl.tableDef.Cols[tbl.clusterByIdx].Name
	}
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
	if bat == nil {
		return nil
	}

	// Write S3 Block
	if bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
		location, err := blockio.EncodeLocationFromString(bat.Vecs[0].GetStringAt(0))
		if err != nil {
			return err
		}
		fileName := location.Name().String()
		ibat := batch.New(true, bat.Attrs)
		for j := range bat.Vecs {
			ibat.SetVector(int32(j), vector.NewVec(*bat.GetVector(int32(j)).GetType()))
		}
		if _, err := ibat.Append(ctx, tbl.db.txn.proc.Mp(), bat); err != nil {
			return err
		}
		return tbl.db.txn.WriteFile(INSERT, tbl.db.databaseId, tbl.tableId, tbl.db.databaseName, tbl.tableName, fileName, ibat, tbl.db.txn.dnStores[0])
	}
	ibat := batch.NewWithSize(len(bat.Attrs))
	ibat.SetAttributes(bat.Attrs)
	for j := range bat.Vecs {
		ibat.SetVector(int32(j), vector.NewVec(*bat.GetVector(int32(j)).GetType()))
	}
	if _, err := ibat.Append(ctx, tbl.db.txn.proc.Mp(), bat); err != nil {
		return err
	}
	if err := tbl.db.txn.WriteBatch(INSERT, tbl.db.databaseId, tbl.tableId,
		tbl.db.databaseName, tbl.tableName, ibat, tbl.db.txn.dnStores[0], tbl.primaryIdx); err != nil {
		return err
	}
	packer, put := tbl.db.txn.engine.packerPool.Get()
	defer put()
	if err := tbl.updateLocalState(ctx, INSERT, ibat, packer); err != nil {
		return err
	}
	return nil
	// return tbl.db.txn.DumpBatch(false, tbl.writesOffset)
}

func (tbl *txnTable) Update(ctx context.Context, bat *batch.Batch) error {
	return nil
}

//	blkId(string)     deltaLoc(string)                   type(int)
//
// |-----------|-----------------------------------|----------------|
// |  blk_id   |   batch.Marshal(metaLoc)          |  FlushMetaLoc  | DN Block
// |  blk_id   |   batch.Marshal(uint32 offset)    |  CNBlockOffset | CN Block
// |  blk_id   |   batch.Marshal(rowId)            |  RawRowIdBatch | DN Blcok
// |  blk_id   |   batch.Marshal(uint32 offset)    | RawBatchOffset | RawBatch (in txn workspace)
func (tbl *txnTable) EnhanceDelete(bat *batch.Batch, name string) error {
	blkId, typ_str := name[:len(name)-2], string(name[len(name)-1])
	typ, err := strconv.ParseInt(typ_str, 10, 64)
	if err != nil {
		return err
	}
	switch typ {
	case deletion.FlushMetaLoc:
		location, err := blockio.EncodeLocationFromString(bat.Vecs[0].GetStringAt(0))
		if err != nil {
			return err
		}
		fileName := location.Name().String()
		copBat := CopyBatch(tbl.db.txn.proc.Ctx, tbl.db.txn.proc, bat)
		if err := tbl.db.txn.WriteFile(DELETE, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, fileName, copBat, tbl.db.txn.dnStores[0]); err != nil {
			return err
		}
		tbl.db.txn.blockId_dn_delete_metaLoc_batch[blkId] = append(tbl.db.txn.blockId_dn_delete_metaLoc_batch[blkId], copBat)
	case deletion.CNBlockOffset:
		vs := vector.MustFixedCol[int64](bat.GetVector(0))
		tbl.db.txn.PutCnBlockDeletes(blkId, vs)
	case deletion.RawRowIdBatch:
		tbl.writeDnPartition(tbl.db.txn.proc.Ctx, bat)
	case deletion.RawBatchOffset:
		vs := vector.MustFixedCol[int64](bat.GetVector(0))
		entry_bat := tbl.db.txn.blockId_raw_batch[blkId]
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
	batchNums := 0
	name, err := s3writer.GenerateWriter(tbl.db.txn.proc)
	if err != nil {
		return err
	}

	var deletedIDs []string
	defer tbl.db.txn.deletedBlocks.removeBlockDeletedInfos(deletedIDs)
	tbl.db.txn.deletedBlocks.iter(func(id string, deleteOffsets []int64) bool {
		blkId := types.Blockid(*(*[20]byte)([]byte(id)))
		pos := tbl.db.txn.cnBlkId_Pos[string(blkId[:])]
		// just do compaction for current txnTable
		entry := tbl.db.txn.writes[pos.idx]
		if !(entry.databaseId == tbl.db.databaseId && entry.tableId == tbl.tableId) {
			return true
		}
		delete(tbl.db.txn.cnBlkId_Pos, string(blkId[:]))
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
		s3BlockReader, e := blockio.NewObjectReader(tbl.db.txn.engine.fs, location)
		if e != nil {
			err = e
			return false
		}
		if tbl.idxs == nil {
			idxs := make([]uint16, 0, len(tbl.tableDef.Cols)-1)
			for i := 0; i < len(tbl.tableDef.Cols)-1; i++ {
				idxs = append(idxs, uint16(i))
			}
			tbl.idxs = idxs
		}
		bat, e := s3BlockReader.LoadColumns(tbl.db.txn.proc.Ctx, tbl.idxs, location.ID(), tbl.db.txn.proc.GetMPool())
		if e != nil {
			err = e
			return false
		}
		bat.SetZs(bat.GetVector(0).Length(), tbl.db.txn.proc.GetMPool())
		bat.AntiShrink(deleteOffsets)
		if bat.Length() == 0 {
			return true
		}
		// ToDo: Optimize this logic, we need to control blocks num in one file
		// and make sure one block has as close as possible to 8192 rows
		// if the batch is little we should not flush, improve this in next pr.
		s3writer.WriteBlock(bat)
		batchNums++
		return true
	})
	if err != nil {
		return err
	}

	if batchNums > 0 {
		metaLocs, err := s3writer.WriteEndBlocks(tbl.db.txn.proc)
		if err != nil {
			return err
		}
		new_bat := batch.New(false, []string{catalog.BlockMeta_MetaLoc})
		new_bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
		for _, metaLoc := range metaLocs {
			vector.AppendBytes(new_bat.GetVector(0), []byte(metaLoc), false, tbl.db.txn.proc.GetMPool())
		}
		new_bat.SetZs(len(metaLocs), tbl.db.txn.proc.GetMPool())
		err = tbl.db.txn.WriteFile(INSERT, tbl.db.databaseId, tbl.tableId, tbl.db.databaseName, tbl.tableName, name.String(), new_bat, tbl.db.txn.dnStores[0])
		if err != nil {
			return err
		}
	}
	remove_batch := make(map[*batch.Batch]bool)
	// delete old block info
	for idx, offsets := range mp {
		bat := tbl.db.txn.writes[idx].bat
		bat.AntiShrink(offsets)
		// update txn.cnBlkId_Pos
		tbl.db.txn.updatePosForCNBlock(bat.GetVector(0), idx)
		if bat.Length() == 0 {
			remove_batch[bat] = true
		}
	}
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
	// remoteDelete
	if name != catalog.Row_ID {
		return tbl.EnhanceDelete(bat, name)
	}
	bat.SetAttributes([]string{catalog.Row_ID})

	packer, put := tbl.db.txn.engine.packerPool.Get()
	defer put()
	if err := tbl.updateLocalState(ctx, DELETE, bat, packer); err != nil {
		return err
	}
	bat = tbl.db.txn.deleteBatch(bat, tbl.db.databaseId, tbl.tableId)
	if bat.Length() == 0 {
		return nil
	}
	return tbl.writeDnPartition(ctx, bat)
}

func CopyBatch(ctx context.Context, proc *process.Process, bat *batch.Batch) *batch.Batch {
	ibat := batch.New(true, bat.Attrs)
	for i := 0; i < len(ibat.Attrs); i++ {
		ibat.SetVector(int32(i), vector.NewVec(*bat.GetVector(int32(i)).GetType()))
	}
	ibat.Append(ctx, proc.GetMPool(), bat)
	return ibat
}

func (tbl *txnTable) writeDnPartition(ctx context.Context, bat *batch.Batch) error {
	ibat := CopyBatch(ctx, tbl.db.txn.proc, bat)
	if err := tbl.db.txn.WriteBatch(DELETE, tbl.db.databaseId, tbl.tableId,
		tbl.db.databaseName, tbl.tableName, ibat, tbl.db.txn.dnStores[0], tbl.primaryIdx); err != nil {
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

func (tbl *txnTable) NewReader(ctx context.Context, num int, expr *plan.Expr, ranges [][]byte) ([]engine.Reader, error) {
	if len(ranges) == 0 {
		return tbl.newMergeReader(ctx, num, expr)
	}
	if len(ranges) == 1 && engine.IsMemtable(ranges[0]) {
		return tbl.newMergeReader(ctx, num, expr)
	}
	if len(ranges) > 1 && engine.IsMemtable(ranges[0]) {
		rds := make([]engine.Reader, num)
		mrds := make([]mergeReader, num)
		rds0, err := tbl.newMergeReader(ctx, num, expr)
		if err != nil {
			return nil, err
		}
		for i := range rds0 {
			mrds[i].rds = append(mrds[i].rds, rds0[i])
		}
		rds0, err = tbl.newBlockReader(ctx, num, expr, ranges[1:])
		if err != nil {
			return nil, err
		}
		for i := range rds0 {
			mrds[i].rds = append(mrds[i].rds, rds0[i])
		}
		for i := range rds {
			rds[i] = &mrds[i]
		}
		return rds, nil
	}
	return tbl.newBlockReader(ctx, num, expr, ranges)
}

func (tbl *txnTable) newMergeReader(ctx context.Context, num int,
	expr *plan.Expr) ([]engine.Reader, error) {

	var encodedPrimaryKey []byte
	if tbl.primaryIdx >= 0 && expr != nil {
		pkColumn := tbl.tableDef.Cols[tbl.primaryIdx]
		ok, v := getPkValueByExpr(expr, pkColumn.Name, types.T(pkColumn.Typ.Id))
		if ok {
			packer, put := tbl.db.txn.engine.packerPool.Get()
			defer put()
			encodedPrimaryKey = encodePrimaryKey(v, packer)
		}
	}

	rds := make([]engine.Reader, num)
	mrds := make([]mergeReader, num)
	for _, i := range tbl.dnList {
		var blks []ModifyBlockMeta

		if len(tbl.blockInfos) > 0 {
			blks = tbl.modifiedBlocks[i]
		}
		rds0, err := tbl.newReader(
			ctx,
			i,
			num,
			encodedPrimaryKey,
			blks,
			tbl.writes,
		)
		if err != nil {
			return nil, err
		}
		mrds[i].rds = append(mrds[i].rds, rds0...)
	}

	for i := range rds {
		rds[i] = &mrds[i]
	}

	return rds, nil
}

func (tbl *txnTable) newBlockReader(ctx context.Context, num int, expr *plan.Expr, ranges [][]byte) ([]engine.Reader, error) {
	rds := make([]engine.Reader, num)
	blks := make([]*catalog.BlockInfo, len(ranges))
	for i := range ranges {
		blks[i] = BlockInfoUnmarshal(ranges[i])
	}
	ts := tbl.db.txn.meta.SnapshotTS
	tableDef := tbl.getTableDef()

	if len(ranges) < num || len(ranges) == 1 {
		for i := range ranges {
			rds[i] = &blockReader{
				fs:         tbl.db.txn.engine.fs,
				tableDef:   tableDef,
				primaryIdx: tbl.primaryIdx,
				expr:       expr,
				ts:         ts,
				ctx:        ctx,
				blks:       []*catalog.BlockInfo{blks[i]},
			}
		}
		for j := len(ranges); j < num; j++ {
			rds[j] = &emptyReader{}
		}
		return rds, nil
	}

	infos, steps := groupBlocksToObjects(blks, num)
	blockReaders := newBlockReaders(ctx, tbl.db.txn.engine.fs, tableDef, tbl.primaryIdx, ts, num, expr)
	distributeBlocksToBlockReaders(blockReaders, num, infos, steps)
	for i := 0; i < num; i++ {
		rds[i] = blockReaders[i]
	}
	return rds, nil
}

func (tbl *txnTable) newReader(
	ctx context.Context,
	partitionIndex int,
	readerNumber int,
	encodedPrimaryKey []byte,
	blks []ModifyBlockMeta,
	entries []Entry,
) ([]engine.Reader, error) {

	txn := tbl.db.txn
	ts := txn.meta.SnapshotTS
	fs := txn.engine.fs

	inserts := make([]*batch.Batch, 0, len(entries))
	deletes := make(map[types.Rowid]uint8)
	for _, entry := range entries {
		if entry.typ == INSERT {
			inserts = append(inserts, entry.bat)
		} else {
			if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					deletes[v] = 0
				}
			}
		}
	}
	// get append block deletes rowids
	non_append_block := make(map[string]bool)
	if len(tbl.blockInfos) > 0 {
		for _, blk := range tbl.blockInfos[0] {
			// append non_append_block
			if !blk.EntryState {
				non_append_block[string(blk.BlockID[:])] = true
			}
		}
	}

	for blkId := range tbl.db.txn.blockId_dn_delete_metaLoc_batch {
		if !non_append_block[blkId] {
			tbl.LoadDeletesForBlock(blkId, nil, deletes)
		}
	}
	readers := make([]engine.Reader, readerNumber)

	mp := make(map[string]types.Type)
	colIdxMp := make(map[string]int)
	if tbl.tableDef != nil {
		for i := range tbl.tableDef.Cols {
			colIdxMp[tbl.tableDef.Cols[i].Name] = i
		}
	}

	mp[catalog.Row_ID] = types.New(types.T_Rowid, 0, 0)
	for _, def := range tbl.defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		mp[attr.Attr.Name] = attr.Attr.Type
	}

	parts, err := tbl.getParts(ctx)
	if err != nil {
		return nil, err
	}

	var iter partitionStateIter
	if len(encodedPrimaryKey) > 0 {
		iter = parts[partitionIndex].NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			encodedPrimaryKey,
		)
	} else {
		iter = parts[partitionIndex].NewRowsIter(
			types.TimestampToTS(ts),
			nil,
			false,
		)
	}

	partReader := &PartitionReader{
		typsMap:         mp,
		inserts:         inserts,
		deletes:         deletes,
		iter:            iter,
		colIdxMp:        colIdxMp,
		extendId2s3File: make(map[string]int),
		s3FileService:   fs,
		procMPool:       txn.proc.GetMPool(),
		deletedBlocks:   txn.deletedBlocks,
	}
	readers[0] = partReader

	if readerNumber == 1 {
		for i := range blks {
			readers = append(readers, &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tbl.tableDef,
				sels:     make([]int64, 0, 1024),
				blks:     []ModifyBlockMeta{blks[i]},
			})
		}
		return []engine.Reader{&mergeReader{readers}}, nil
	}

	if len(blks) < readerNumber-1 {
		for i := range blks {
			readers[i+1] = &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tbl.tableDef,
				sels:     make([]int64, 0, 1024),
				blks:     []ModifyBlockMeta{blks[i]},
			}
		}
		for j := len(blks) + 1; j < readerNumber; j++ {
			readers[j] = &emptyReader{}
		}
		return readers, nil
	}

	step := len(blks) / (readerNumber - 1)
	if step < 1 {
		step = 1
	}
	for i := 1; i < readerNumber; i++ {
		if i == readerNumber-1 {
			readers[i] = &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tbl.tableDef,
				blks:     blks[(i-1)*step:],
				sels:     make([]int64, 0, 1024),
			}
		} else {
			readers[i] = &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tbl.tableDef,
				blks:     blks[(i-1)*step : i*step],
				sels:     make([]int64, 0, 1024),
			}
		}
	}

	return readers, nil
}

func (tbl *txnTable) updateLocalState(
	ctx context.Context,
	typ int,
	bat *batch.Batch,
	packer *types.Packer,
) (
	err error,
) {

	if bat.Vecs[0].GetType().Oid != types.T_Rowid {
		// skip
		return nil
	}

	if tbl.primaryIdx < 0 {
		// no primary key, skip
		return nil
	}

	// hide primary key, auto_incr, nevery dedup.
	if tbl.tableDef != nil {
		pk := tbl.tableDef.Cols[tbl.primaryIdx]
		if pk.Hidden && pk.Typ.AutoIncr {
			return nil
		}
	}

	if tbl.localState == nil {
		tbl.localState = NewPartitionState(true)
	}

	// make a logtail compatible batch
	protoBatch, err := batch.BatchToProtoBatch(bat)
	if err != nil {
		panic(err)
	}
	vec := vector.NewVec(types.T_TS.ToType())
	ts := types.TimestampToTS(tbl.nextLocalTS())
	for i, l := 0, bat.Length(); i < l; i++ {
		if err := vector.AppendFixed(
			vec,
			ts,
			false,
			tbl.db.txn.proc.Mp(),
		); err != nil {
			panic(err)
		}
	}
	protoVec, err := vector.VectorToProtoVector(vec)
	if err != nil {
		panic(err)
	}
	newAttrs := make([]string, 0, len(protoBatch.Attrs)+1)
	newAttrs = append(newAttrs, protoBatch.Attrs[:1]...)
	newAttrs = append(newAttrs, "name")
	newAttrs = append(newAttrs, protoBatch.Attrs[1:]...)
	protoBatch.Attrs = newAttrs
	newVecs := make([]*api.Vector, 0, len(protoBatch.Vecs)+1)
	newVecs = append(newVecs, protoBatch.Vecs[:1]...)
	newVecs = append(newVecs, protoVec)
	newVecs = append(newVecs, protoBatch.Vecs[1:]...)
	protoBatch.Vecs = newVecs

	switch typ {

	case INSERT:
		primaryKeys := tbl.localState.HandleRowsInsert(ctx, protoBatch, tbl.primaryIdx, packer)

		// check primary key
		for idx, primaryKey := range primaryKeys {
			iter := tbl.localState.NewPrimaryKeyIter(ts, primaryKey)
			n := 0
			for iter.Next() {
				n++
			}
			iter.Close()
			if n > 1 {
				primaryKeyVector := bat.Vecs[tbl.primaryIdx+1 /* skip the first row id column */]
				nullableVec := memorytable.VectorAt(primaryKeyVector, idx)
				return moerr.NewDuplicateEntry(
					ctx,
					common.TypeStringValue(
						*primaryKeyVector.GetType(),
						nullableVec.Value, nullableVec.IsNull,
					),
					bat.Attrs[tbl.primaryIdx+1],
				)
			}
		}

	case DELETE:
		tbl.localState.HandleRowsDelete(ctx, protoBatch)

	default:
		panic(fmt.Sprintf("unknown type: %v", typ))

	}

	return
}

func (tbl *txnTable) nextLocalTS() timestamp.Timestamp {
	tbl.localTS = tbl.localTS.Next()
	return tbl.localTS
}

func (tbl *txnTable) getParts(ctx context.Context) ([]*PartitionState, error) {
	if tbl._parts == nil {
		if err := tbl.updateLogtail(ctx); err != nil {
			return nil, err
		}
		tbl._parts = tbl.db.txn.engine.getPartitions(tbl.db.databaseId, tbl.tableId).Snapshot()
	}
	return tbl._parts, nil
}

func (tbl *txnTable) updateBlockInfos(ctx context.Context, expr *plan.Expr) error {
	tbl.dnList = []int{0}
	_, created := tbl.db.txn.createMap.Load(genTableKey(ctx, tbl.tableName, tbl.db.databaseId))
	if !created && !tbl.blockInfosUpdated {
		if err := tbl.updateLogtail(ctx); err != nil {
			return err
		}
		blocks, err := tbl.db.txn.getBlockInfos(ctx, tbl)
		if err != nil {
			return err
		}
		tbl.blockInfos = blocks
		tbl.blockInfosUpdated = true
	}
	return nil
}

func (tbl *txnTable) updateLogtail(ctx context.Context) error {
	_, created := tbl.db.txn.createMap.Load(genTableKey(ctx, tbl.tableName, tbl.db.databaseId))
	if created {
		return nil
	}

	if tbl.logtailUpdated {
		return nil
	}

	if tbl.db.txn.engine.UsePushModelOrNot() {
		if err := tbl.db.txn.engine.UpdateOfPush(ctx, tbl.db.databaseId, tbl.tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
			return err
		}
		err := tbl.db.txn.engine.lazyLoad(ctx, tbl)
		if err != nil {
			return err
		}
	} else {
		if err := tbl.db.txn.engine.UpdateOfPull(ctx, tbl.db.txn.dnStores[:1], tbl, tbl.db.txn.op, tbl.primaryIdx,
			tbl.db.databaseId, tbl.tableId, tbl.db.txn.meta.SnapshotTS); err != nil {
			return err
		}
	}

	tbl.logtailUpdated = true
	return nil
}
