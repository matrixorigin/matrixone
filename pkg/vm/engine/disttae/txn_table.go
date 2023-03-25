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
	"math/rand"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
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
	if tbl.meta == nil || !tbl.updated {
		err := tbl.updateMeta(ctx, expr)
		if err != nil {
			return plan2.DefaultStats(), err
		}
	}
	if tbl.meta != nil {
		return CalcStats(ctx, &tbl.meta.blocks, expr, tbl.getTableDef(), tbl.db.txn.proc, tbl.getCbName(), s)
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
	for _, part := range tbl.parts {
		iter := part.NewRowsIter(ts, nil, false)
		for iter.Next() {
			entry := iter.Entry()
			if _, ok := deletes[entry.RowID]; ok {
				continue
			}
			if tbl.skipBlocks != nil {
				if _, ok := tbl.skipBlocks[entry.BlockID]; ok {
					continue
				}
			}
			rows++
		}
		iter.Close()
	}

	if tbl.meta != nil {
		for _, blks := range tbl.meta.blocks {
			for _, blk := range blks {
				rows += blockRows(blk)
			}
		}
	}

	return rows, nil
}

func (tbl *txnTable) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	cols := tbl.getTableDef().GetCols()
	dataLength := len(cols) - 1
	//dateType of each column for table
	tableTypes := make([]uint8, dataLength)
	dataTypes := make([]types.Type, dataLength)

	columns := make([]int, dataLength)
	for i := 0; i < dataLength; i++ {
		columns[i] = i
	}
	//minimum --- maximum
	tableVal := make([][2]any, dataLength)

	if tbl.meta == nil {
		return nil, nil, moerr.NewInvalidInputNoCtx("table meta is nil")
	}

	var init bool
	for _, blks := range tbl.meta.blocks {
		for _, blk := range blks {
			blkVal, blkTypes, err := getZonemapDataFromMeta(columns, blk, tbl.getTableDef())
			if err != nil {
				return nil, nil, err
			}

			if !init {
				//init the tableVal
				init = true

				for i := range blkVal {
					tableVal[i][0] = blkVal[i][0]
					tableVal[i][1] = blkVal[i][1]
					dataTypes[i] = types.T(blkTypes[i]).ToType()
				}

				tableTypes = blkTypes
			} else {
				for i := range blkVal {
					if compute.CompareGeneric(blkVal[i][0], tableVal[i][0], dataTypes[i]) < 0 {
						tableVal[i][0] = blkVal[i][0]
					}

					if compute.CompareGeneric(blkVal[i][1], tableVal[i][1], dataTypes[i]) > 0 {
						tableVal[i][1] = blkVal[i][1]
					}
				}
			}
		}
	}
	return tableVal, tableTypes, nil
}

func (tbl *txnTable) Size(ctx context.Context, name string) (int64, error) {
	// TODO
	return 0, nil
}

// return all unmodified blocks
func (tbl *txnTable) Ranges(ctx context.Context, expr *plan.Expr) ([][]byte, error) {
	if err := tbl.db.txn.DumpBatch(false, 0); err != nil {
		return nil, err
	}
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

	err := tbl.updateMeta(ctx, expr)
	if err != nil {
		return nil, err
	}

	tbl.parts = tbl.db.txn.engine.getPartitions(tbl.db.databaseId, tbl.tableId).Snapshot()

	ranges := make([][]byte, 0, 1)
	ranges = append(ranges, []byte{})
	tbl.skipBlocks = make(map[types.Blockid]uint8)
	if tbl.meta == nil {
		return ranges, nil
	}
	tbl.meta.modifedBlocks = make([][]ModifyBlockMeta, len(tbl.meta.blocks))

	exprMono := plan2.CheckExprIsMonotonic(tbl.db.txn.proc.Ctx, expr)
	columnMap, columns, maxCol := plan2.GetColumnsByExpr(expr, tbl.getTableDef())
	for _, i := range tbl.dnList {
		blocks := tbl.meta.blocks[i]
		blks := make([]BlockMeta, 0, len(blocks))
		deletes := make(map[types.Blockid][]int)
		if len(blocks) > 0 {
			ts := tbl.db.txn.meta.SnapshotTS
			ids := make([]types.Blockid, len(blocks))
			for i := range blocks {
				// if cn can see a appendable block, this block must contain all updates
				// in cache, no need to do merge read, BlockRead will filter out
				// invisible and deleted rows with respect to the timestamp
				if !blocks[i].Info.EntryState {
					if blocks[i].Info.CommitTs.ToTimestamp().Less(ts) { // hack
						ids[i] = blocks[i].Info.BlockID
					}
				}
			}

			for _, blockID := range ids {
				ts := types.TimestampToTS(ts)
				iter := tbl.parts[i].NewRowsIter(ts, &blockID, true)
				for iter.Next() {
					entry := iter.Entry()
					id, offset := entry.RowID.Decode()
					deletes[id] = append(deletes[id], int(offset))
				}
				iter.Close()
			}

			for _, entry := range tbl.writes {
				if entry.typ == DELETE {
					vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
					for _, v := range vs {
						id, offset := v.Decode()
						deletes[id] = append(deletes[id], int(offset))
					}
				}
			}
			for i := range blocks {
				if _, ok := deletes[blocks[i].Info.BlockID]; !ok {
					blks = append(blks, blocks[i])
				}
			}
		}
		for _, blk := range blks {
			tbl.skipBlocks[blk.Info.BlockID] = 0
			if !exprMono || needRead(ctx, expr, blk, tbl.getTableDef(), columnMap, columns, maxCol, tbl.db.txn.proc) {
				ranges = append(ranges, blockMarshal(blk))
			}
		}
		tbl.meta.modifedBlocks[i] = genModifedBlocks(ctx, deletes,
			tbl.meta.blocks[i], blks, expr, tbl.getTableDef(), tbl.db.txn.proc)
	}
	return ranges, nil
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
	if tbl.partition != "" {
		partitionDef := new(engine.PartitionDef)
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
		fileName := strings.Split(bat.Vecs[0].GetStringAt(0), ":")[0]
		ibat := batch.New(true, bat.Attrs)
		for j := range bat.Vecs {
			ibat.SetVector(int32(j), vector.NewVec(*bat.GetVector(int32(j)).GetType()))
		}
		if _, err := ibat.Append(ctx, tbl.db.txn.proc.Mp(), bat); err != nil {
			return err
		}
		i := rand.Int() % len(tbl.db.txn.dnStores)
		return tbl.db.txn.WriteFile(INSERT, tbl.db.databaseId, tbl.tableId, tbl.db.databaseName, tbl.tableName, fileName, ibat, tbl.db.txn.dnStores[i])
	}
	ibat := batch.New(true, bat.Attrs)
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
	return tbl.db.txn.DumpBatch(false, tbl.writesOffset)
}

func (tbl *txnTable) Update(ctx context.Context, bat *batch.Batch) error {
	return nil
}

func (tbl *txnTable) Delete(ctx context.Context, bat *batch.Batch, name string) error {
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
	bats, err := partitionDeleteBatch(tbl, bat)
	if err != nil {
		return err
	}
	for i := range bats {
		if bats[i].Length() == 0 {
			continue
		}
		if err := tbl.db.txn.WriteBatch(DELETE, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, bats[i], tbl.db.txn.dnStores[i], tbl.primaryIdx); err != nil {
			return err
		}
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

		if tbl.meta != nil {
			blks = tbl.meta.modifedBlocks[i]
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
	blks := make([]BlockMeta, len(ranges))
	for i := range ranges {
		blks[i] = blockUnmarshal(ranges[i])
	}
	ts := tbl.db.txn.meta.SnapshotTS
	tableDef := tbl.getTableDef()

	if len(ranges) < num {
		for i := range ranges {
			rds[i] = &blockReader{
				fs:         tbl.db.txn.engine.fs,
				tableDef:   tableDef,
				primaryIdx: tbl.primaryIdx,
				expr:       expr,
				ts:         ts,
				ctx:        ctx,
				blks:       []BlockMeta{blks[i]},
			}
		}
		for j := len(ranges); j < num; j++ {
			rds[j] = &emptyReader{}
		}
		return rds, nil
	}
	step := (len(ranges)) / num
	if step < 1 {
		step = 1
	}
	for i := 0; i < num; i++ {
		if i == num-1 {
			rds[i] = &blockReader{
				fs:         tbl.db.txn.engine.fs,
				tableDef:   tableDef,
				primaryIdx: tbl.primaryIdx,
				expr:       expr,
				ts:         ts,
				ctx:        ctx,
				blks:       blks[i*step:],
			}
		} else {
			rds[i] = &blockReader{
				fs:         tbl.db.txn.engine.fs,
				tableDef:   tableDef,
				primaryIdx: tbl.primaryIdx,
				expr:       expr,
				ts:         ts,
				ctx:        ctx,
				blks:       blks[i*step : (i+1)*step],
			}
		}
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

	var iter partitionStateIter
	if len(encodedPrimaryKey) > 0 {
		iter = tbl.parts[partitionIndex].NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			encodedPrimaryKey,
		)
	} else {
		iter = tbl.parts[partitionIndex].NewRowsIter(
			types.TimestampToTS(ts),
			nil,
			false,
		)
	}

	partReader := &PartitionReader{
		typsMap:         mp,
		inserts:         inserts,
		deletes:         deletes,
		skipBlocks:      tbl.skipBlocks,
		iter:            iter,
		colIdxMp:        colIdxMp,
		extendId2s3File: make(map[string]int),
		s3FileService:   fs,
		procMPool:       txn.proc.GetMPool(),
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

	var state *PartitionState
	if tbl.localState == nil {
		tbl.localState = NewPartitionState()
		state = tbl.localState
	} else {
		state = tbl.localState.Copy()
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
		primaryKeys := state.HandleRowsInsert(ctx, protoBatch, tbl.primaryIdx, packer)

		// check primary key
		for idx, primaryKey := range primaryKeys {
			iter := state.NewPrimaryKeyIter(ts, primaryKey)
			n := 0
			for iter.Next() {
				n++
			}
			iter.Close()
			if n > 1 {
				primaryKeyVector := bat.Vecs[tbl.primaryIdx+1 /* skip the first row id column */]
				return moerr.NewDuplicateEntry(
					ctx,
					common.TypeStringValue(
						*primaryKeyVector.GetType(),
						memorytable.VectorAt(primaryKeyVector, idx).Value,
					),
					bat.Attrs[tbl.primaryIdx+1],
				)
			}
		}

	case DELETE:
		state.HandleRowsDelete(ctx, protoBatch)

	default:
		panic(fmt.Sprintf("unknown type: %v", typ))

	}

	tbl.localState = state

	return
}

func (tbl *txnTable) nextLocalTS() timestamp.Timestamp {
	tbl.localTS = tbl.localTS.Next()
	return tbl.localTS
}
