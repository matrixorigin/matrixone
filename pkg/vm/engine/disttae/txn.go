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
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func (txn *Transaction) getBlockInfos(
	ctx context.Context,
	tbl *txnTable,
) (blocks []catalog.BlockInfo, err error) {
	ts := types.TimestampToTS(txn.meta.SnapshotTS)
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	var objectName objectio.ObjectNameShort
	iter := state.NewBlocksIter(ts)
	fs, err := fileservice.Get[fileservice.FileService](txn.proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	for iter.Next() {
		entry := iter.Entry()
		location := entry.MetaLocation()
		if !objectio.IsSameObjectLocVsShort(location, &objectName) {
			// Prefetch object meta
			if err = blockio.PrefetchMeta(fs, location); err != nil {
				iter.Close()
				return
			}
			objectName = *location.Name().Short()
		}
		blocks = append(blocks, entry.BlockInfo)
	}
	iter.Close()
	return
}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly.Load()
}

// WriteBatch used to write data to the transaction buffer
// insert/delete/update all use this api
// insertBatchHasRowId : it denotes the batch has Rowid when the typ is INSERT.
// if typ is not INSERT, it is always false.
// truncate : it denotes the batch with typ DELETE on mo_tables is generated when Truncating
// a table.
func (txn *Transaction) WriteBatch(
	typ int,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	dnStore DNStore,
	primaryIdx int, // pass -1 to indicate no primary key or disable primary key checking
	insertBatchHasRowId bool,
	truncate bool) error {
	txn.readOnly.Store(false)
	bat.Cnt = 1
	txn.Lock()
	defer txn.Unlock()
	if typ == INSERT {
		if !insertBatchHasRowId {
			txn.genBlock()
			len := bat.RowCount()
			vec := txn.proc.GetVector(types.T_Rowid.ToType())
			for i := 0; i < len; i++ {
				if err := vector.AppendFixed(vec, txn.genRowId(), false,
					txn.proc.Mp()); err != nil {
					return err
				}
			}
			bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
			bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
		}
		// for TestPrimaryKeyCheck
		if txn.blockId_raw_batch != nil {
			txn.blockId_raw_batch[*txn.getCurrentBlockId()] = bat
		}
		txn.workspaceSize += uint64(bat.Size())
	}
	e := Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		dnStore:      dnStore,
		truncate:     truncate,
	}
	txn.writes = append(txn.writes, e)
	return nil
}

func (txn *Transaction) dumpBatch(offset int) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.dumpBatchLocked(offset)
}

// dumpBatch if txn.workspaceSize is larger than threshold, cn will write workspace to s3
func (txn *Transaction) dumpBatchLocked(offset int) error {
	var size uint64
	if txn.workspaceSize < WorkspaceThreshold {
		return nil
	}

	txn.hasS3Op.Store(true)
	mp := make(map[[2]string][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		// TODO: after shrink, we should update workspace size
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			key := [2]string{txn.writes[i].databaseName, txn.writes[i].tableName}
			bat := txn.writes[i].bat
			size += uint64(bat.Size())
			// skip rowid
			bat.Attrs = bat.Attrs[1:]
			bat.Vecs = bat.Vecs[1:]
			mp[key] = append(mp[key], bat)
			// DON'T MODIFY THE IDX OF AN ENTRY IN LOG
			// THIS IS VERY IMPORTANT FOR CN BLOCK COMPACTION
			// maybe this will cause that the log increments unlimitedly
			// txn.writes = append(txn.writes[:i], txn.writes[i+1:]...)
			// i--
			txn.writes[i].bat = nil
		}
	}

	for key := range mp {
		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl, err := txn.getTable(key)
		if err != nil {
			return err
		}
		s3Writer, err := colexec.AllocS3Writer(txn.proc, tbl.(*txnTable).getTableDef())
		if err != nil {
			return err
		}
		defer s3Writer.Free(txn.proc)

		s3Writer.InitBuffers(txn.proc, mp[key][0])
		for i := 0; i < len(mp[key]); i++ {
			s3Writer.Put(mp[key][i], txn.proc)
		}
		err = s3Writer.SortAndFlush(txn.proc)

		if err != nil {
			return err
		}
		blockInfo := s3Writer.GetBlockInfoBat()

		lenVecs := len(blockInfo.Attrs)
		// only remain the metaLoc col
		blockInfo.Vecs = blockInfo.Vecs[lenVecs-1:]
		blockInfo.Attrs = blockInfo.Attrs[lenVecs-1:]
		blockInfo.SetRowCount(blockInfo.Vecs[0].Length())

		table := tbl.(*txnTable)
		fileName := catalog.DecodeBlockInfo(
			blockInfo.Vecs[0].GetBytesAt(0)).
			MetaLocation().Name().String()
		err = table.db.txn.WriteFileLocked(
			INSERT,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			blockInfo,
			table.db.txn.dnStores[0],
		)
		//err = tbl.Write(txn.proc.Ctx, metaLoc)
		if err != nil {
			return err
		}
		// free batches
		for _, bat := range mp[key] {
			txn.proc.PutBatch(bat)
		}
	}
	if offset == 0 {
		txn.workspaceSize = 0
		writes := txn.writes[:0]
		for i, write := range txn.writes {
			if write.bat != nil {
				writes = append(writes, txn.writes[i])
			}
		}
		txn.writes = writes
		txn.statements[txn.statementID-1] = len(txn.writes)
	} else {
		txn.workspaceSize -= size
	}
	return nil
}

func (txn *Transaction) getTable(key [2]string) (engine.Relation, error) {
	databaseName := key[0]
	tableName := key[1]

	database, err := txn.engine.Database(txn.proc.Ctx, databaseName, txn.proc.TxnOperator)
	if err != nil {
		return nil, err
	}
	tbl, err := database.Relation(txn.proc.Ctx, tableName, nil)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

// vec contains block infos.
func (txn *Transaction) insertPosForCNBlock(
	vec *vector.Vector,
	b *batch.Batch,
	dbName string,
	tbName string) error {
	blks := vector.MustBytesCol(vec)
	for i, blk := range blks {
		blkInfo := *catalog.DecodeBlockInfo(blk)
		txn.cnBlkId_Pos[blkInfo.BlockID] = Pos{
			bat:     b,
			dbName:  dbName,
			tbName:  tbName,
			offset:  int64(i),
			blkInfo: blkInfo}
	}
	return nil
}

func (txn *Transaction) WriteFileLocked(
	typ int,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	dnStore DNStore) error {
	txn.hasS3Op.Store(true)
	newBat := bat
	if typ == INSERT {
		//bat.Attrs = {catalog.BlockMeta_BlockInfo}
		newBat = batch.NewWithSize(len(bat.Vecs))
		newBat.SetAttributes([]string{catalog.BlockMeta_MetaLoc})
		newBat.SetVector(0, vector.NewVec(types.T_text.ToType()))

		for _, blk := range vector.MustBytesCol(bat.GetVector(0)) {
			blkInfo := *catalog.DecodeBlockInfo(blk)
			vector.AppendBytes(
				newBat.GetVector(0),
				[]byte(blkInfo.MetaLocation().String()),
				false,
				txn.proc.Mp())
		}
		newBat.SetRowCount(bat.Vecs[0].Length())
	}
	txn.readOnly.Store(false)
	entry := Entry{
		typ:          typ,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          newBat,
		dnStore:      dnStore,
	}
	txn.writes = append(txn.writes, entry)
	//the first part of the file name is the segment id which generated by cn through writing S3.
	if sid, err := types.ParseUuid(strings.Split(fileName, "_")[0]); err != nil {
		panic("fileName parse Uuid error")
	} else {
		// get uuid string
		if typ == INSERT {
			colexec.Srv.PutCnSegment(&sid, colexec.CnBlockIdType)
			txn.insertPosForCNBlock(
				bat.GetVector(0),
				entry.bat,
				entry.databaseName,
				entry.tableName)
		}
	}
	return nil
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(
	typ int,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	dnStore DNStore) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.WriteFileLocked(
		typ, databaseId, tableId,
		databaseName, tableName, fileName, bat, dnStore)
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {
	mp := make(map[types.Rowid]uint8)
	deleteBlkId := make(map[types.Blockid]bool)
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(0))
	min1 := uint32(math.MaxUint32)
	max1 := uint32(0)
	cnRowIdOffsets := make([]int64, 0, len(rowids))
	for i, rowid := range rowids {
		// process cn block deletes
		uid := rowid.BorrowSegmentID()
		blkid := rowid.CloneBlockID()
		deleteBlkId[blkid] = true
		mp[rowid] = 0
		rowOffset := rowid.GetRowOffset()
		if colexec.Srv != nil && colexec.Srv.GetCnSegmentType(uid) == colexec.CnBlockIdType {
			txn.deletedBlocks.addDeletedBlocks(&blkid, []int64{int64(rowOffset)})
			cnRowIdOffsets = append(cnRowIdOffsets, int64(i))
			continue
		}
		if rowOffset < (min1) {
			min1 = rowOffset
		}

		if rowOffset > max1 {
			max1 = rowOffset
		}
		// update workspace
	}
	// cn rowId antiShrink
	bat.AntiShrink(cnRowIdOffsets)
	if bat.RowCount() == 0 {
		return bat
	}
	sels := txn.proc.Mp().GetSels()
	txn.deleteTableWrites(databaseId, tableId, sels, deleteBlkId, min1, max1, mp)
	sels = sels[:0]
	for k, rowid := range rowids {
		if mp[rowid] == 0 {
			sels = append(sels, int64(k))
		}
	}
	bat.Shrink(sels)
	txn.proc.Mp().PutSels(sels)
	return bat
}

func (txn *Transaction) deleteTableWrites(
	databaseId uint64,
	tableId uint64,
	sels []int64,
	deleteBlkId map[types.Blockid]bool,
	min, max uint32,
	mp map[types.Rowid]uint8,
) {
	txn.Lock()
	defer txn.Unlock()

	// txn worksapce will have four batch type:
	// 1.RawBatch 2.DN Block RowId(mixed rowid from different block)
	// 3.CN block Meta batch(record block meta generated by cn insert write s3)
	// 4.DN delete Block Meta batch(record block meta generated by cn delete write s3)
	for _, e := range txn.writes {
		// nil batch will generated by comapction or dumpBatch
		if e.bat == nil {
			continue
		}
		// for 3 and 4 above.
		if e.bat.Attrs[0] == catalog.BlockMeta_MetaLoc ||
			e.bat.Attrs[0] == catalog.BlockMeta_DeltaLoc {
			continue
		}
		sels = sels[:0]
		if e.tableId == tableId && e.databaseId == databaseId {
			vs := vector.MustFixedCol[types.Rowid](e.bat.GetVector(0))
			if len(vs) == 0 {
				continue
			}
			// skip 2 above
			if !vs[0].BorrowSegmentID().Eq(txn.segId) {
				continue
			}
			// current batch is not be deleted
			if !deleteBlkId[vs[0].CloneBlockID()] {
				continue
			}
			min2 := vs[0].GetRowOffset()
			max2 := vs[len(vs)-1].GetRowOffset()
			if min > max2 || max < min2 {
				continue
			}
			for k, v := range vs {
				if _, ok := mp[v]; !ok {
					sels = append(sels, int64(k))
				} else {
					mp[v]++
				}
			}
			if len(sels) != len(vs) {
				txn.batchSelectList[e.bat] = append(txn.batchSelectList[e.bat], sels...)

			}
		}
	}
}

func (txn *Transaction) allocateID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return txn.idGen.AllocateID(ctx)
}

func (txn *Transaction) genBlock() {
	txn.rowId[4]++
	txn.rowId[5] = INIT_ROWID_OFFSET
}

func (txn *Transaction) getCurrentBlockId() *types.Blockid {
	rowId := types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
	return rowId.BorrowBlockID()
}

func (txn *Transaction) genRowId() types.Rowid {
	if txn.rowId[5] != INIT_ROWID_OFFSET {
		txn.rowId[5]++
	} else {
		txn.rowId[5] = 0
	}
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
}

func (txn *Transaction) mergeTxnWorkspaceLocked() error {
	if len(txn.batchSelectList) > 0 {
		for _, e := range txn.writes {
			if sels, ok := txn.batchSelectList[e.bat]; ok {
				e.bat.Shrink(sels)
				delete(txn.batchSelectList, e.bat)
			}
		}
	}
	return nil
}

// CN blocks compaction for txn
func (txn *Transaction) mergeCompactionLocked() error {
	compactedBlks := make(map[[2]string]map[catalog.BlockInfo][]int64)
	compactedEntries := make(map[*batch.Batch][]int64)
	defer func() {
		txn.deletedBlocks = nil
	}()
	txn.deletedBlocks.iter(
		func(blkId *types.Blockid, offsets []int64) bool {
			pos := txn.cnBlkId_Pos[*blkId]
			if v, ok := compactedBlks[[2]string{pos.dbName, pos.tbName}]; ok {
				v[pos.blkInfo] = offsets
			} else {
				compactedBlks[[2]string{pos.dbName, pos.tbName}] =
					map[catalog.BlockInfo][]int64{pos.blkInfo: offsets}
			}
			compactedEntries[pos.bat] = append(compactedEntries[pos.bat], pos.offset)
			delete(txn.cnBlkId_Pos, *blkId)
			return true
		})

	for key, blks := range compactedBlks {
		rel, err := txn.getTable(key)
		if err != nil {
			return err
		}
		//TODO::do parallel compaction for table
		tbl := rel.(*txnTable)
		createdBlks, err := tbl.mergeCompaction(blks)
		if err != nil {
			return err
		}
		if len(createdBlks) > 0 {
			bat := batch.NewWithSize(1)
			bat.Attrs = []string{catalog.BlockMeta_BlockInfo}
			bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
			for _, blkInfo := range createdBlks {
				vector.AppendBytes(
					bat.GetVector(0),
					catalog.EncodeBlockInfo(blkInfo),
					false,
					tbl.db.txn.proc.GetMPool())
			}
			bat.SetRowCount(len(createdBlks))
			defer func() {
				bat.Clean(tbl.db.txn.proc.GetMPool())
			}()

			err := txn.WriteFileLocked(
				INSERT,
				tbl.db.databaseId,
				tbl.tableId,
				tbl.db.databaseName,
				tbl.tableName,
				createdBlks[0].MetaLocation().Name().String(),
				bat,
				tbl.db.txn.dnStores[0],
			)
			if err != nil {
				return err
			}
		}
	}

	//compaction for txn.writes
	for i, entry := range txn.writes {
		if entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}
		if entry.typ != INSERT ||
			entry.bat.Attrs[0] != catalog.BlockMeta_MetaLoc {
			continue
		}
		entry.bat.AntiShrink(compactedEntries[entry.bat])
		if entry.bat.RowCount() == 0 {
			txn.writes[i].bat.Clean(txn.proc.GetMPool())
			txn.writes[i].bat = nil
		}
	}
	return nil
}

func (txn *Transaction) getInsertedBlocksForTable(
	databaseId uint64,
	tableId uint64) (blks []catalog.BlockInfo, err error) {
	txn.Lock()
	defer txn.Unlock()
	for _, entry := range txn.writes {
		if entry.databaseId != databaseId ||
			entry.tableId != tableId {
			continue
		}
		if entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}
		if entry.typ != INSERT ||
			entry.bat.Attrs[0] != catalog.BlockMeta_MetaLoc {
			continue
		}
		metaLocs := vector.MustStrCol(entry.bat.Vecs[0])
		for _, metaLoc := range metaLocs {
			location, err := blockio.EncodeLocationFromString(metaLoc)
			if err != nil {
				return nil, err
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
		}

	}
	return blks, nil

}

func (txn *Transaction) getTableWrites(databaseId uint64, tableId uint64, writes []Entry) []Entry {
	txn.Lock()
	defer txn.Unlock()
	for _, entry := range txn.writes {
		if entry.databaseId != databaseId {
			continue
		}
		if entry.tableId != tableId {
			continue
		}
		writes = append(writes, entry)
	}
	return writes
}

// getCachedTable returns the cached table in this transaction if it exists, nil otherwise.
// Before it gets the cached table, it checks whether the table is deleted by another
// transaction by go through the delete tables slice, and advance its cachedIndex.
func (txn *Transaction) getCachedTable(
	ctx context.Context, k tableKey, snapshotTS timestamp.Timestamp,
) *txnTable {
	if txn.meta.IsRCIsolation() {
		oldIdx := txn.tableCache.cachedIndex
		newIdx := txn.engine.catalog.GetDeletedTableIndex()
		if oldIdx < newIdx {
			deleteTables := txn.engine.catalog.GetDeletedTables(oldIdx, snapshotTS)
			for _, item := range deleteTables {
				txn.tableCache.tableMap.Delete(genTableKey(ctx, item.Name, item.DatabaseId))
				txn.tableCache.cachedIndex++
			}
		}
	}
	if v, ok := txn.tableCache.tableMap.Load(k); ok {
		return v.(*txnTable)
	}
	return nil
}

func (txn *Transaction) Commit(ctx context.Context) ([]txn.TxnRequest, error) {
	logDebugf(txn.op.Txn(), "Transaction.Commit")
	txn.IncrStatementID(ctx, true)
	defer txn.delTransaction()
	if txn.readOnly.Load() {
		return nil, nil
	}
	if err := txn.mergeTxnWorkspaceLocked(); err != nil {
		return nil, err
	}
	if err := txn.dumpBatchLocked(0); err != nil {
		return nil, err
	}
	if err := txn.mergeCompactionLocked(); err != nil {
		return nil, err
	}
	reqs, err := genWriteReqs(ctx, txn.writes)
	if err != nil {
		return nil, err
	}
	return reqs, nil
}

func (txn *Transaction) Rollback(ctx context.Context) error {
	logDebugf(txn.op.Txn(), "Transaction.Rollback")
	txn.delTransaction()
	return nil
}

func (txn *Transaction) delTransaction() {
	if txn.removed {
		return
	}

	for i := range txn.writes {
		if txn.writes[i].bat == nil {
			continue
		}
		txn.proc.PutBatch(txn.writes[i].bat)
	}
	txn.tableCache.cachedIndex = -1
	txn.tableCache.tableMap = nil
	txn.createMap = nil
	txn.databaseMap = nil
	txn.deletedTableMap = nil
	txn.blockId_dn_delete_metaLoc_batch = nil
	txn.blockId_raw_batch = nil
	txn.deletedBlocks = nil
	segmentnames := make([]objectio.Segmentid, 0, len(txn.cnBlkId_Pos)+1)
	segmentnames = append(segmentnames, txn.segId)
	for blkId := range txn.cnBlkId_Pos {
		// blkId:
		// |------|----------|----------|
		//   uuid    filelen   blkoffset
		//    16        2          2
		segmentnames = append(segmentnames, *blkId.Segment())
	}
	colexec.Srv.DeleteTxnSegmentIds(segmentnames)
	txn.cnBlkId_Pos = nil
	txn.hasS3Op.Store(false)
	txn.removed = true
}
