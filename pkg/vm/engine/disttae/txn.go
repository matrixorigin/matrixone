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
	"encoding/hex"
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

//func (txn *Transaction) getObjInfos(
//	ctx context.Context,
//	tbl *txnTable,
//) (objs []logtailreplay.ObjectEntry, err error) {
//	ts := types.TimestampToTS(txn.op.SnapshotTS())
//	state, err := tbl.getPartitionState(ctx)
//	if err != nil {
//		return nil, err
//	}
//	iter, err := state.NewObjectsIter(ts)
//	if err != nil {
//		return nil, err
//	}
//	for iter.Next() {
//		entry := iter.Entry()
//		objs = append(objs, entry)
//	}
//	iter.Close()
//	return
//}

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
	accountId uint32,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	tnStore DNStore,
	primaryIdx int, // pass -1 to indicate no primary key or disable primary key checking
	insertBatchHasRowId bool,
	truncate bool) error {
	start := time.Now()
	seq := txn.op.NextSequence()
	trace.GetService(txn.proc.GetService()).AddTxnDurationAction(
		txn.op,
		client.WorkspaceWriteEvent,
		seq,
		tableId,
		0,
		nil)
	defer func() {
		trace.GetService(txn.proc.GetService()).AddTxnDurationAction(
			txn.op,
			client.WorkspaceWriteEvent,
			seq,
			tableId,
			time.Since(start),
			nil)
	}()

	txn.readOnly.Store(false)
	bat.Cnt = 1
	txn.Lock()
	defer txn.Unlock()
	if typ == INSERT || typ == INSERT_TXN {
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
		if tableId != catalog.MO_DATABASE_ID &&
			tableId != catalog.MO_TABLES_ID && tableId != catalog.MO_COLUMNS_ID {
			txn.workspaceSize += uint64(bat.Size())
			txn.insertCount += bat.RowCount()
		}
	}
	e := Entry{
		typ:          typ,
		accountId:    accountId,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		tnStore:      tnStore,
		truncate:     truncate,
	}
	txn.writes = append(txn.writes, e)
	txn.pkCount += bat.RowCount()

	trace.GetService(txn.proc.GetService()).TxnWrite(txn.op, tableId, typesNames[typ], bat)
	return nil
}

func (txn *Transaction) dumpBatch(offset int) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.dumpBatchLocked(offset)
}

func checkPKDupGeneric[T comparable](
	mp map[any]bool,
	t *types.Type,
	vals []T,
	start, count int) (bool, string) {
	for _, v := range vals[start : start+count] {
		if _, ok := mp[v]; ok {
			entry := common.TypeStringValue(*t, v, false)
			return true, entry
		}
		mp[v] = true
	}
	return false, ""
}

func checkPKDup(
	mp map[any]bool,
	pk *vector.Vector,
	start, count int) (bool, string) {
	colType := pk.GetType()
	switch colType.Oid {
	case types.T_bool:
		vs := vector.MustFixedCol[bool](pk)
		return checkPKDupGeneric[bool](mp, colType, vs, start, count)
	case types.T_bit:
		vs := vector.MustFixedCol[uint64](pk)
		return checkPKDupGeneric[uint64](mp, colType, vs, start, count)
	case types.T_int8:
		vs := vector.MustFixedCol[int8](pk)
		return checkPKDupGeneric[int8](mp, colType, vs, start, count)
	case types.T_int16:
		vs := vector.MustFixedCol[int16](pk)
		return checkPKDupGeneric[int16](mp, colType, vs, start, count)
	case types.T_int32:
		vs := vector.MustFixedCol[int32](pk)
		return checkPKDupGeneric[int32](mp, colType, vs, start, count)
	case types.T_int64:
		vs := vector.MustFixedCol[int64](pk)
		return checkPKDupGeneric[int64](mp, colType, vs, start, count)
	case types.T_uint8:
		vs := vector.MustFixedCol[uint8](pk)
		return checkPKDupGeneric[uint8](mp, colType, vs, start, count)
	case types.T_uint16:
		vs := vector.MustFixedCol[uint16](pk)
		return checkPKDupGeneric[uint16](mp, colType, vs, start, count)
	case types.T_uint32:
		vs := vector.MustFixedCol[uint32](pk)
		return checkPKDupGeneric[uint32](mp, colType, vs, start, count)
	case types.T_uint64:
		vs := vector.MustFixedCol[uint64](pk)
		return checkPKDupGeneric[uint64](mp, colType, vs, start, count)
	case types.T_decimal64:
		vs := vector.MustFixedCol[types.Decimal64](pk)
		return checkPKDupGeneric[types.Decimal64](mp, colType, vs, start, count)
	case types.T_decimal128:
		vs := vector.MustFixedCol[types.Decimal128](pk)
		return checkPKDupGeneric[types.Decimal128](mp, colType, vs, start, count)
	case types.T_uuid:
		vs := vector.MustFixedCol[types.Uuid](pk)
		return checkPKDupGeneric[types.Uuid](mp, colType, vs, start, count)
	case types.T_float32:
		vs := vector.MustFixedCol[float32](pk)
		return checkPKDupGeneric[float32](mp, colType, vs, start, count)
	case types.T_float64:
		vs := vector.MustFixedCol[float64](pk)
		return checkPKDupGeneric[float64](mp, colType, vs, start, count)
	case types.T_date:
		vs := vector.MustFixedCol[types.Date](pk)
		return checkPKDupGeneric[types.Date](mp, colType, vs, start, count)
	case types.T_timestamp:
		vs := vector.MustFixedCol[types.Timestamp](pk)
		return checkPKDupGeneric[types.Timestamp](mp, colType, vs, start, count)
	case types.T_time:
		vs := vector.MustFixedCol[types.Time](pk)
		return checkPKDupGeneric[types.Time](mp, colType, vs, start, count)
	case types.T_datetime:
		vs := vector.MustFixedCol[types.Datetime](pk)
		return checkPKDupGeneric[types.Datetime](mp, colType, vs, start, count)
	case types.T_enum:
		vs := vector.MustFixedCol[types.Enum](pk)
		return checkPKDupGeneric[types.Enum](mp, colType, vs, start, count)
	case types.T_TS:
		vs := vector.MustFixedCol[types.TS](pk)
		return checkPKDupGeneric[types.TS](mp, colType, vs, start, count)
	case types.T_Rowid:
		vs := vector.MustFixedCol[types.Rowid](pk)
		return checkPKDupGeneric[types.Rowid](mp, colType, vs, start, count)
	case types.T_Blockid:
		vs := vector.MustFixedCol[types.Blockid](pk)
		return checkPKDupGeneric[types.Blockid](mp, colType, vs, start, count)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		for i := start; i < start+count; i++ {
			v := pk.UnsafeGetStringAt(i)
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, []byte(v), false)
				return true, entry
			}
			mp[v] = true
		}
	case types.T_array_float32:
		for i := start; i < start+count; i++ {
			v := types.ArrayToString[float32](vector.GetArrayAt[float32](pk, i))
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, pk.GetBytesAt(i), false)
				return true, entry
			}
			mp[v] = true
		}
	case types.T_array_float64:
		for i := start; i < start+count; i++ {
			v := types.ArrayToString[float64](vector.GetArrayAt[float64](pk, i))
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, pk.GetBytesAt(i), false)
				return true, entry
			}
			mp[v] = true
		}
	default:
		panic(moerr.NewInternalErrorNoCtx("%s not supported", pk.GetType().String()))
	}
	return false, ""
}

// checkDup check whether the txn.writes has duplicate pk entry
func (txn *Transaction) checkDup() error {
	start := time.Now()
	defer func() {
		v2.TxnCheckPKDupDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	//table id is global unique
	tablesDef := make(map[uint64]*plan.TableDef)
	pkIndex := make(map[uint64]int)
	insertPks := make(map[uint64]map[any]bool)
	delPks := make(map[uint64]map[any]bool)

	for _, e := range txn.writes {
		if e.bat == nil || e.bat.RowCount() == 0 {
			continue
		}
		if e.fileName != "" {
			continue
		}
		if e.isCatalog() {
			continue
		}

		dbkey := genDatabaseKey(e.accountId, e.databaseName)
		if _, ok := txn.deletedDatabaseMap.Load(dbkey); ok {
			continue
		}

		tableKey := genTableKey(e.accountId, e.tableName, e.databaseId)
		if _, ok := txn.deletedTableMap.Load(tableKey); ok {
			continue
		}
		//build pk index for tables.
		if _, ok := tablesDef[e.tableId]; !ok {
			tbl, err := txn.getTable(e.accountId, e.databaseName, e.tableName)
			if err != nil {
				return err
			}
			tablesDef[e.tableId] = tbl.GetTableDef(txn.proc.Ctx)
		}
		tableDef := tablesDef[e.tableId]
		if _, ok := pkIndex[e.tableId]; !ok {
			for idx, colDef := range tableDef.Cols {
				if colDef.Name == tableDef.Pkey.PkeyColName {
					if colDef.Name == catalog.FakePrimaryKeyColName ||
						colDef.Name == catalog.CPrimaryKeyColName {
						pkIndex[e.tableId] = -1
					} else {
						pkIndex[e.tableId] = idx
					}
					break
				}
			}
		}

		if e.typ == INSERT || e.typ == INSERT_TXN {
			bat := e.bat
			if index, ok := pkIndex[e.tableId]; ok && index != -1 {
				if *bat.Vecs[0].GetType() == types.T_Rowid.ToType() {
					newBat := batch.NewWithSize(len(bat.Vecs) - 1)
					newBat.SetAttributes(bat.Attrs[1:])
					newBat.Vecs = bat.Vecs[1:]
					newBat.SetRowCount(bat.Vecs[0].Length())
					bat = newBat
				}
				if _, ok := insertPks[e.tableId]; !ok {
					insertPks[e.tableId] = make(map[any]bool)
				}
				if dup, pk := checkPKDup(
					insertPks[e.tableId],
					bat.Vecs[index],
					0,
					bat.RowCount()); dup {
					logutil.Errorf("txn:%s wants to insert duplicate primary key:%s in table:[%v-%v:%s-%s]",
						hex.EncodeToString(txn.op.Txn().ID),
						pk,
						e.databaseId,
						e.tableId,
						e.databaseName,
						e.tableName)
					return moerr.NewDuplicateEntryNoCtx(pk, bat.Attrs[index])
				}
			}
			continue
		}
		//if entry.tyep is DELETE, then e.bat.Vecs[0] is rowid,e.bat.Vecs[1] is PK
		if e.typ == DELETE || e.typ == DELETE_TXN {
			if len(e.bat.Vecs) < 2 {
				logutil.Warnf("delete has no pk, database:%s, table:%s",
					e.databaseName, e.tableName)
				continue
			}
			if index, ok := pkIndex[e.tableId]; ok && index != -1 {
				if _, ok := delPks[e.tableId]; !ok {
					delPks[e.tableId] = make(map[any]bool)
				}
				if dup, pk := checkPKDup(
					delPks[e.tableId],
					e.bat.Vecs[1],
					0,
					e.bat.RowCount()); dup {
					logutil.Errorf("txn:%s wants to delete duplicate primary key:%s in table:[%v-%v:%s-%s]",
						hex.EncodeToString(txn.op.Txn().ID),
						pk,
						e.databaseId,
						e.tableId,
						e.databaseName,
						e.tableName)
					return moerr.NewDuplicateEntryNoCtx(pk, e.bat.Attrs[1])
				}
			}
		}
	}
	return nil
}

// dumpBatch if txn.workspaceSize is larger than threshold, cn will write workspace to s3
// start from write offset.   Pass in offset -1 to dump all.   Note that dump all will
// modify txn.writes, so it can only be called right before txn.commit.
func (txn *Transaction) dumpBatchLocked(offset int) error {
	var size uint64
	var pkCount int

	//offset < 0 indicates commit.
	if offset < 0 {
		//if txn.workspaceSize < WorkspaceThreshold {
		//	return nil
		//}
		if txn.workspaceSize < WorkspaceThreshold && txn.insertCount < InsertEntryThreshold {
			return nil
		}
	} else {
		if txn.workspaceSize < WorkspaceThreshold {
			return nil
		}
	}

	dumpAll := offset < 0
	if dumpAll {
		offset = 0
	}

	if !dumpAll {
		for i := offset; i < len(txn.writes); i++ {
			if txn.writes[i].tableId == catalog.MO_DATABASE_ID ||
				txn.writes[i].tableId == catalog.MO_TABLES_ID ||
				txn.writes[i].tableId == catalog.MO_COLUMNS_ID {
				continue
			}
			if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
				continue
			}
			if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
				size += uint64(txn.writes[i].bat.Size())
			}
		}
		if size < WorkspaceThreshold {
			return nil
		}
		size = 0
	}
	txn.hasS3Op.Store(true)
	mp := make(map[tableKey][]*batch.Batch)

	lastTxnWritesIndex := offset
	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].tableId == catalog.MO_DATABASE_ID ||
			txn.writes[i].tableId == catalog.MO_TABLES_ID ||
			txn.writes[i].tableId == catalog.MO_COLUMNS_ID {
			txn.writes[lastTxnWritesIndex] = txn.writes[i]
			lastTxnWritesIndex++
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			txn.writes[lastTxnWritesIndex] = txn.writes[i]
			lastTxnWritesIndex++
			continue
		}

		keepElement := true
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			tbKey := tableKey{
				accountId:  txn.writes[i].accountId,
				databaseId: txn.writes[i].databaseId,
				dbName:     txn.writes[i].databaseName,
				name:       txn.writes[i].tableName,
			}
			bat := txn.writes[i].bat
			size += uint64(bat.Size())
			pkCount += bat.RowCount()
			// skip rowid
			newBat := batch.NewWithSize(len(bat.Vecs) - 1)
			newBat.SetAttributes(bat.Attrs[1:])
			newBat.Vecs = bat.Vecs[1:]
			newBat.SetRowCount(bat.Vecs[0].Length())
			mp[tbKey] = append(mp[tbKey], newBat)
			txn.toFreeBatches[tbKey] = append(txn.toFreeBatches[tbKey], bat)

			keepElement = false
		}

		if keepElement {
			txn.writes[lastTxnWritesIndex] = txn.writes[i]
			lastTxnWritesIndex++
		}
	}
	txn.writes = txn.writes[:lastTxnWritesIndex]

	for tbKey := range mp {
		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl, err := txn.getTable(tbKey.accountId, tbKey.dbName, tbKey.name)
		if err != nil {
			return err
		}

		tableDef := tbl.GetTableDef(txn.proc.Ctx)

		s3Writer, err := colexec.AllocS3Writer(txn.proc, tableDef)
		if err != nil {
			return err
		}
		defer s3Writer.Free(txn.proc)

		s3Writer.InitBuffers(txn.proc, mp[tbKey][0])
		for i := 0; i < len(mp[tbKey]); i++ {
			s3Writer.Put(mp[tbKey][i], txn.proc)
		}
		err = s3Writer.SortAndFlush(txn.proc)

		if err != nil {
			return err
		}
		blockInfo := s3Writer.GetBlockInfoBat()

		lenVecs := len(blockInfo.Attrs)
		// only remain the metaLoc col and object stats
		blockInfo.Vecs = blockInfo.Vecs[lenVecs-2:]
		blockInfo.Attrs = blockInfo.Attrs[lenVecs-2:]
		blockInfo.SetRowCount(blockInfo.Vecs[0].Length())

		var table *txnTable
		if v, ok := tbl.(*txnTableDelegate); ok {
			table = v.origin
		} else {
			table = tbl.(*txnTable)
		}
		fileName := objectio.DecodeBlockInfo(
			blockInfo.Vecs[0].GetBytesAt(0)).
			MetaLocation().Name().String()
		err = table.getTxn().WriteFileLocked(
			INSERT,
			table.accountId,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			blockInfo,
			table.getTxn().tnStores[0],
		)
		if err != nil {
			return err
		}
	}

	if dumpAll {
		txn.workspaceSize = 0
		txn.pkCount -= pkCount
		// modifies txn.writes.
		writes := txn.writes[:0]
		for i, write := range txn.writes {
			if write.bat != nil {
				writes = append(writes, txn.writes[i])
			}
		}
		txn.writes = writes
	} else {
		txn.workspaceSize -= size
		txn.pkCount -= pkCount
	}
	return nil
}

func (txn *Transaction) getTable(
	id uint32,
	dbName string,
	tbName string,
) (engine.Relation, error) {
	ctx := context.WithValue(
		context.Background(),
		defines.TenantIDKey{},
		id,
	)

	database, err := txn.engine.Database(ctx, dbName, txn.proc.GetTxnOperator())
	if err != nil {
		return nil, err
	}
	tbl, err := database.Relation(ctx, tbName, nil)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

// vec contains block infos.
func (txn *Transaction) insertPosForCNBlock(
	vec *vector.Vector,
	id uint32,
	b *batch.Batch,
	dbName string,
	tbName string) error {
	blks, area := vector.MustVarlenaRawData(vec)
	for i := range blks {
		blkInfo := *objectio.DecodeBlockInfo(blks[i].GetByteSlice(area))
		txn.cnBlkId_Pos[blkInfo.BlockID] = Pos{
			bat:       b,
			accountId: id,
			dbName:    dbName,
			tbName:    tbName,
			offset:    int64(i),
			blkInfo:   blkInfo}
	}
	return nil
}

func (txn *Transaction) WriteFileLocked(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	tnStore DNStore) error {
	txn.hasS3Op.Store(true)
	newBat := bat
	if typ == INSERT {
		newBat = batch.NewWithSize(len(bat.Vecs))
		newBat.SetAttributes([]string{catalog.BlockMeta_MetaLoc, catalog.ObjectMeta_ObjectStats})

		for idx := 0; idx < newBat.VectorCount(); idx++ {
			newBat.SetVector(int32(idx), vector.NewVec(*bat.Vecs[idx].GetType()))
		}

		blkInfosVec := bat.Vecs[0]
		for idx := 0; idx < blkInfosVec.Length(); idx++ {
			blkInfo := *objectio.DecodeBlockInfo(blkInfosVec.GetBytesAt(idx))
			vector.AppendBytes(newBat.Vecs[0], []byte(blkInfo.MetaLocation().String()),
				false, txn.proc.Mp())
			colexec.Get().PutCnSegment(&blkInfo.SegmentID, colexec.CnBlockIdType)
		}

		// append obj stats, may multiple
		statsListVec := bat.Vecs[1]
		for idx := 0; idx < statsListVec.Length(); idx++ {
			vector.AppendBytes(newBat.Vecs[1], statsListVec.GetBytesAt(idx), false, txn.proc.Mp())
		}
		newBat.SetRowCount(bat.Vecs[0].Length())

		txn.insertPosForCNBlock(
			bat.GetVector(0),
			accountId,
			newBat,
			databaseName,
			tableName)
	}
	txn.readOnly.Store(false)
	entry := Entry{
		typ:          typ,
		accountId:    accountId,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          newBat,
		tnStore:      tnStore,
	}
	txn.writes = append(txn.writes, entry)
	return nil
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	tnStore DNStore) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.WriteFileLocked(
		typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, bat, tnStore)
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {
	start := time.Now()
	seq := txn.op.NextSequence()
	trace.GetService(txn.proc.GetService()).AddTxnDurationAction(
		txn.op,
		client.WorkspaceWriteEvent,
		seq,
		tableId,
		0,
		nil)
	defer func() {
		trace.GetService(txn.proc.GetService()).AddTxnDurationAction(
			txn.op,
			client.WorkspaceWriteEvent,
			seq,
			tableId,
			time.Since(start),
			nil)
	}()

	trace.GetService(txn.proc.GetService()).TxnWrite(txn.op, tableId, typesNames[DELETE], bat)

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
		if colexec.Get() != nil && colexec.Get().GetCnSegmentType(uid) == colexec.CnBlockIdType {
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
	bat.Shrink(cnRowIdOffsets, true)
	if bat.RowCount() == 0 {
		return bat
	}
	sels := txn.proc.Mp().GetSels()
	txn.deleteTableWrites(databaseId, tableId, sels, deleteBlkId, min1, max1, mp)

	sels = sels[:0]
	rowids = vector.MustFixedCol[types.Rowid](bat.GetVector(0))
	for k, rowid := range rowids {
		// put rowid to be deleted into sels.
		if mp[rowid] != 0 {
			sels = append(sels, int64(k))
		}
	}
	bat.Shrink(sels, true)
	txn.proc.Mp().PutSels(sels)
	return bat
}

// Delete rows belongs to uncommitted raw data batch in txn's workspace.
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
		if e.typ == UPDATE || e.typ == ALTER {
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
			// Now, e.bat is uncommitted raw data batch which belongs to only one block allocated by CN.
			// so if e.bat is not to be deleted,skip it.
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
					// if the v is not to be deleted, then add its index into the sels.
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
				txn.insertCount -= e.bat.RowCount() - len(sels)
				e.bat.Shrink(sels, false)
				delete(txn.batchSelectList, e.bat)
			}
		}
	}
	return txn.compactionBlksLocked()
}

// CN blocks compaction for txn
func (txn *Transaction) compactionBlksLocked() error {
	compactedBlks := make(map[tableKey]map[objectio.ObjectLocation][]int64)
	compactedEntries := make(map[*batch.Batch][]int64)
	defer func() {
		//txn.deletedBlocks = nil
		txn.deletedBlocks.clean()
	}()
	txn.deletedBlocks.iter(
		func(blkId *types.Blockid, offsets []int64) bool {
			pos := txn.cnBlkId_Pos[*blkId]
			if v, ok := compactedBlks[tableKey{
				accountId: pos.accountId,
				dbName:    pos.dbName,
				name:      pos.tbName,
			}]; ok {
				v[pos.blkInfo.MetaLoc] = offsets
			} else {
				compactedBlks[tableKey{
					accountId: pos.accountId,
					dbName:    pos.dbName,
					name:      pos.tbName,
				}] =
					map[objectio.ObjectLocation][]int64{pos.blkInfo.MetaLoc: offsets}
			}
			compactedEntries[pos.bat] = append(compactedEntries[pos.bat], pos.offset)
			//delete(txn.cnBlkId_Pos, *blkId)
			return true
		})

	for tbKey, blks := range compactedBlks {
		rel, err := txn.getTable(tbKey.accountId, tbKey.dbName, tbKey.name)
		if err != nil {
			return err
		}
		//TODO::do parallel compaction for table
		tbl, ok := rel.(*txnTable)
		if !ok {
			delegate := rel.(*txnTableDelegate)
			tbl = delegate.origin
		}
		createdBlks, stats, err := tbl.compaction(blks)
		if err != nil {
			return err
		}
		if len(createdBlks) > 0 {
			bat := batch.NewWithSize(2)
			bat.Attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
			bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
			bat.SetVector(1, vector.NewVec(types.T_binary.ToType()))
			for _, blkInfo := range createdBlks {
				vector.AppendBytes(
					bat.GetVector(0),
					objectio.EncodeBlockInfo(blkInfo),
					false,
					tbl.getTxn().proc.GetMPool())
			}

			// append the object stats to bat
			for idx := 0; idx < len(stats); idx++ {
				if stats[idx].IsZero() {
					continue
				}
				if err = vector.AppendBytes(bat.Vecs[1], stats[idx].Marshal(),
					false, tbl.getTxn().proc.GetMPool()); err != nil {
					return err
				}
			}

			bat.SetRowCount(len(createdBlks))
			defer func() {
				bat.Clean(tbl.getTxn().proc.GetMPool())
			}()

			err := txn.WriteFileLocked(
				INSERT,
				tbl.accountId,
				tbl.db.databaseId,
				tbl.tableId,
				tbl.db.databaseName,
				tbl.tableName,
				createdBlks[0].MetaLocation().Name().String(),
				bat,
				tbl.getTxn().tnStores[0],
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

		if entry.typ == INSERT_TXN {
			continue
		}

		if entry.typ != INSERT ||
			entry.bat.Attrs[0] != catalog.BlockMeta_MetaLoc {
			continue
		}
		entry.bat.Shrink(compactedEntries[entry.bat], true)
		if entry.bat.RowCount() == 0 {
			txn.writes[i].bat.Clean(txn.proc.GetMPool())
			txn.writes[i].bat = nil
		}
	}
	return nil
}

func (txn *Transaction) hasDeletesOnUncommitedObject() bool {
	return !txn.deletedBlocks.isEmpty()
}

func (txn *Transaction) hasUncommittedDeletesOnBlock(id *types.Blockid) bool {
	return txn.deletedBlocks.hasDeletes(id)
}

// TODO:: refactor in next PR, to make it more efficient and include persisted deletes in S3
func (txn *Transaction) forEachTableHasDeletesLocked(f func(tbl *txnTable) error) error {
	tables := make(map[uint64]*txnTable)
	for i := 0; i < len(txn.writes); i++ {
		e := txn.writes[i]
		if e.typ != DELETE || e.fileName != "" {
			continue
		}
		if _, ok := tables[e.tableId]; ok {
			continue
		}
		db, err := txn.engine.Database(txn.proc.Ctx, e.databaseName, txn.op)
		if err != nil {
			return err
		}
		rel, err := db.Relation(txn.proc.Ctx, e.tableName, nil)
		if err != nil {
			return err
		}

		if v, ok := rel.(*txnTableDelegate); ok {
			tables[e.tableId] = v.origin
		} else {
			tables[e.tableId] = rel.(*txnTable)
		}

	}
	for _, tbl := range tables {
		if err := f(tbl); err != nil {
			return err
		}
	}
	return nil
}

func (txn *Transaction) forEachTableWrites(databaseId uint64, tableId uint64, offset int, f func(Entry)) {
	txn.Lock()
	defer txn.Unlock()
	for i := 0; i < offset; i++ {
		e := txn.writes[i]
		if e.databaseId != databaseId {
			continue
		}
		if e.tableId != tableId {
			continue
		}
		f(e)
	}
}

// getCachedTable returns the cached table in this transaction if it exists, nil otherwise.
// Before it gets the cached table, it checks whether the table is deleted by another
// transaction by go through the delete tables slice, and advance its cachedIndex.
func (txn *Transaction) getCachedTable(
	ctx context.Context,
	k tableKey,
) *txnTableDelegate {
	var tbl *txnTableDelegate
	if v, ok := txn.tableCache.tableMap.Load(k); ok {
		tbl = v.(*txnTableDelegate)

		tblKey := cache.TableKey{
			AccountId:  k.accountId,
			DatabaseId: k.databaseId,
			Name:       k.name,
		}
		var catache *cache.CatalogCache
		var err error
		if !txn.op.IsSnapOp() {
			catache = txn.engine.getLatestCatalogCache()
		} else {
			catache, err = txn.engine.getOrCreateSnapCatalogCache(
				ctx,
				types.TimestampToTS(txn.op.SnapshotTS()))
			if err != nil {
				return nil
			}
		}
		val := catache.GetSchemaVersion(tblKey)
		if val != nil {
			if val.Ts.Greater(tbl.origin.lastTS) && val.Version != tbl.origin.version {
				txn.tableCache.tableMap.Delete(genTableKey(k.accountId, k.name, k.databaseId))
				return nil
			}
		}

	}
	return tbl
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
	if err := txn.dumpBatchLocked(-1); err != nil {
		return nil, err
	}

	txn.traceWorkspaceLocked(true)

	if !txn.hasS3Op.Load() &&
		txn.op.TxnOptions().CheckDupEnabled() {
		if err := txn.checkDup(); err != nil {
			return nil, err
		}
	}
	reqs, err := genWriteReqs(
		ctx,
		txn.proc.GetService(),
		txn.writes,
		txn.op,
	)
	if err != nil {
		return nil, err
	}
	return reqs, nil
}

func (txn *Transaction) Rollback(ctx context.Context) error {
	logDebugf(txn.op.Txn(), "Transaction.Rollback")
	//to gc the s3 objs
	if err := txn.gcObjs(0); err != nil {
		panic("Rollback txn failed: to gc objects generated by CN failed")
	}
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
	txn.deletedDatabaseMap = nil
	txn.deletedTableMap = nil
	txn.blockId_tn_delete_metaLoc_batch.data = nil
	txn.deletedBlocks = nil
	txn.haveDDL.Store(false)
	segmentnames := make([]objectio.Segmentid, 0, len(txn.cnBlkId_Pos)+1)
	segmentnames = append(segmentnames, txn.segId)
	for blkId := range txn.cnBlkId_Pos {
		// blkId:
		// |------|----------|----------|
		//   uuid    filelen   blkoffset
		//    16        2          2
		segmentnames = append(segmentnames, *blkId.Segment())
	}
	colexec.Get().DeleteTxnSegmentIds(segmentnames)
	txn.cnBlkId_Pos = nil
	txn.hasS3Op.Store(false)
	txn.removed = true
}

func (txn *Transaction) addCreateTable(
	key tableKey,
	value *txnTable) {
	txn.Lock()
	defer txn.Unlock()
	value.createByStatementID = txn.statementID
	txn.createMap.Store(key, value)
}

func (txn *Transaction) rollbackCreateTableLocked() {
	txn.createMap.Range(func(key, value any) bool {
		if value.(*txnTable).createByStatementID == txn.statementID {
			txn.createMap.Delete(key)
		}
		return true
	})
}

func (txn *Transaction) clearTableCache() {
	txn.tableCache.tableMap.Range(func(key, value any) bool {
		txn.tableCache.tableMap.Delete(key)
		return true
	})
}

func (txn *Transaction) GetSnapshotWriteOffset() int {
	txn.Lock()
	defer txn.Unlock()
	return txn.snapshotWriteOffset
}

func (txn *Transaction) transferDeletesLocked(ctx context.Context, commit bool) error {
	var latestTs timestamp.Timestamp
	txn.timestamps = append(txn.timestamps, txn.op.SnapshotTS())
	if txn.statementID > 0 && txn.op.Txn().IsRCIsolation() {
		var ts timestamp.Timestamp
		if txn.statementID == 1 {
			ts = txn.timestamps[0]
			txn.start = time.Now()
		} else {
			//statementID > 1
			ts = txn.timestamps[txn.statementID-2]
		}
		if commit {
			if time.Since(txn.start) < time.Second*5 {
				return nil
			}
			//It's important to push the snapshot ts to the latest ts
			if err := txn.op.UpdateSnapshot(
				ctx,
				timestamp.Timestamp{}); err != nil {
				return err
			}
			latestTs = txn.op.SnapshotTS()
			txn.resetSnapshot()
		}

		return txn.forEachTableHasDeletesLocked(func(tbl *txnTable) error {
			ctx := tbl.proc.Load().Ctx
			state, err := tbl.getPartitionState(ctx)
			if err != nil {
				return err
			}
			var endTs timestamp.Timestamp
			if commit {
				endTs = latestTs
			} else {
				endTs = tbl.db.op.SnapshotTS()
			}
			deleteObjs, createObjs := state.GetChangedObjsBetween(
				types.TimestampToTS(ts),
				types.TimestampToTS(endTs))

			trace.GetService(txn.proc.GetService()).ApplyFlush(
				tbl.db.op.Txn().ID,
				tbl.tableId,
				ts,
				tbl.db.op.SnapshotTS(),
				len(deleteObjs))

			if len(deleteObjs) > 0 {
				if err := tbl.transferDeletes(ctx, state, deleteObjs, createObjs); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return nil
}

func (txn *Transaction) UpdateSnapshotWriteOffset() {
	txn.Lock()
	defer txn.Unlock()
	txn.snapshotWriteOffset = len(txn.writes)
}

func (txn *Transaction) CloneSnapshotWS() client.Workspace {
	ws := &Transaction{
		proc:     txn.proc,
		engine:   txn.engine,
		tnStores: txn.tnStores,

		tableCache: struct {
			cachedIndex int
			tableMap    *sync.Map
		}{tableMap: new(sync.Map)},
		databaseMap:        new(sync.Map),
		deletedDatabaseMap: new(sync.Map),
		createMap:          new(sync.Map),
		deletedTableMap:    new(sync.Map),
		deletedBlocks: &deletedBlocks{
			offsets: map[types.Blockid][]int64{},
		},
		cnBlkId_Pos:     map[types.Blockid]Pos{},
		batchSelectList: make(map[*batch.Batch][]int64),
		toFreeBatches:   make(map[tableKey][]*batch.Batch),
	}

	ws.blockId_tn_delete_metaLoc_batch = struct {
		sync.RWMutex
		data map[types.Blockid][]*batch.Batch
	}{data: make(map[types.Blockid][]*batch.Batch)}

	ws.readOnly.Store(true)

	return ws
}

func (txn *Transaction) BindTxnOp(op client.TxnOperator) {
	txn.op = op
}

func (txn *Transaction) SetHaveDDL(haveDDL bool) {
	txn.haveDDL.Store(haveDDL)
}

func (txn *Transaction) GetHaveDDL() bool {
	return txn.haveDDL.Load()
}
