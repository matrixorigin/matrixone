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
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
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
// truncate : it denotes the batch with typ DELETE on mo_tables is generated when Truncating
// a table.
//
// NOTE: For Insert type, rowid is generated in this function.
// Be carefule use this function multiple times for the same batch
func (txn *Transaction) WriteBatch(
	typ int, note string,
	accountId uint32,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	tnStore DNStore) (genRowidVec *vector.Vector, err error) {
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
	txn.Lock()
	defer txn.Unlock()
	// generate rowid for insert
	// TODO(aptend): move this outside WriteBatch? Call twice for the same batch will generate different rowid
	if typ == INSERT {
		if bat.Vecs[0].GetType().Oid == types.T_Rowid {
			panic("rowid should not be generated in Insert WriteBatch")
		}
		txn.genBlock()
		len := bat.RowCount()
		genRowidVec = vector.NewVec(types.T_Rowid.ToType())
		for i := 0; i < len; i++ {
			if err := vector.AppendFixed(genRowidVec, txn.genRowId(), false,
				txn.proc.Mp()); err != nil {
				return nil, err
			}
		}
		bat.Vecs = append([]*vector.Vector{genRowidVec}, bat.Vecs...)
		bat.Attrs = append([]string{objectio.PhysicalAddr_Attr}, bat.Attrs...)
		if tableId != catalog.MO_DATABASE_ID &&
			tableId != catalog.MO_TABLES_ID && tableId != catalog.MO_COLUMNS_ID {
			txn.approximateInMemInsertSize += uint64(bat.Size())
			txn.approximateInMemInsertCnt += bat.RowCount()
		}
	}

	if typ == DELETE && tableId != catalog.MO_DATABASE_ID &&
		tableId != catalog.MO_TABLES_ID && tableId != catalog.MO_COLUMNS_ID {
		txn.approximateInMemDeleteCnt += bat.RowCount()
	}

	if injected, logLevel := objectio.LogWorkspaceInjected(
		databaseName, tableName,
	); injected {
		if logLevel == 0 {
			rowCnt := 0
			if bat != nil {
				rowCnt = bat.RowCount()
			}
			logutil.Info(
				"INJECT-LOG-WORKSPACE",
				zap.String("table", tableName),
				zap.String("db", databaseName),
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.String("typ", typesNames[typ]),
				zap.Int("offset", len(txn.writes)),
				zap.Int("rows", rowCnt),
			)
		} else {
			maxCnt := 10
			if logLevel > 1 && bat != nil {
				maxCnt = bat.RowCount()
			}
			var dataStr string
			if bat != nil {
				dataStr = common.MoBatchToString(bat, maxCnt)
			}
			logutil.Info(
				"INJECT-LOG-WORKSPACE",
				zap.String("table", tableName),
				zap.String("db", databaseName),
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.String("typ", typesNames[typ]),
				zap.Int("offset", len(txn.writes)),
				zap.String("data", dataStr),
			)
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
		note:         note,
	}
	txn.writes = append(txn.writes, e)
	txn.pkCount += bat.RowCount()
	txn.workspaceSize += uint64(bat.Size())

	trace.GetService(txn.proc.GetService()).TxnWrite(txn.op, tableId, typesNames[typ], bat)
	return
}

func (txn *Transaction) dumpBatch(ctx context.Context, offset int) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.dumpBatchLocked(ctx, offset)
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
		vs := vector.MustFixedColNoTypeCheck[bool](pk)
		return checkPKDupGeneric[bool](mp, colType, vs, start, count)
	case types.T_bit:
		vs := vector.MustFixedColNoTypeCheck[uint64](pk)
		return checkPKDupGeneric[uint64](mp, colType, vs, start, count)
	case types.T_int8:
		vs := vector.MustFixedColNoTypeCheck[int8](pk)
		return checkPKDupGeneric[int8](mp, colType, vs, start, count)
	case types.T_int16:
		vs := vector.MustFixedColNoTypeCheck[int16](pk)
		return checkPKDupGeneric[int16](mp, colType, vs, start, count)
	case types.T_int32:
		vs := vector.MustFixedColNoTypeCheck[int32](pk)
		return checkPKDupGeneric[int32](mp, colType, vs, start, count)
	case types.T_int64:
		vs := vector.MustFixedColNoTypeCheck[int64](pk)
		return checkPKDupGeneric[int64](mp, colType, vs, start, count)
	case types.T_uint8:
		vs := vector.MustFixedColNoTypeCheck[uint8](pk)
		return checkPKDupGeneric[uint8](mp, colType, vs, start, count)
	case types.T_uint16:
		vs := vector.MustFixedColNoTypeCheck[uint16](pk)
		return checkPKDupGeneric[uint16](mp, colType, vs, start, count)
	case types.T_uint32:
		vs := vector.MustFixedColNoTypeCheck[uint32](pk)
		return checkPKDupGeneric[uint32](mp, colType, vs, start, count)
	case types.T_uint64:
		vs := vector.MustFixedColNoTypeCheck[uint64](pk)
		return checkPKDupGeneric[uint64](mp, colType, vs, start, count)
	case types.T_decimal64:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal64](pk)
		return checkPKDupGeneric[types.Decimal64](mp, colType, vs, start, count)
	case types.T_decimal128:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal128](pk)
		return checkPKDupGeneric[types.Decimal128](mp, colType, vs, start, count)
	case types.T_uuid:
		vs := vector.MustFixedColNoTypeCheck[types.Uuid](pk)
		return checkPKDupGeneric[types.Uuid](mp, colType, vs, start, count)
	case types.T_float32:
		vs := vector.MustFixedColNoTypeCheck[float32](pk)
		return checkPKDupGeneric[float32](mp, colType, vs, start, count)
	case types.T_float64:
		vs := vector.MustFixedColNoTypeCheck[float64](pk)
		return checkPKDupGeneric[float64](mp, colType, vs, start, count)
	case types.T_date:
		vs := vector.MustFixedColNoTypeCheck[types.Date](pk)
		return checkPKDupGeneric[types.Date](mp, colType, vs, start, count)
	case types.T_timestamp:
		vs := vector.MustFixedColNoTypeCheck[types.Timestamp](pk)
		return checkPKDupGeneric[types.Timestamp](mp, colType, vs, start, count)
	case types.T_time:
		vs := vector.MustFixedColNoTypeCheck[types.Time](pk)
		return checkPKDupGeneric[types.Time](mp, colType, vs, start, count)
	case types.T_datetime:
		vs := vector.MustFixedColNoTypeCheck[types.Datetime](pk)
		return checkPKDupGeneric[types.Datetime](mp, colType, vs, start, count)
	case types.T_enum:
		vs := vector.MustFixedColNoTypeCheck[types.Enum](pk)
		return checkPKDupGeneric[types.Enum](mp, colType, vs, start, count)
	case types.T_TS:
		vs := vector.MustFixedColNoTypeCheck[types.TS](pk)
		return checkPKDupGeneric[types.TS](mp, colType, vs, start, count)
	case types.T_Rowid:
		vs := vector.MustFixedColNoTypeCheck[types.Rowid](pk)
		return checkPKDupGeneric[types.Rowid](mp, colType, vs, start, count)
	case types.T_Blockid:
		vs := vector.MustFixedColNoTypeCheck[types.Blockid](pk)
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
		panic(moerr.NewInternalErrorNoCtxf("%s not supported", pk.GetType().String()))
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

		tableKey := genTableKey(e.accountId, e.tableName, e.databaseId, e.databaseName)
		if txn.tableOps.existAndDeleted(tableKey) {
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

		if e.typ == INSERT {
			bat := e.bat
			if index, ok := pkIndex[e.tableId]; ok && index != -1 {
				if *bat.Vecs[0].GetType() == types.T_Rowid.ToType() {
					bat2 := batch.NewWithSize(len(bat.Vecs) - 1)
					bat2.SetAttributes(bat.Attrs[1:])
					bat2.Vecs = bat.Vecs[1:]
					bat2.SetRowCount(bat.Vecs[0].Length())
					bat = bat2
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
		if e.typ == DELETE {
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
func (txn *Transaction) dumpBatchLocked(ctx context.Context, offset int) error {
	var size uint64
	var pkCount int

	//offset < 0 indicates commit.
	if offset < 0 {
		if txn.approximateInMemInsertSize < txn.commitWorkspaceThreshold &&
			txn.approximateInMemInsertCnt < txn.engine.config.insertEntryMaxCount &&
			txn.approximateInMemDeleteCnt < txn.engine.config.insertEntryMaxCount {
			return nil
		}
	} else {
		if txn.approximateInMemInsertSize < txn.writeWorkspaceThreshold {
			return nil
		}
	}

	dumpAll := offset < 0
	if dumpAll {
		offset = 0
	}

	if !dumpAll {
		for i := offset; i < len(txn.writes); i++ {
			if txn.writes[i].isCatalog() {
				continue
			}
			if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
				continue
			}
			if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
				size += uint64(txn.writes[i].bat.Size())
			}
		}
		if size < txn.writeWorkspaceThreshold {
			return nil
		}
		size = 0
	}
	txn.hasS3Op.Store(true)

	if err := txn.dumpInsertBatchLocked(ctx, offset, &size, &pkCount); err != nil {
		return err
	}

	if dumpAll {
		if txn.approximateInMemDeleteCnt >= txn.engine.config.insertEntryMaxCount {
			if err := txn.dumpDeleteBatchLocked(ctx, offset, &size); err != nil {
				return err
			}
		}
		txn.approximateInMemDeleteCnt = 0
		txn.approximateInMemInsertSize = 0
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
		txn.approximateInMemInsertSize -= size
		txn.pkCount -= pkCount
	}

	txn.workspaceSize -= size
	return nil
}

func (txn *Transaction) dumpInsertBatchLocked(ctx context.Context, offset int, size *uint64, pkCount *int) error {
	tbSize := make(map[uint64]int)
	tbCount := make(map[uint64]int)
	skipTable := make(map[uint64]bool)

	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].isCatalog() {
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			tbSize[txn.writes[i].tableId] += txn.writes[i].bat.Size()
			tbCount[txn.writes[i].tableId] += txn.writes[i].bat.RowCount()
		}
	}

	keys := make([]uint64, 0, len(tbSize))
	for k := range tbSize {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return tbSize[keys[i]] < tbSize[keys[j]]
	})
	sum := 0
	for _, k := range keys {
		if tbCount[k] >= txn.engine.config.insertEntryMaxCount {
			continue
		}
		if uint64(sum+tbSize[k]) >= txn.writeWorkspaceThreshold {
			break
		}
		sum += tbSize[k]
		skipTable[k] = true
	}

	lastWritesIndex := offset
	writes := txn.writes
	mp := make(map[tableKey][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		if skipTable[txn.writes[i].tableId] {
			writes[lastWritesIndex] = writes[i]
			lastWritesIndex++
			continue
		}
		if txn.writes[i].isCatalog() {
			writes[lastWritesIndex] = writes[i]
			lastWritesIndex++
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			writes[lastWritesIndex] = writes[i]
			lastWritesIndex++
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
			*size += uint64(bat.Size())
			*pkCount += bat.RowCount()
			// skip rowid
			newBatch := batch.NewWithSize(len(bat.Vecs) - 1)
			newBatch.SetAttributes(bat.Attrs[1:])
			newBatch.Vecs = bat.Vecs[1:]
			newBatch.SetRowCount(bat.Vecs[0].Length())
			mp[tbKey] = append(mp[tbKey], newBatch)
			defer bat.Clean(txn.proc.GetMPool())

			keepElement = false
		}

		if keepElement {
			writes[lastWritesIndex] = writes[i]
			lastWritesIndex++
		}
	}

	txn.writes = writes[:lastWritesIndex]

	for tbKey := range mp {
		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl, err := txn.getTable(tbKey.accountId, tbKey.dbName, tbKey.name)
		if err != nil {
			return err
		}

		tableDef := tbl.GetTableDef(txn.proc.Ctx)

		s3Writer, err := colexec.NewS3Writer(tableDef, 0)
		if err != nil {
			return err
		}
		defer s3Writer.Free(txn.proc.GetMPool())
		for i := 0; i < len(mp[tbKey]); i++ {
			s3Writer.StashBatch(txn.proc, mp[tbKey][i])
		}
		blockInfos, stats, err := s3Writer.SortAndSync(ctx, txn.proc)
		if err != nil {
			return err
		}
		err = s3Writer.FillBlockInfoBat(blockInfos, stats, txn.proc.GetMPool())
		if err != nil {
			return err
		}
		blockInfo := s3Writer.GetBlockInfoBat()

		lenVecs := len(blockInfo.Attrs)
		// only remain the metaLoc col and object stats
		for i := 0; i < lenVecs-2; i++ {
			blockInfo.Vecs[i].Free(txn.proc.GetMPool())
		}
		blockInfo.Vecs = blockInfo.Vecs[lenVecs-2:]
		blockInfo.Attrs = blockInfo.Attrs[lenVecs-2:]
		blockInfo.SetRowCount(blockInfo.Vecs[0].Length())

		var table *txnTable
		if v, ok := tbl.(*txnTableDelegate); ok {
			table = v.origin
		} else {
			table = tbl.(*txnTable)
		}
		fileName := objectio.DecodeBlockInfo(blockInfo.Vecs[0].GetBytesAt(0)).MetaLocation().Name().String()
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
	return nil
}

func (txn *Transaction) dumpDeleteBatchLocked(ctx context.Context, offset int, size *uint64) error {
	deleteCnt := 0
	lastWritesIndex := offset
	writes := txn.writes
	mp := make(map[tableKey][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].isCatalog() {
			writes[lastWritesIndex] = writes[i]
			lastWritesIndex++
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			writes[lastWritesIndex] = writes[i]
			lastWritesIndex++
			continue
		}

		keepElement := true
		if txn.writes[i].typ == DELETE && txn.writes[i].fileName == "" {
			tbKey := tableKey{
				accountId:  txn.writes[i].accountId,
				databaseId: txn.writes[i].databaseId,
				dbName:     txn.writes[i].databaseName,
				name:       txn.writes[i].tableName,
			}
			bat := txn.writes[i].bat
			deleteCnt += bat.RowCount()
			*size += uint64(bat.Size())

			newBat := batch.NewWithSize(len(bat.Vecs))
			newBat.SetAttributes(bat.Attrs)
			newBat.Vecs = bat.Vecs
			newBat.SetRowCount(bat.Vecs[0].Length())

			mp[tbKey] = append(mp[tbKey], newBat)
			defer bat.Clean(txn.proc.GetMPool())

			keepElement = false
		}

		if keepElement {
			writes[lastWritesIndex] = writes[i]
			lastWritesIndex++
		}
	}

	txn.writes = writes[:lastWritesIndex]

	for tbKey := range mp {
		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl, err := txn.getTable(tbKey.accountId, tbKey.dbName, tbKey.name)
		if err != nil {
			return err
		}

		s3Writer, err := colexec.NewS3TombstoneWriter()
		if err != nil {
			return err
		}
		defer s3Writer.Free(txn.proc.GetMPool())
		for i := 0; i < len(mp[tbKey]); i++ {
			s3Writer.StashBatch(txn.proc, mp[tbKey][i])
		}
		_, stats, err := s3Writer.SortAndSync(ctx, txn.proc)
		if err != nil {
			return err
		}
		bat := batch.NewWithSize(1)
		bat.Attrs = []string{catalog2.ObjectAttr_ObjectStats}
		bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
		if err = vector.AppendBytes(
			bat.GetVector(0), stats.Marshal(), false, txn.proc.GetMPool()); err != nil {
			return err
		}

		bat.SetRowCount(bat.Vecs[0].Length())

		var table *txnTable
		if v, ok := tbl.(*txnTableDelegate); ok {
			table = v.origin
		} else {
			table = tbl.(*txnTable)
		}
		fileName := stats.ObjectLocation().String()
		err = table.getTxn().WriteFileLocked(
			DELETE,
			table.accountId,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			bat,
			table.getTxn().tnStores[0],
		)
		if err != nil {
			return err
		}
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
	bat2 := bat
	if typ == INSERT {
		bat2 = batch.NewWithSize(len(bat.Vecs))
		bat2.SetAttributes([]string{catalog.BlockMeta_MetaLoc, catalog.ObjectMeta_ObjectStats})

		for idx := 0; idx < bat2.VectorCount(); idx++ {
			bat2.SetVector(int32(idx), vector.NewVec(*bat.Vecs[idx].GetType()))
		}

		blkInfosVec := bat.Vecs[0]
		for idx := 0; idx < blkInfosVec.Length(); idx++ {
			blkInfo := *objectio.DecodeBlockInfo(blkInfosVec.GetBytesAt(idx))
			vector.AppendBytes(bat2.Vecs[0], []byte(blkInfo.MetaLocation().String()),
				false, txn.proc.Mp())
			colexec.Get().PutCnSegment(blkInfo.BlockID.Segment(), colexec.CnBlockIdType)
		}

		// append obj stats, may multiple
		statsListVec := bat.Vecs[1]
		for idx := 0; idx < statsListVec.Length(); idx++ {
			vector.AppendBytes(bat2.Vecs[1], statsListVec.GetBytesAt(idx), false, txn.proc.Mp())
		}
		bat2.SetRowCount(bat.Vecs[0].Length())

		txn.insertPosForCNBlock(
			bat.GetVector(0),
			accountId,
			bat2,
			databaseName,
			tableName)
	}
	txn.readOnly.Store(false)
	txn.workspaceSize += uint64(bat2.Size())
	entry := Entry{
		typ:          typ,
		accountId:    accountId,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          bat2,
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
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](bat.GetVector(0))
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
	rowids = vector.MustFixedColWithTypeCheck[types.Rowid](bat.GetVector(0))
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
	for _, entry := range txn.writes {
		// nil batch will generated by comapction or dumpBatch
		if entry.bat == nil || entry.bat.RowCount() == 0 {
			continue
		}
		if entry.typ == ALTER || entry.typ == DELETE ||
			entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			continue
		}
		sels = sels[:0]
		if entry.tableId == tableId && entry.databaseId == databaseId {
			vs := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
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
				txn.batchSelectList[entry.bat] = append(txn.batchSelectList[entry.bat], sels...)
			}
		}
	}
}

func (txn *Transaction) allocateID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, time.Minute, moerr.CauseAllocateID)
	defer cancel()
	id, err := txn.idGen.AllocateID(ctx)
	return id, moerr.AttachCause(ctx, err)
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

func (txn *Transaction) mergeTxnWorkspaceLocked(ctx context.Context) error {
	txn.restoreTxnTableFunc = txn.restoreTxnTableFunc[:0]

	if len(txn.batchSelectList) > 0 {
		for _, e := range txn.writes {
			if sels, ok := txn.batchSelectList[e.bat]; ok {
				txn.approximateInMemInsertCnt -= e.bat.RowCount() - len(sels)
				e.bat.Shrink(sels, false)
				delete(txn.batchSelectList, e.bat)
			}
		}
	}
	if len(txn.tablesInVain) > 0 {
		for i, e := range txn.writes {
			if _, ok := txn.tablesInVain[e.tableId]; e.bat != nil && ok {
				e.bat.Clean(txn.proc.GetMPool())
				txn.writes[i].bat = nil
			}
		}
	}
	return txn.compactionBlksLocked(ctx)
}

// CN blocks compaction for txn
func (txn *Transaction) compactionBlksLocked(ctx context.Context) error {
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
		createdBlks, stats, err := tbl.compaction(ctx, blks)
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
					objectio.EncodeBlockInfo(&blkInfo),
					false,
					tbl.getTxn().proc.GetMPool())
			}

			// append the object stats to bat
			if err = vector.AppendBytes(bat.Vecs[1], stats.Marshal(),
				false, tbl.getTxn().proc.GetMPool()); err != nil {
				return err
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

// TODO::remove it after workspace refactor.
func (txn *Transaction) getUncommittedS3Tombstone(
	appendTo func(stats *objectio.ObjectStats),
) (err error) {

	txn.cn_flushed_s3_tombstone_object_stats_list.Range(func(k, v any) bool {
		ss := k.(objectio.ObjectStats)
		appendTo(&ss)
		return true
	})

	return nil
}

// TODO:: refactor in next PR, to make it more efficient and include persisted deletes in S3
func (txn *Transaction) forEachTableHasDeletesLocked(
	isObject bool,
	f func(tbl *txnTable) error) error {
	tables := make(map[uint64]*txnTable)
	for i := 0; i < len(txn.writes); i++ {
		e := txn.writes[i]
		if e.typ != DELETE || e.bat == nil || e.bat.RowCount() == 0 ||
			(!isObject && e.fileName != "" || isObject && e.fileName == "") {
			continue
		}

		if _, ok := tables[e.tableId]; ok {
			continue
		}
		ctx := context.WithValue(txn.proc.Ctx, defines.TenantIDKey{}, e.accountId)
		db, err := txn.engine.Database(ctx, e.databaseName, txn.op)
		if err != nil {
			return err
		}
		rel, err := db.Relation(ctx, e.tableName, nil)
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

func (txn *Transaction) ForEachTableWrites(databaseId uint64, tableId uint64, offset int, f func(Entry)) {
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
	_ context.Context,
	k tableKey,
) *txnTableDelegate {
	var tbl *txnTableDelegate
	if v, ok := txn.tableCache.Load(k); ok {
		tbl = v.(*txnTableDelegate)

		if txn.op.IsSnapOp() || !txn.op.Txn().IsRCIsolation() {
			// if the table has been put into tableCache in snapshot read txn, keep it as it is, do not check new version.
			return tbl
		}

		catalogCache := txn.engine.GetLatestCatalogCache()
		tblKey := &cache.TableChangeQuery{
			AccountId:  k.accountId,
			DatabaseId: k.databaseId,
			Name:       k.name,
			Version:    tbl.origin.version,
			TableId:    tbl.origin.tableId,
			Ts:         tbl.origin.lastTS,
		}
		if catalogCache.HasNewerVersion(tblKey) {
			txn.tableCache.Delete(genTableKey(k.accountId, k.name, k.databaseId, k.dbName))
			return nil
		}
	}
	return tbl
}

func (txn *Transaction) Commit(ctx context.Context) ([]txn.TxnRequest, error) {
	common.DoIfDebugEnabled(func() {
		logutil.Debug(
			"Transaction.Commit",
			zap.String("txn", txn.op.Txn().DebugString()),
		)
	})

	defer txn.delTransaction()
	if txn.readOnly.Load() {
		return nil, nil
	}

	if txn.workspaceSize > 10*mpool.MB {
		logutil.Info(
			"BIG-TXN",
			zap.Uint64("workspace-size", txn.workspaceSize),
			zap.String("txn", txn.op.Txn().DebugString()),
		)
	}

	if txn.workspaceSize > 100*mpool.MB {
		size := 0
		for _, e := range txn.writes {
			if e.bat == nil || e.bat.RowCount() == 0 {
				continue
			}
			size += e.bat.Size()
		}
		logutil.Warn(
			"BIG-TXN",
			zap.Uint64("statistical-size", txn.workspaceSize),
			zap.Int("actual-size", size),
			zap.String("txn", txn.op.Txn().DebugString()),
		)
	}

	if err := txn.IncrStatementID(ctx, true); err != nil {
		return nil, err
	}

	if err := txn.transferTombstonesByCommit(ctx); err != nil {
		return nil, err
	}

	if err := txn.mergeTxnWorkspaceLocked(ctx); err != nil {
		return nil, err
	}
	if err := txn.dumpBatchLocked(ctx, -1); err != nil {
		return nil, err
	}

	txn.traceWorkspaceLocked(true)

	if !txn.hasS3Op.Load() &&
		txn.op.TxnOptions().CheckDupEnabled() {
		if err := txn.checkDup(); err != nil {
			return nil, err
		}
	}
	reqs, err := genWriteReqs(ctx, txn)
	if err != nil {
		return nil, err
	}
	return reqs, nil
}

func (txn *Transaction) transferTombstonesByStatement(
	ctx context.Context,
	snapshotUpdated bool,
	isCommit bool) error {

	// we would prefer delay this transfer util the commit if it is a commit
	// statement. if it is not a commit statement, this transfer cannot be delay,
	// or the later statements could miss any deletes that happened before that point.
	if (snapshotUpdated || forceTransfer(ctx)) && !isCommit {

		// if this transfer is triggered by UT solely,
		// should advance the snapshot manually here.
		if !snapshotUpdated {
			if err := txn.advanceSnapshot(ctx, timestamp.Timestamp{}); err != nil {
				return err
			}
		}

		return txn.transferTombstones(ctx)

	} else {
		// pending transfer until the next statement or commit
		txn.transfer.pendingTransfer =
			txn.transfer.pendingTransfer || snapshotUpdated || forceTransfer(ctx)
	}

	return nil
}

func (txn *Transaction) transferTombstonesByCommit(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	if !txn.op.Txn().IsRCIsolation() {
		return nil
	}

	if txn.transfer.pendingTransfer ||
		forceTransfer(ctx) ||
		!skipTransfer(ctx, txn) {

		if err := txn.advanceSnapshot(ctx, timestamp.Timestamp{}); err != nil {
			return err
		}

		return txn.transferTombstones(ctx)
	}

	return nil
}

func (txn *Transaction) transferTombstones(
	ctx context.Context,
) (err error) {
	start := txn.transfer.lastTransferred
	end := types.TimestampToTS(txn.op.SnapshotTS())

	defer func() {
		txn.transfer.pendingTransfer = false
		txn.transfer.lastTransferred = end
	}()

	if err = transferInmemTombstones(ctx, txn, start, end); err != nil {
		return err
	}

	return transferTombstoneObjects(ctx, txn, start, end)
}

func forceTransfer(ctx context.Context) bool {
	return ctx.Value(UT_ForceTransCheck{}) != nil
}

func skipTransfer(ctx context.Context, txn *Transaction) bool {
	return time.Since(txn.start) < txn.engine.config.cnTransferTxnLifespanThreshold
}

func (txn *Transaction) Rollback(ctx context.Context) error {
	if !txn.ReadOnly() && len(txn.writes) > 0 {
		logutil.Info(
			"Transaction.Rollback",
			zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
		)
	}
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
		txn.writes[i].bat.Clean(txn.proc.Mp())
	}
	txn.tableCache = nil
	txn.tableOps = nil
	txn.databaseMap = nil
	txn.deletedDatabaseMap = nil
	txn.cn_flushed_s3_tombstone_object_stats_list = nil
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

	//txn.transfer.workerPool.Release()
	txn.transfer.timestamps = nil
	txn.transfer.lastTransferred = types.TS{}
	txn.transfer.pendingTransfer = false
}

func (txn *Transaction) rollbackTableOpLocked() {
	txn.tableOps.rollbackLastStatement(txn.statementID)

	for k, v := range txn.tablesInVain {
		if v == txn.statementID {
			delete(txn.tablesInVain, k)
		}
	}
}

func (txn *Transaction) clearTableCache() {
	txn.tableCache.Range(func(key, value any) bool {
		txn.tableCache.Delete(key)
		return true
	})
}

func (txn *Transaction) GetSnapshotWriteOffset() int {
	txn.Lock()
	defer txn.Unlock()
	return txn.snapshotWriteOffset
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
		idGen:    txn.idGen,

		tableCache:         new(sync.Map),
		databaseMap:        new(sync.Map),
		deletedDatabaseMap: new(sync.Map),
		tableOps:           newTableOps(),
		tablesInVain:       make(map[uint64]int),
		deletedBlocks: &deletedBlocks{
			offsets: map[types.Blockid][]int64{},
		},
		cnBlkId_Pos:     map[types.Blockid]Pos{},
		batchSelectList: make(map[*batch.Batch][]int64),
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),

		commitWorkspaceThreshold: txn.commitWorkspaceThreshold,
		writeWorkspaceThreshold:  txn.writeWorkspaceThreshold,
	}

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

func newTableOps() *tableOpsChain {
	return &tableOpsChain{
		names: make(map[tableKey][]tableOp),
	}
}

func (c *tableOpsChain) addCreateTable(key tableKey, statementId int, t *txnTable) {
	c.Lock()
	defer c.Unlock()
	c.names[key] = append(c.names[key],
		tableOp{kind: INSERT, statementId: statementId, tableId: t.tableId, payload: t})
}

func (c *tableOpsChain) addDeleteTable(key tableKey, statementId int, tid uint64) {
	c.Lock()
	defer c.Unlock()
	c.names[key] = append(c.names[key],
		tableOp{kind: DELETE, tableId: tid, statementId: statementId})
}

func (c *tableOpsChain) existAndDeleted(key tableKey) bool {
	c.RLock()
	defer c.RUnlock()
	x, exist := c.names[key]
	if !exist {
		return false
	}
	return x[len(x)-1].kind == DELETE
}

func (c *tableOpsChain) existAndActive(key tableKey) *txnTable {
	c.RLock()
	defer c.RUnlock()
	if x, exist := c.names[key]; exist && x[len(x)-1].kind == INSERT {
		return x[len(x)-1].payload
	}
	return nil
}

// queryNameByTid:
// 1. if dname or tname is not empty, the table is found active
// 2. if dname and tname is emtpy and deleted is true, the table is found deleted
// 3. if dname and tname is emtpy and deleted is false, the table is not found
func (c *tableOpsChain) queryNameByTid(tid uint64) (dname, tname string, deleted bool) {
	c.RLock()
	defer c.RUnlock()
	for k, v := range c.names {
		latest := v[len(v)-1]
		if latest.kind == INSERT && latest.tableId == tid {
			dname = k.dbName
			tname = k.name
			return
		}
	}
	for _, v := range c.names {
		for _, op := range v {
			if op.tableId == tid {
				deleted = true
				return
			}
		}
	}
	return
}

// rollbackLastStatement will rollback the operations caused by last statement in the chain.
// Note: the chain is ordered by statementId
func (c *tableOpsChain) rollbackLastStatement(statementId int) {
	c.Lock()
	defer c.Unlock()
	for k, v := range c.names {
		if len(v) == 0 {
			panic("empty names")
		}
		i := len(v) - 1
		if v[i].statementId > statementId {
			panic("rollback statement error")
		}
		for ; i >= 0; i-- {
			if v[i].statementId != statementId {
				break
			}
		}
		if i < 0 {
			delete(c.names, k)
		} else if i < len(v)-1 {
			c.names[k] = v[:i+1]
		}
	}
}

func (c *tableOpsChain) string() string {
	c.RLock()
	defer c.RUnlock()
	return stringifyMap(c.names, func(a1, a2 any) string {
		k := a1.(tableKey)
		return fmt.Sprintf("%v-%v-%v-%v:%v", k.accountId, k.databaseId, k.dbName, k.name, stringifySlice(a2, func(a any) string {
			op := a.(tableOp)
			if op.kind == DELETE {
				return fmt.Sprintf("DEL-%v@%v", op.tableId, op.statementId)
			} else {
				return fmt.Sprintf("INS-%v-%v@%v", op.payload.tableId, op.payload.tableName, op.statementId)
			}
		}))
	})
}
