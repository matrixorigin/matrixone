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
	"runtime"
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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/panjf2000/ants/v2"
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

		ll := bat.RowCount()
		rowIdVec, err := txn.batchAllocNewRowIds(ll)
		defer func() {
			if rowIdVec != nil {
				rowIdVec.Free(txn.proc.Mp())
			}
		}()

		if err != nil {
			return nil, err
		}

		rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](rowIdVec)

		genRowidVec = vector.NewVec(types.T_Rowid.ToType())
		for i := 0; i < ll; i++ {
			if err := vector.AppendFixed(
				genRowidVec,
				rowIds[i],
				false,
				txn.proc.Mp(),
			); err != nil {
				return nil, err
			}
		}
		bat.InsertVector(0, objectio.PhysicalAddr_Attr, genRowidVec)

		if !catalog.IsSystemTable(tableId) {
			txn.approximateInMemInsertSize += uint64(bat.Size())
			txn.approximateInMemInsertCnt += bat.RowCount()
		}
	}

	if typ == DELETE && !catalog.IsSystemTable(tableId) {
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

	if typ == DELETE && !catalog.IsSystemTable(tableId) &&
		bat != nil && bat.RowCount() > 1 {

		// attr: row_id, pk
		if err = mergeutil.SortColumnsByIndex(bat.Vecs, 0, txn.proc.Mp()); err != nil {
			return nil, err
		}

		bat.Vecs[0].SetSorted(true)
		bat.Vecs[1].SetSorted(true)
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
		if txn.databaseOps.existAndDeleted(dbkey) {
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

	// Check fault injection first - if enabled, force flush
	forceFlush := objectio.CNWorkspaceForceFlushInjected()

	//offset < 0 indicates commit.
	if offset < 0 {
		if !forceFlush && txn.approximateInMemInsertSize < txn.commitWorkspaceThreshold &&
			txn.approximateInMemInsertCnt < txn.engine.config.insertEntryMaxCount &&
			txn.approximateInMemDeleteCnt < txn.engine.config.insertEntryMaxCount {
			return nil
		}
	} else {
		if !forceFlush && txn.approximateInMemInsertSize < txn.writeWorkspaceThreshold {
			return nil
		}
	}

	dumpAll := offset < 0
	if dumpAll {
		offset = 0
	}

	if !dumpAll && !forceFlush {
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

		if size < txn.engine.config.extraWorkspaceThreshold {
			// try to increase the write threshold from quota, if failed, then dump all
			// acquire 5M more than we need
			quota := size - txn.writeWorkspaceThreshold + txn.engine.config.writeWorkspaceThreshold
			remaining, acquired := txn.engine.AcquireQuota(int64(quota))
			if acquired {
				logutil.Info(
					"WORKSPACE-QUOTA-ACQUIRE",
					zap.Uint64("quota", quota),
					zap.Uint64("remaining", uint64(remaining)),
					zap.String("txn", txn.op.Txn().DebugString()),
				)
				txn.writeWorkspaceThreshold += quota
				txn.extraWriteWorkspaceThreshold += quota
				return nil
			}
		}
		size = 0
	}
	txn.hasS3Op.Store(true)

	var (
		err error
		fs  fileservice.FileService
	)

	if fs, err = colexec.GetSharedFSFromProc(txn.proc); err != nil {
		return err
	}

	if err := txn.dumpInsertBatchLocked(ctx, fs, offset, &size, &pkCount); err != nil {
		return err
	}
	// release the extra quota
	if txn.extraWriteWorkspaceThreshold > 0 {
		remaining := txn.engine.ReleaseQuota(int64(txn.extraWriteWorkspaceThreshold))
		logutil.Info(
			"WORKSPACE-QUOTA-RELEASE",
			zap.Uint64("quota", txn.extraWriteWorkspaceThreshold),
			zap.Uint64("remaining", remaining),
			zap.String("txn", txn.op.Txn().DebugString()),
		)
		txn.extraWriteWorkspaceThreshold = 0
		txn.writeWorkspaceThreshold = txn.engine.config.writeWorkspaceThreshold
	}

	if dumpAll {
		if txn.approximateInMemDeleteCnt >= txn.engine.config.insertEntryMaxCount {
			if err := txn.dumpDeleteBatchLocked(ctx, fs, offset, &size); err != nil {
				return err
			}
			//After flushing inserts/deletes in memory into S3, the entries in txn.writes will be unordered,
			//should adjust the order to make sure deletes are in front of the inserts.
			if err := txn.adjustUpdateOrderLocked(0); err != nil {
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

func (txn *Transaction) dumpInsertBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	offset int,
	size *uint64,
	pkCount *int,
) error {

	// Check if force flush is enabled
	forceFlush := objectio.CNWorkspaceForceFlushInjected()

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

	// Skip the skipTable logic if force flush is enabled
	if !forceFlush {
		sum := 0
		for _, k := range keys {
			if tbCount[k] >= txn.engine.config.insertEntryMaxCount {
				continue
			}
			if uint64(sum+tbSize[k]) >= txn.commitWorkspaceThreshold {
				break
			}
			sum += tbSize[k]
			skipTable[k] = true
		}
	}

	lastWriteIndex := offset
	writes := txn.writes
	mp := make(map[tableKey][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		if skipTable[txn.writes[i].tableId] {
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
			continue
		}
		if txn.writes[i].isCatalog() {
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
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
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
		}
	}

	txn.writes = writes[:lastWriteIndex]

	var (
		stats    []objectio.ObjectStats
		s3Writer *colexec.CNS3Writer
		fileName string
		bat      *batch.Batch
	)

	defer func() {
		if s3Writer != nil {
			s3Writer.Close()
		}
	}()

	for tbKey := range mp {
		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl, err := txn.getTable(tbKey.accountId, tbKey.dbName, tbKey.name)
		if err != nil {
			return err
		}

		tableDef := tbl.GetTableDef(txn.proc.Ctx)
		s3Writer = colexec.NewCNS3DataWriter(
			txn.proc.GetMPool(), fs, tableDef, -1, false,
		)

		for _, bat = range mp[tbKey] {
			if err = s3Writer.Write(txn.proc.Ctx, bat); err != nil {
				return err
			}
		}

		if stats, err = s3Writer.Sync(txn.proc.Ctx); err != nil {
			return err
		}

		fileName = stats[0].ObjectLocation().String()
		if bat, err = s3Writer.FillBlockInfoBat(); err != nil {
			return err
		}

		var table *txnTable
		if v, ok := tbl.(*txnTableDelegate); ok {
			table = v.origin
		} else {
			table = tbl.(*txnTable)
		}

		if err = table.getTxn().WriteFileLocked(
			INSERT,
			table.accountId,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			bat,
			table.getTxn().tnStores[0],
		); err != nil {
			return err
		}

		s3Writer.Close()

		s3Writer = nil
	}

	return nil
}

func (txn *Transaction) dumpDeleteBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	offset int,
	size *uint64,
) error {

	deleteCnt := 0
	lastWriteIndex := offset
	writes := txn.writes
	mp := make(map[tableKey][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].isCatalog() {
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
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
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
		}
	}

	txn.writes = writes[:lastWriteIndex]

	var (
		pkCol    *plan.ColDef
		s3Writer *colexec.CNS3Writer

		stats    []objectio.ObjectStats
		fileName string
		bat      *batch.Batch
	)

	defer func() {
		if s3Writer != nil {
			s3Writer.Close()
		}
	}()

	for tbKey := range mp {
		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl, err := txn.getTable(tbKey.accountId, tbKey.dbName, tbKey.name)
		if err != nil {
			return err
		}

		pkCol = plan2.PkColByTableDef(tbl.GetTableDef(txn.proc.Ctx))
		s3Writer = colexec.NewCNS3TombstoneWriter(
			txn.proc.GetMPool(), fs, plan2.ExprType2Type(&pkCol.Typ), -1,
		)

		for i := 0; i < len(mp[tbKey]); i++ {
			if err = s3Writer.Write(txn.proc.Ctx, mp[tbKey][i]); err != nil {
				return err
			}
		}

		if stats, err = s3Writer.Sync(txn.proc.Ctx); err != nil {
			return err
		}

		fileName = stats[0].ObjectLocation().String()

		if bat, err = s3Writer.FillBlockInfoBat(); err != nil {
			return err
		}

		var table *txnTable
		if v, ok := tbl.(*txnTableDelegate); ok {
			table = v.origin
		} else {
			table = tbl.(*txnTable)
		}

		if err = table.getTxn().WriteFileLocked(
			DELETE,
			table.accountId,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			bat,
			table.getTxn().tnStores[0],
		); err != nil {
			return err
		}

		if err = s3Writer.Close(); err != nil {
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
func (txn *Transaction) registerCNObjects(
	objId types.Objectid,
	accountId uint32,
	objBat *batch.Batch,
	dbName string,
	tbName string,
) {
	txn.cnObjsSummary[objId] = Summary{
		objBat:    objBat,
		accountId: accountId,
		dbName:    dbName,
		tbName:    tbName,
	}
}

func (txn *Transaction) WriteFileLocked(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	inputBat *batch.Batch,
	tnStore DNStore,
) (err error) {

	txn.hasS3Op.Store(true)

	var (
		copied *batch.Batch
	)

	if copied, err = inputBat.Dup(txn.proc.Mp()); err != nil {
		return err
	}

	if typ == INSERT {
		col, area := vector.MustVarlenaRawData(copied.Vecs[1])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			oid := stats.ObjectName().ObjectId()
			sid := oid.Segment()

			colexec.RecordTxnUnCommitSegment(txn.op.Txn().ID, tableId, sid)
			txn.registerCNObjects(*oid, accountId, copied, databaseName, tableName)
		}
	}

	txn.readOnly.Store(false)
	txn.workspaceSize += uint64(copied.Size())

	if typ == DELETE {
		col, area := vector.MustVarlenaRawData(copied.Vecs[0])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			txn.StashFlushedTombstones(stats)
		}
	}

	entry := Entry{
		typ:          typ,
		accountId:    accountId,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          copied,
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
	tnStore DNStore,
) error {

	txn.Lock()
	defer txn.Unlock()

	return txn.WriteFileLocked(
		typ,
		accountId,
		databaseId,
		tableId,
		databaseName,
		tableName,
		fileName,
		bat,
		tnStore,
	)
}

func (txn *Transaction) deleteBatch(
	bat *batch.Batch,
	databaseId, tableId uint64,
) *batch.Batch {
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

	var (
		mp             = make(map[types.Rowid]uint8)
		deleteBlkId    = make(map[types.Blockid]bool)
		rowids         = vector.MustFixedColWithTypeCheck[types.Rowid](bat.GetVector(0))
		min1           = uint32(math.MaxUint32)
		max1           = uint32(0)
		cnRowIdOffsets = make([]int64, 0, len(rowids))
	)

	for i, rowid := range rowids {

		blkid := rowid.CloneBlockID()
		deleteBlkId[blkid] = true
		mp[rowid] = 0
		rowOffset := rowid.GetRowOffset()

		if colexec.IsDeletionOnTxnUnCommitPersisted(nil, rowid.BorrowSegmentID(), tableId, txn.op.Txn().ID) {
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
	sels := vector.GetSels()
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
	vector.PutSels(sels)
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

		// skip ALTER, DELETE, BlockMeta
		if entry.typ == ALTER ||
			entry.typ == DELETE ||
			entry.bat.Attrs[0] == catalog.BlockMeta_BlockInfo {
			continue
		}

		sels = sels[:0]
		if entry.tableId == tableId && entry.databaseId == databaseId {
			rowids := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
			if len(rowids) == 0 {
				continue
			}

			// Now, e.bat is uncommitted raw data batch which belongs to only one block allocated by CN.
			// so if e.bat is not to be deleted,skip it.
			if !deleteBlkId[rowids[0].CloneBlockID()] {
				continue
			}
			min2 := rowids[0].GetRowOffset()
			max2 := rowids[len(rowids)-1].GetRowOffset()
			if min > max2 || max < min2 {
				continue
			}
			for k, v := range rowids {
				if _, ok := mp[v]; ok {
					// if the v will be deleted, then add its index into the sels.
					sels = append(sels, int64(k))
					mp[v]++
				}
			}
			if len(sels) > 0 {
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

// one call to generate a batch of rowIds.
// in these rowIds, every objectio.BlockMaxRows rowIds share one blockId
// and the row offsets always start from 0.
// the users need to free the returned vector by themselves.
func (txn *Transaction) batchAllocNewRowIds(count int) (*vector.Vector, error) {

	var (
		ptr = 0
		ret *vector.Vector
	)

	for ptr < count {
		if err := txn.currentRowId.IncrBlk(); err != nil {
			return nil, err
		}

		ll := options.DefaultBlockMaxRows
		if ptr+ll > count {
			ll = count - ptr
		}

		if vec, err := objectio.ConstructRowidColumn(
			txn.currentRowId.BorrowBlockID(),
			0,
			uint32(ll),
			txn.proc.Mp()); err != nil {
			return nil, err
		} else if ret == nil {
			ret = vec
		} else {
			if err = ret.UnionBatch(
				vec,
				0,
				vec.Length(),
				nil,
				txn.proc.Mp()); err != nil {
				vec.Free(txn.proc.Mp())
				return nil, err
			}
			vec.Free(txn.proc.Mp())
		}

		ptr += ll
	}

	return ret, nil
}

func (txn *Transaction) mergeTxnWorkspaceLocked(ctx context.Context) error {
	txn.restoreTxnTableFunc = txn.restoreTxnTableFunc[:0]

	if len(txn.batchSelectList) > 0 {
		for _, e := range txn.writes {
			if sels, ok := txn.batchSelectList[e.bat]; ok {
				txn.approximateInMemInsertCnt -= len(sels)
				sort.Slice(sels, func(i, j int) bool {
					return sels[i] < (sels[j])
				})
				shrinkBatchWithRowids(e.bat, sels)
				delete(txn.batchSelectList, e.bat)
			}
		}
	}

	if len(txn.tablesInVain) > 0 {
		for i, e := range txn.writes {
			if _, ok := txn.tablesInVain[e.tableId]; e.bat != nil && ok {
				// if the entry contains objects, need to clean it from the disk.
				if len(e.fileName) != 0 {
					_ = txn.GCObjsByIdxRange(i, i)
				}
				e.bat.Clean(txn.proc.GetMPool())
				txn.writes[i].bat = nil
			}
		}
	}

	if err := txn.compactDeletionOnObjsLocked(ctx); err != nil {
		return err
	}

	inserts := objectio.GetReusableBitmap()
	deletes := objectio.GetReusableBitmap()

	defer func() {
		inserts.Release()
		deletes.Release()
	}()

	for i, e := range txn.writes {
		if e.bat == nil || e.bat.IsEmpty() ||
			e.bat.Attrs[0] == catalog.BlockMeta_BlockInfo || // inserts object
			e.bat.Attrs[0] == catalog.ObjectMeta_ObjectStats { // deletes object
			continue
		}

		if e.databaseId == catalog.MO_CATALOG_ID {
			continue
		}

		if e.bat.RowCount() >= objectio.BlockMaxRows/2 {
			continue
		}

		if e.typ == INSERT {
			inserts.Add(uint64(i))
		} else if e.typ == DELETE {
			deletes.Add(uint64(i))
		}
	}

	foo := func(idxes []int64) (err error) {
		for i := 0; i < len(idxes); i++ {
			a := &txn.writes[idxes[i]]
			if a.bat == nil || a.bat.RowCount() == objectio.BlockMaxRows {
				continue
			}

			merged := false
			for j := i + 1; j < len(idxes); j++ {
				b := &txn.writes[idxes[j]]
				if b.bat != nil && a.tableId == b.tableId && a.databaseId == b.databaseId &&
					a.bat.RowCount()+b.bat.RowCount() <= objectio.BlockMaxRows {
					merged = true
					if _, err = a.bat.Append(ctx, txn.proc.Mp(), b.bat); err != nil {
						return err
					}

					b.bat.Clean(txn.proc.GetMPool())
					b.bat = nil
				}

				if a.bat.RowCount() == objectio.BlockMaxRows {
					break
				}
			}

			if merged && a.typ == INSERT {
				// rewrite rowIds.
				// all the rowIds in the batch share one blkId.
				rowIdVector, err := txn.batchAllocNewRowIds(a.bat.RowCount())
				if err != nil {
					if rowIdVector != nil {
						rowIdVector.Free(txn.proc.Mp())
					}
					return err
				}

				rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](rowIdVector)
				for x := range a.bat.RowCount() {
					if err = vector.SetFixedAtWithTypeCheck[objectio.Rowid](a.bat.Vecs[0], x, rowIds[x]); err != nil {
						rowIdVector.Free(txn.proc.GetMPool())
						return err
					}
				}

				rowIdVector.Free(txn.proc.GetMPool())
			}
		}

		return nil
	}

	// this threshold may have a bad effect on the performance
	if inserts.Count()+deletes.Count() >= 30 {
		arr := vector.GetSels()
		defer func() {
			vector.PutSels(arr)
		}()

		ins := inserts.ToI64Array(&arr)
		if err := foo(ins); err != nil {
			return err
		}

		del := deletes.ToI64Array(&arr)
		if err := foo(del); err != nil {
			return err
		}

		i, j := 0, len(txn.writes)-1
		for i <= j {
			if txn.writes[i].bat == nil {
				txn.writes[i], txn.writes[j] = txn.writes[j], txn.writes[i]
				j--
			} else {
				i++
			}
		}

		txn.writes = txn.writes[:i]

		for i = range txn.writes {
			if txn.writes[i].typ == DELETE && txn.writes[i].bat.RowCount() > 1 {
				if err := mergeutil.SortColumnsByIndex(
					txn.writes[i].bat.Vecs, 0, txn.proc.Mp()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// CN blocks compaction for txn
func (txn *Transaction) compactDeletionOnObjsLocked(ctx context.Context) error {

	if txn.deletedBlocks.size() == 0 {
		return nil
	}

	// object --> blk id --> deletion
	objBlkDeletion := make(map[objectio.ObjectId]map[objectio.Blockid][]int64)
	objTables := make(map[objectio.ObjectId]tableKey)

	affectedBats := make(map[*batch.Batch]struct{})

	defer func() {
		txn.deletedBlocks.clean()
	}()

	txn.deletedBlocks.iter(
		func(blkId *types.Blockid, offsets []int64) bool {
			summary := txn.cnObjsSummary[*blkId.Object()]

			affectedBats[summary.objBat] = struct{}{}

			blkDel := objBlkDeletion[*blkId.Object()]
			if blkDel == nil {
				objBlkDeletion[*blkId.Object()] = make(map[objectio.Blockid][]int64)
				blkDel = objBlkDeletion[*blkId.Object()]
			}

			blkDel[*blkId] = offsets

			if _, ok := objTables[*blkId.Object()]; !ok {
				objTables[*blkId.Object()] = tableKey{
					accountId: summary.accountId,
					dbName:    summary.dbName,
					name:      summary.tbName,
				}
			}

			return true
		})

	waiter := sync.WaitGroup{}
	locker := sync.Mutex{}

	compactFunc := func(stats objectio.ObjectStats) {
		defer func() {
			waiter.Done()
		}()

		// need to rewrite the whole object
		objId := stats.ObjectName().ObjectId()

		tbKey := objTables[*objId]

		panicWhenFailed := func(err error, hint string) {
			logutil.Panic(hint,
				zap.Error(err),
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Uint32("account id", tbKey.accountId),
				zap.String("db name", tbKey.dbName),
				zap.String("tbl name", tbKey.name),
				zap.String("obj", stats.String()),
			)
		}

		rel, err := txn.getTable(tbKey.accountId, tbKey.dbName, tbKey.name)
		if err != nil {
			panicWhenFailed(err, "get table failed")
		}

		tbl, ok := rel.(*txnTable)
		if !ok {
			delegate := rel.(*txnTableDelegate)
			tbl = delegate.origin
		}

		locker.Lock()
		tbl.ensureSeqnumsAndTypesExpectRowid()
		locker.Unlock()

		bat, fileName, err := tbl.rewriteObjectByDeletion(ctx, stats, objBlkDeletion[*objId])
		if err != nil {
			panicWhenFailed(err, "rewrite object by deletion failed")
		}

		locker.Lock()
		defer locker.Unlock()
		if err = txn.WriteFileLocked(
			INSERT,
			tbl.accountId,
			tbl.db.databaseId,
			tbl.tableId,
			tbl.db.databaseName,
			tbl.tableName,
			fileName,
			bat,
			txn.tnStores[0],
		); err != nil {
			bat.Clean(txn.proc.Mp())
			panicWhenFailed(err, "write txn file failed")
		}

		bat.Clean(txn.proc.Mp())
	}

	dirtyObject := make([]objectio.ObjectStats, 0, 1)

	ll := len(txn.writes)
	for i, entry := range txn.writes[:ll] {
		if entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}

		if entry.typ != INSERT ||
			entry.bat.Attrs[0] != catalog.BlockMeta_BlockInfo {
			continue
		}

		if _, ok := affectedBats[entry.bat]; !ok {
			if injected, _ := objectio.LogWorkspaceInjected(
				entry.databaseName, entry.tableName); !injected {
				continue
			}
		}

		offset := 0
		col, area := vector.MustVarlenaRawData(entry.bat.Vecs[1])
		for j := range len(col) {
			stats := objectio.ObjectStats(col[j].GetByteSlice(area))

			if objBlkDeletion[*stats.ObjectName().ObjectId()] == nil {
				// clean object, no deletion on it
				bat := colexec.AllocCNS3ResultBat(false, false)
				if err := bat.Vecs[0].UnionBatch(entry.bat.Vecs[0],
					int64(offset), int(stats.BlkCnt()), nil, txn.proc.Mp()); err != nil {
					return err
				}
				if err := vector.AppendBytes(
					bat.Vecs[1], stats.Marshal(), false, txn.proc.Mp()); err != nil {
					return nil
				}

				bat.SetRowCount(bat.Vecs[0].Length())

				if err := txn.WriteFileLocked(
					INSERT,
					entry.accountId,
					entry.databaseId,
					entry.tableId,
					entry.databaseName,
					entry.tableName,
					stats.ObjectName().String(),
					bat,
					entry.tnStore,
				); err != nil {
					bat.Clean(txn.proc.Mp())
					return err
				}

				//_ = txn.GCObjsByStats(stats)
				bat.Clean(txn.proc.Mp())

			} else {
				dirtyObject = append(dirtyObject, stats)
			}

			offset += int(stats.BlkCnt())
		}

		txn.writes[i].bat.Clean(txn.proc.GetMPool())
		txn.writes[i].bat = nil
	}

	if txn.compactWorker == nil {
		txn.compactWorker, _ = ants.NewPool(min(runtime.NumCPU(), 4))
		defer func() {
			txn.compactWorker.Release()
			txn.compactWorker = nil
		}()
	}

	for _, stats := range dirtyObject {
		waiter.Add(1)
		txn.compactWorker.Submit(func() {
			compactFunc(stats)
		})
	}

	waiter.Wait()
	_ = txn.GCObjsByStats(dirtyObject...)

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
		// Database might craft a sql on the current txn to get the table,
		// so we need to unlock the txn
		txn.Unlock()
		db, err := txn.engine.Database(ctx, e.databaseName, txn.op)
		if err != nil {
			txn.Lock()
			return err
		}
		rel, err := db.Relation(ctx, e.tableName, nil)
		if err != nil {
			txn.Lock()
			return err
		}
		txn.Lock()
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
	if err := txn.GCObjsByIdxRange(0, len(txn.writes)-1); err != nil {
		panic("Rollback txn failed: to gc objects generated by CN failed")
	}
	txn.delTransaction()
	return nil
}

func (txn *Transaction) delTransaction() {
	if txn.removed {
		return
	}

	if txn.isCloneTxn {
		txn.engine.cloneTxnCache.DeleteTxn(txn.op.Txn().ID)
		txn.isCloneTxn = false
	}

	for i := range txn.writes {
		if txn.writes[i].bat == nil {
			continue
		}
		txn.writes[i].bat.Clean(txn.proc.Mp())
	}

	txn.tableCache = nil
	txn.tableOps = nil
	txn.databaseOps = nil

	txn.cn_flushed_s3_tombstone_object_stats_list = nil
	txn.deletedBlocks = nil
	txn.haveDDL.Store(false)
	colexec.Get().DeleteTxnSegmentIds(txn.op.Txn().ID)
	txn.cnObjsSummary = nil
	txn.hasS3Op.Store(false)
	txn.removed = true

	//txn.transfer.workerPool.Release()
	txn.transfer.timestamps = nil
	txn.transfer.lastTransferred = types.TS{}
	txn.transfer.pendingTransfer = false

	if txn.compactWorker != nil {
		txn.compactWorker.Release()
	}

	if txn.extraWriteWorkspaceThreshold > 0 {
		remaining := txn.engine.ReleaseQuota(int64(txn.extraWriteWorkspaceThreshold))
		logutil.Info(
			"WORKSPACE-QUOTA-RELEASE",
			zap.Uint64("quota", txn.extraWriteWorkspaceThreshold),
			zap.Uint64("remaining", remaining),
		)
		txn.extraWriteWorkspaceThreshold = 0
	}
}

func (txn *Transaction) rollbackTableOpLocked() {
	txn.databaseOps.rollbackLastStatement(txn.statementID)
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

		tableCache:   new(sync.Map),
		databaseOps:  newDbOps(),
		tableOps:     newTableOps(),
		tablesInVain: make(map[uint64]int),
		deletedBlocks: &deletedBlocks{
			offsets: map[types.Blockid][]int64{},
		},
		cnObjsSummary:   map[types.Objectid]Summary{},
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

func newDbOps() *dbOpsChain {
	return &dbOpsChain{
		names: make(map[databaseKey][]dbOp),
	}
}

func (c *dbOpsChain) addCreateDatabase(key databaseKey, statementId int, db *txnDatabase) {
	c.Lock()
	defer c.Unlock()
	c.names[key] = append(c.names[key],
		dbOp{kind: INSERT, statementId: statementId, databaseId: db.databaseId, payload: db})
}

func (c *dbOpsChain) addDeleteDatabase(key databaseKey, statementId int, did uint64) {
	c.Lock()
	defer c.Unlock()
	c.names[key] = append(c.names[key],
		dbOp{kind: DELETE, databaseId: did, statementId: statementId})
}

func (c *dbOpsChain) existAndDeleted(key databaseKey) bool {
	c.RLock()
	defer c.RUnlock()
	x, exist := c.names[key]
	if !exist {
		return false
	}
	return x[len(x)-1].kind == DELETE
}

func (c *dbOpsChain) existAndActive(key databaseKey) *txnDatabase {
	c.RLock()
	defer c.RUnlock()
	if x, exist := c.names[key]; exist && x[len(x)-1].kind == INSERT {
		return x[len(x)-1].payload
	}
	return nil
}

func (c *dbOpsChain) rollbackLastStatement(statementId int) {
	c.Lock()
	defer c.Unlock()
	for k, v := range c.names {
		i := len(v) - 1
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

func newTableOps() *tableOpsChain {
	return &tableOpsChain{
		names:       make(map[tableKey][]tableOp),
		creatdInTxn: make(map[uint64]int),
	}
}

func (c *tableOpsChain) addCreatedInTxn(tid uint64, statementid int) {
	c.Lock()
	defer c.Unlock()
	c.creatdInTxn[tid] = statementid
	// Note: we do not consider anything like table deleting or failed creating in createdInTxn map because the id is unique, and if the table is queried, it must be not deleted and created successfully.
}

func (c *tableOpsChain) existCreatedInTxn(tid uint64) bool {
	c.RLock()
	defer c.RUnlock()
	_, exist := c.creatdInTxn[tid]
	return exist
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
	for k, v := range c.creatdInTxn {
		if v == statementId {
			delete(c.creatdInTxn, k)
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
