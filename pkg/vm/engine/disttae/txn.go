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
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
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

func (txn *Transaction) accountWorkspaceEntryLocked(entry *Entry) {
	if entry.accountedSize != 0 {
		panic("BUG: workspace entry is already accounted")
	}
	if entry.bat == nil {
		return
	}

	entry.accountedSize = uint64(entry.bat.Size())
	txn.workspaceSize += entry.accountedSize
	if entry.fileName != "" || catalog.IsSystemTable(entry.tableId) {
		return
	}

	switch entry.typ {
	case INSERT:
		txn.approximateInMemInsertSize += entry.accountedSize
		txn.approximateInMemInsertCnt += entry.bat.RowCount()
	case DELETE:
		txn.approximateInMemDeleteCnt += entry.bat.RowCount()
	}
}

func (txn *Transaction) unaccountWorkspaceEntryLocked(entry *Entry) {
	if entry.bat == nil {
		if entry.accountedSize != 0 {
			panic("BUG: nil workspace entry has accounted bytes")
		}
		return
	}
	if entry.accountedSize != uint64(entry.bat.Size()) {
		panic("BUG: workspace entry changed outside accounting helpers")
	}
	if txn.workspaceSize < entry.accountedSize {
		panic("BUG: workspace size accounting underflow")
	}

	txn.workspaceSize -= entry.accountedSize
	if entry.fileName == "" && !catalog.IsSystemTable(entry.tableId) {
		switch entry.typ {
		case INSERT:
			if txn.approximateInMemInsertSize < entry.accountedSize ||
				txn.approximateInMemInsertCnt < entry.bat.RowCount() {
				panic("BUG: in-memory insert accounting underflow")
			}
			txn.approximateInMemInsertSize -= entry.accountedSize
			txn.approximateInMemInsertCnt -= entry.bat.RowCount()
		case DELETE:
			if txn.approximateInMemDeleteCnt < entry.bat.RowCount() {
				panic("BUG: in-memory delete accounting underflow")
			}
			txn.approximateInMemDeleteCnt -= entry.bat.RowCount()
		}
	}
	entry.accountedSize = 0
}

func (txn *Transaction) appendWorkspaceEntryLocked(entry Entry) {
	txn.accountWorkspaceEntryLocked(&entry)
	txn.writes = append(txn.writes, entry)
}

func (txn *Transaction) releaseWorkspaceEntryBatchLocked(idx int) {
	entry := &txn.writes[idx]
	if entry.bat == nil {
		return
	}

	bat := entry.bat
	txn.unaccountWorkspaceEntryLocked(entry)
	delete(txn.batchSelectList, bat)
	bat.Clean(txn.proc.GetMPool())
	entry.bat = nil
}

func (txn *Transaction) shrinkWorkspaceEntryBatchLocked(idx int, sels []int64) {
	entry := &txn.writes[idx]
	if entry.bat == nil {
		return
	}
	bat := entry.bat
	if len(sels) == 0 {
		delete(txn.batchSelectList, bat)
		return
	}

	slices.Sort(sels)
	sels = slices.Compact(sels)
	txn.unaccountWorkspaceEntryLocked(entry)
	shrinkBatchWithRowids(bat, sels)
	txn.accountWorkspaceEntryLocked(entry)
	delete(txn.batchSelectList, bat)
}

// Stage payload and new rowids before changing any retained source.
func (txn *Transaction) stageWorkspaceMerge(ctx context.Context, indexes []int64, insert bool) (out *batch.Batch, err error) {
	var view batch.Batch
	payload := func(src *batch.Batch) *batch.Batch {
		if !insert {
			return src
		}
		view.Attrs = src.Attrs[1:]
		view.Vecs = src.Vecs[1:]
		view.Recursive, view.ShuffleIDX = src.Recursive, src.ShuffleIDX
		view.SetRowCount(src.RowCount())
		return &view
	}
	out, err = txn.copyWorkspaceBatch(payload(txn.writes[indexes[0]].bat))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			out.Clean(txn.proc.Mp())
			out = nil
		}
	}()
	totalRows := out.RowCount()
	for _, idx := range indexes[1:] {
		rows := txn.writes[idx].bat.RowCount()
		if len(out.Vecs) == 0 {
			totalRows += rows
			out.SetRowCount(totalRows)
			continue
		}
		if _, err = out.Append(ctx, txn.proc.Mp(), payload(txn.writes[idx].bat)); err != nil {
			return out, err
		}
	}
	if insert {
		var rowids *vector.Vector
		rowids, err = txn.batchAllocNewRowIds(out.RowCount())
		if err != nil {
			return out, err
		}
		out.InsertVector(0, objectio.PhysicalAddr_Attr, rowids)
	}
	return out, nil
}

func (txn *Transaction) checkWorkspaceAccountingLocked() error {
	var workspaceSize uint64
	var insertSize uint64
	var insertCnt int
	var deleteCnt int
	for i := range txn.writes {
		entry := &txn.writes[i]
		if entry.bat == nil {
			if entry.accountedSize != 0 {
				return moerr.NewInternalErrorNoCtxf("entry %d: nil batch accounts %d bytes", i, entry.accountedSize)
			}
			continue
		}
		size := uint64(entry.bat.Size())
		if entry.accountedSize != size {
			return moerr.NewInternalErrorNoCtxf("entry %d: accounted size %d, batch size %d", i, entry.accountedSize, size)
		}
		workspaceSize += size
		if entry.fileName != "" || catalog.IsSystemTable(entry.tableId) {
			continue
		}
		switch entry.typ {
		case INSERT:
			insertSize += size
			insertCnt += entry.bat.RowCount()
		case DELETE:
			deleteCnt += entry.bat.RowCount()
		}
	}

	if txn.workspaceSize != workspaceSize ||
		txn.approximateInMemInsertSize != insertSize ||
		txn.approximateInMemInsertCnt != insertCnt ||
		txn.approximateInMemDeleteCnt != deleteCnt {
		return moerr.NewInternalErrorNoCtxf(
			"workspace accounting mismatch: workspace %d/%d, insert bytes %d/%d, insert rows %d/%d, delete rows %d/%d",
			txn.workspaceSize, workspaceSize,
			txn.approximateInMemInsertSize, insertSize,
			txn.approximateInMemInsertCnt, insertCnt,
			txn.approximateInMemDeleteCnt, deleteCnt,
		)
	}
	return nil
}

func (txn *Transaction) assertWorkspaceAccountingLocked() {
	if txn.engine != nil && txn.engine.config.checkWorkspaceAccounting {
		if err := txn.checkWorkspaceAccountingLocked(); err != nil {
			panic(err)
		}
		return
	}
	common.DoIfDebugEnabled(func() {
		if err := txn.checkWorkspaceAccountingLocked(); err != nil {
			panic(err)
		}
	})
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
	ctx := context.Background()
	if txn.proc != nil && txn.proc.Ctx != nil {
		ctx = txn.proc.Ctx
	}
	return txn.writeBatchWithAutoIncrEpochKnown(ctx, typ, note, accountId, databaseId, tableId,
		databaseName, tableName, bat, tnStore, 0, false)
}

func (txn *Transaction) writeBatchWithAutoIncrEpoch(
	ctx context.Context,
	typ int, note string,
	accountId uint32,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
) (genRowidVec *vector.Vector, err error) {
	return txn.writeBatchWithAutoIncrEpochKnown(ctx, typ, note, accountId, databaseId, tableId,
		databaseName, tableName, bat, tnStore, autoIncrEpoch, true)
}

func (txn *Transaction) writeBatchWithAutoIncrEpochKnown(
	ctx context.Context,
	typ int, note string,
	accountId uint32,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
	autoIncrEpochKnown bool,
) (genRowidVec *vector.Vector, err error) {
	if ctx == nil {
		return nil, moerr.NewInvalidInputNoCtx("disttae workspace write context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := txn.requireAutoIncrEpochFenceCommit(autoIncrEpoch, autoIncrEpochKnown); err != nil {
		return nil, err
	}
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

	pkCheckPos := -1
	pkCheckReady := false
	if typ == INSERT || typ == DELETE {
		// resolvePKCheckPosForWrite may reach Engine.Database, which can craft an
		// internal SQL on the current txn and reenter txn.Lock via
		// UpdateSnapshotWriteOffset. Resolve the PK position before taking txn.Lock.
		pkCheckPos, pkCheckReady, err = txn.resolvePKCheckPosForWrite(
			ctx,
			typ,
			accountId,
			databaseName,
			tableName,
			tableId,
			bat,
		)
		if err != nil {
			return nil, err
		}
		if typ == INSERT && pkCheckPos >= 0 {
			// WriteBatch prepends rowid at attr 0 for inserts after this metadata lookup.
			pkCheckPos++
		}
	}

	txn.Lock()
	defer txn.Unlock()
	// generate rowid for insert
	// TODO(aptend): move this outside WriteBatch? Call twice for the same batch will generate different rowid
	if typ == INSERT {
		if bat.Vecs[0].GetType().Oid == types.T_Rowid {
			panic("rowid should not be generated in Insert WriteBatch")
		}

		rowIdVec, allocErr := txn.batchAllocNewRowIds(bat.RowCount())
		if allocErr != nil {
			return nil, allocErr
		}
		// Only vectors admitted to this workspace transfer their rowid backing.
		// Raw/foreign WriteBatch callers retain the historical caller-owned lifetime.
		if len(bat.Vecs) > 0 && bat.Vecs[0].AllocationAccountSelection() == txn.workspaceSelection {
			genRowidVec = rowIdVec
		} else {
			genRowidVec = vector.NewVec(types.T_Rowid.ToType())
			err = vector.AppendFixedList(genRowidVec, vector.MustFixedColNoTypeCheck[types.Rowid](rowIdVec), nil, txn.proc.Mp())
			rowIdVec.Free(txn.proc.Mp())
			if err != nil {
				return nil, err
			}
		}

		bat.InsertVector(0, objectio.PhysicalAddr_Attr, genRowidVec)

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
		typ:                typ,
		accountId:          accountId,
		bat:                bat,
		tableId:            tableId,
		databaseId:         databaseId,
		tableName:          tableName,
		databaseName:       databaseName,
		tnStore:            tnStore,
		autoIncrEpoch:      autoIncrEpoch,
		autoIncrEpochKnown: autoIncrEpochKnown,
		note:               note,
		pkCheckPos:         pkCheckPos,
		pkCheckReady:       pkCheckReady,
	}
	txn.appendWorkspaceEntryLocked(e)
	txn.pkCount += bat.RowCount()

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
	pk *vector.Vector,
	vals []T,
	start, count int) (bool, string) {
	nsp := pk.GetNulls()
	for i, v := range vals[start : start+count] {
		// SQL standard: NULL != NULL, skip NULLs from duplicate check
		if nsp.Contains(uint64(start + i)) {
			continue
		}
		if _, ok := mp[v]; ok {
			entry := common.TypeStringValue(*t, v, false)
			return true, entry
		}
		mp[v] = true
	}
	return false, ""
}

// checkPKDupArray de-duplicates a narrow vector (bf16/f16/int8/uint8) primary-key
// column by its textual form, mirroring the T_array_float32/float64 cases. Vector
// columns are rejected as primary keys at DDL admission (build_ddl.go inline,
// checkPrimaryKeyPartType for ALTER, build_index_util.go for table-level), so this
// is defense-in-depth: it keeps checkPKDup from reaching its default panic if a
// narrow-vector pk ever arrives here.
func checkPKDupArray[T types.ArrayElement](
	mp map[any]bool,
	t *types.Type,
	pk *vector.Vector,
	start, count int) (bool, string) {
	nsp := pk.GetNulls()
	for i := start; i < start+count; i++ {
		if nsp.Contains(uint64(i)) {
			continue
		}
		v := types.ArrayToString[T](vector.GetArrayAt[T](pk, i))
		if _, ok := mp[v]; ok {
			entry := common.TypeStringValue(*t, pk.GetBytesAt(i), false)
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
		return checkPKDupGeneric[bool](mp, colType, pk, vs, start, count)
	case types.T_bit:
		vs := vector.MustFixedColNoTypeCheck[uint64](pk)
		return checkPKDupGeneric[uint64](mp, colType, pk, vs, start, count)
	case types.T_int8:
		vs := vector.MustFixedColNoTypeCheck[int8](pk)
		return checkPKDupGeneric[int8](mp, colType, pk, vs, start, count)
	case types.T_int16:
		vs := vector.MustFixedColNoTypeCheck[int16](pk)
		return checkPKDupGeneric[int16](mp, colType, pk, vs, start, count)
	case types.T_int32:
		vs := vector.MustFixedColNoTypeCheck[int32](pk)
		return checkPKDupGeneric[int32](mp, colType, pk, vs, start, count)
	case types.T_int64:
		vs := vector.MustFixedColNoTypeCheck[int64](pk)
		return checkPKDupGeneric[int64](mp, colType, pk, vs, start, count)
	case types.T_uint8:
		vs := vector.MustFixedColNoTypeCheck[uint8](pk)
		return checkPKDupGeneric[uint8](mp, colType, pk, vs, start, count)
	case types.T_uint16:
		vs := vector.MustFixedColNoTypeCheck[uint16](pk)
		return checkPKDupGeneric[uint16](mp, colType, pk, vs, start, count)
	case types.T_uint32:
		vs := vector.MustFixedColNoTypeCheck[uint32](pk)
		return checkPKDupGeneric[uint32](mp, colType, pk, vs, start, count)
	case types.T_uint64:
		vs := vector.MustFixedColNoTypeCheck[uint64](pk)
		return checkPKDupGeneric[uint64](mp, colType, pk, vs, start, count)
	case types.T_decimal64:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal64](pk)
		return checkPKDupGeneric[types.Decimal64](mp, colType, pk, vs, start, count)
	case types.T_decimal128:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal128](pk)
		return checkPKDupGeneric[types.Decimal128](mp, colType, pk, vs, start, count)
	case types.T_uuid:
		vs := vector.MustFixedColNoTypeCheck[types.Uuid](pk)
		return checkPKDupGeneric[types.Uuid](mp, colType, pk, vs, start, count)
	case types.T_float32:
		vs := vector.MustFixedColNoTypeCheck[float32](pk)
		return checkPKDupGeneric[float32](mp, colType, pk, vs, start, count)
	case types.T_float64:
		vs := vector.MustFixedColNoTypeCheck[float64](pk)
		return checkPKDupGeneric[float64](mp, colType, pk, vs, start, count)
	case types.T_date:
		vs := vector.MustFixedColNoTypeCheck[types.Date](pk)
		return checkPKDupGeneric[types.Date](mp, colType, pk, vs, start, count)
	case types.T_timestamp:
		vs := vector.MustFixedColNoTypeCheck[types.Timestamp](pk)
		return checkPKDupGeneric[types.Timestamp](mp, colType, pk, vs, start, count)
	case types.T_time:
		vs := vector.MustFixedColNoTypeCheck[types.Time](pk)
		return checkPKDupGeneric[types.Time](mp, colType, pk, vs, start, count)
	case types.T_datetime:
		vs := vector.MustFixedColNoTypeCheck[types.Datetime](pk)
		return checkPKDupGeneric[types.Datetime](mp, colType, pk, vs, start, count)
	case types.T_enum:
		vs := vector.MustFixedColNoTypeCheck[types.Enum](pk)
		return checkPKDupGeneric[types.Enum](mp, colType, pk, vs, start, count)
	case types.T_TS:
		vs := vector.MustFixedColNoTypeCheck[types.TS](pk)
		return checkPKDupGeneric[types.TS](mp, colType, pk, vs, start, count)
	case types.T_Rowid:
		vs := vector.MustFixedColNoTypeCheck[types.Rowid](pk)
		return checkPKDupGeneric[types.Rowid](mp, colType, pk, vs, start, count)
	case types.T_Blockid:
		vs := vector.MustFixedColNoTypeCheck[types.Blockid](pk)
		return checkPKDupGeneric[types.Blockid](mp, colType, pk, vs, start, count)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		nsp := pk.GetNulls()
		for i := start; i < start+count; i++ {
			if nsp.Contains(uint64(i)) {
				continue
			}
			v := pk.UnsafeGetStringAt(i)
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, []byte(v), false)
				return true, entry
			}
			mp[v] = true
		}
	case types.T_array_float32:
		nsp := pk.GetNulls()
		for i := start; i < start+count; i++ {
			if nsp.Contains(uint64(i)) {
				continue
			}
			v := types.ArrayToString[float32](vector.GetArrayAt[float32](pk, i))
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, pk.GetBytesAt(i), false)
				return true, entry
			}
			mp[v] = true
		}
	case types.T_array_float64:
		nsp := pk.GetNulls()
		for i := start; i < start+count; i++ {
			if nsp.Contains(uint64(i)) {
				continue
			}
			v := types.ArrayToString[float64](vector.GetArrayAt[float64](pk, i))
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, pk.GetBytesAt(i), false)
				return true, entry
			}
			mp[v] = true
		}
	case types.T_array_bf16:
		if found, entry := checkPKDupArray[types.BF16](mp, colType, pk, start, count); found {
			return true, entry
		}
	case types.T_array_float16:
		if found, entry := checkPKDupArray[types.Float16](mp, colType, pk, start, count); found {
			return true, entry
		}
	case types.T_array_int8:
		if found, entry := checkPKDupArray[int8](mp, colType, pk, start, count); found {
			return true, entry
		}
	case types.T_array_uint8:
		if found, entry := checkPKDupArray[uint8](mp, colType, pk, start, count); found {
			return true, entry
		}
	default:
		panic(moerr.NewInternalErrorNoCtxf("%s not supported", pk.GetType().String()))
	}
	return false, ""
}

// checkDup check whether the txn.writes has duplicate pk entry
func (txn *Transaction) checkDup(ctx context.Context) error {
	start := time.Now()
	defer func() {
		v2.TxnCheckPKDupDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	// Legacy fallback metadata path for entries that do not carry write-time PK info.
	tablesDef := make(map[uint64]*plan.TableDef)
	pkIndex := make(map[uint64]int)

	insertPks := make(map[uint64]map[any]bool)
	delPks := make(map[uint64]map[any]bool)

	legacyPKIndex := func(e Entry) (int, error) {
		if idx, ok := pkIndex[e.tableId]; ok {
			return idx, nil
		}
		if _, ok := tablesDef[e.tableId]; !ok {
			tbl, err := txn.getTable(ctx, e.accountId, e.databaseName, e.tableName)
			if err != nil {
				return -1, err
			}
			tablesDef[e.tableId] = tbl.GetTableDef(txn.proc.Ctx)
		}
		tableDef := tablesDef[e.tableId]
		pkIndex[e.tableId] = -1
		if tableDef != nil && tableDef.Pkey != nil {
			for idx, colDef := range tableDef.Cols {
				if colDef.Name == tableDef.Pkey.PkeyColName {
					if colDef.Name != catalog.FakePrimaryKeyColName &&
						colDef.Name != catalog.CPrimaryKeyColName {
						pkIndex[e.tableId] = idx
					}
					break
				}
			}
		}
		return pkIndex[e.tableId], nil
	}

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

		if e.typ == INSERT {
			fallbackToLegacy := !e.pkCheckReady
			if e.pkCheckReady {
				index := e.pkCheckPos
				if index >= 0 {
					if index >= len(e.bat.Vecs) || index >= len(e.bat.Attrs) {
						logutil.Warnf("pk check pos out of range, database:%s, table:%s, pos:%d, attrs:%v",
							e.databaseName, e.tableName, index, e.bat.Attrs)
						fallbackToLegacy = true
					} else {
						if _, ok := insertPks[e.tableId]; !ok {
							insertPks[e.tableId] = make(map[any]bool)
						}
						if dup, pk := checkPKDup(
							insertPks[e.tableId],
							e.bat.Vecs[index],
							0,
							e.bat.RowCount()); dup {
							logutil.Errorf("txn:%s wants to insert duplicate primary key:%s in table:[%v-%v:%s-%s], mode:%s",
								hex.EncodeToString(txn.op.Txn().ID),
								pk,
								e.databaseId,
								e.tableId,
								e.databaseName,
								e.tableName,
								"write-entry")
							return moerr.NewDuplicateEntryNoCtx(pk, e.bat.Attrs[index])
						}
					}
				}
				if !fallbackToLegacy {
					continue
				}
			}

			bat := e.bat
			index, err := legacyPKIndex(e)
			if err != nil {
				return err
			}
			if index != -1 {
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
					logutil.Errorf("txn:%s wants to insert duplicate primary key:%s in table:[%v-%v:%s-%s], mode:%s",
						hex.EncodeToString(txn.op.Txn().ID),
						pk,
						e.databaseId,
						e.tableId,
						e.databaseName,
						e.tableName,
						"legacy-tabledef")
					return moerr.NewDuplicateEntryNoCtx(pk, bat.Attrs[index])
				}
			}
			continue
		}
		//if entry.tyep is DELETE, then e.bat.Vecs[0] is rowid,e.bat.Vecs[1] is PK
		if e.typ == DELETE {
			fallbackToLegacy := !e.pkCheckReady
			if e.pkCheckReady {
				index := e.pkCheckPos
				if index >= 0 {
					if index >= len(e.bat.Vecs) || index >= len(e.bat.Attrs) {
						logutil.Warnf("pk check pos out of range, database:%s, table:%s, pos:%d, attrs:%v",
							e.databaseName, e.tableName, index, e.bat.Attrs)
						fallbackToLegacy = true
					} else {
						if _, ok := delPks[e.tableId]; !ok {
							delPks[e.tableId] = make(map[any]bool)
						}
						if dup, pk := checkPKDup(
							delPks[e.tableId],
							e.bat.Vecs[index],
							0,
							e.bat.RowCount()); dup {
							logutil.Errorf("txn:%s wants to delete duplicate primary key:%s in table:[%v-%v:%s-%s], mode:%s",
								hex.EncodeToString(txn.op.Txn().ID),
								pk,
								e.databaseId,
								e.tableId,
								e.databaseName,
								e.tableName,
								"write-entry")
							return moerr.NewDuplicateEntryNoCtx(pk, e.bat.Attrs[index])
						}
					}
				}
				if !fallbackToLegacy {
					continue
				}
			}

			if len(e.bat.Vecs) < 2 {
				logutil.Warnf("delete has no pk, database:%s, table:%s",
					e.databaseName, e.tableName)
				continue
			}
			index, err := legacyPKIndex(e)
			if err != nil {
				return err
			}
			if index != -1 {
				if _, ok := delPks[e.tableId]; !ok {
					delPks[e.tableId] = make(map[any]bool)
				}
				if dup, pk := checkPKDup(
					delPks[e.tableId],
					e.bat.Vecs[1],
					0,
					e.bat.RowCount()); dup {
					logutil.Errorf("txn:%s wants to delete duplicate primary key:%s in table:[%v-%v:%s-%s], mode:%s",
						hex.EncodeToString(txn.op.Txn().ID),
						pk,
						e.databaseId,
						e.tableId,
						e.databaseName,
						e.tableName,
						"legacy-tabledef")
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
// scanInMemInsertSize sums the in-memory INSERT batch sizes in writes[from:].
func (txn *Transaction) scanInMemInsertSize(from int) uint64 {
	var size uint64
	for i := from; i < len(txn.writes); i++ {
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
	return size
}

func (txn *Transaction) dumpBatchLocked(ctx context.Context, offset int) (err error) {
	if err := txn.handoffPendingObjectCleanupLocked(false); err != nil {
		return err
	}
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
		forceDump := false
		size = txn.scanInMemInsertSize(offset)
		if size < txn.writeWorkspaceThreshold {
			// Safety valve: even though the current statement's writes are small,
			// the global workspace may have accumulated too much data (e.g., when
			// IncrStatementID is disabled during HNSW index creation via RunSql).
			// In that case, rescan from the current statement's start to dump all
			// accumulated data within this statement. We use the statement boundary
			// (not 0) to avoid compacting prior statements' entries, which would
			// break offsets[] used by RollbackLastStatement.
			if txn.approximateInMemInsertSize >= txn.engine.config.extraWorkspaceThreshold {
				stmtStart := 0
				if txn.statementID > 0 {
					stmtStart = txn.offsets[txn.statementID-1]
				}
				logutil.Info(
					"WORKSPACE-FORCE-DUMP",
					zap.Uint64("approximateInMemInsertSize", txn.approximateInMemInsertSize),
					zap.Uint64("extraWorkspaceThreshold", txn.engine.config.extraWorkspaceThreshold),
					zap.Int("stmtStart", stmtStart),
					zap.String("txn", txn.op.Txn().DebugString()),
				)
				offset = stmtStart
				size = txn.scanInMemInsertSize(stmtStart)
				forceDump = true
			} else {
				return nil
			}
		}

		if !forceDump && size < txn.engine.config.extraWorkspaceThreshold {
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
	}
	if offset < txn.adjustWriteOffset {
		txn.adjustWriteOffset = offset
	}
	txn.hasS3Op.Store(true)

	var fs fileservice.FileService

	if fs, err = colexec.GetSharedFSFromProc(txn.proc); err != nil {
		return err
	}

	kinds := []int{INSERT}
	if dumpAll {
		kinds = append(kinds, DELETE)
	}
	tables, ok, resolveErr := txn.resolveDumpTablesLocked(ctx, offset, nil, kinds[0], kinds[1:]...)
	if resolveErr != nil {
		return resolveErr
	}
	if !ok {
		return nil
	}
	skipTable := txn.dumpInsertSkipTablesLocked(offset, forceFlush)
	var combined workspaceDumpStage
	defer func() {
		if err != nil {
			combined.abort(txn)
		}
	}()
	if err := txn.dumpWorkspaceTablesLocked(ctx, fs, offset, INSERT, skipTable, tables, &pkCount, &combined); err != nil {
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

	orderDeletes := dumpAll && txn.approximateInMemDeleteCnt >= txn.engine.config.insertEntryMaxCount
	if orderDeletes {
		if err := txn.dumpWorkspaceTablesLocked(ctx, fs, offset, DELETE, nil, tables, nil, &combined); err != nil {
			return err
		}
	}
	pkCount += combined.publish(txn)
	if orderDeletes {
		// This stable in-memory reorder has no fallible operation; its implementation
		// always returns nil. It runs only after the complete dump is prepared.
		if txn.compactionUndo != nil {
			txn.compactionDeferredOrder = true
		} else {
			_ = txn.adjustUpdateOrderLocked(0)
		}
	}
	txn.pkCount -= pkCount
	if dumpAll && txn.compactionUndo == nil {
		txn.compactNilWorkspaceSlotsLocked()
	}

	txn.assertWorkspaceAccountingLocked()
	return nil
}

func (txn *Transaction) dumpInsertSkipTablesLocked(offset int, forceFlush bool) map[uint64]bool {
	// Check if force flush is enabled

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
	slices.SortFunc(keys, func(a, b uint64) int {
		return cmp.Compare(tbSize[a], tbSize[b])
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

	return skipTable
}

func (txn *Transaction) dumpInsertBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	offset int,
	pkCount *int,
	combined ...*workspaceDumpStage,
) error {

	skipTable := txn.dumpInsertSkipTablesLocked(offset, objectio.CNWorkspaceForceFlushInjected())

	// Resolve every table that will be flushed BEFORE mutating txn.writes:
	// getTable must never run while this goroutine holds the lock. See
	// resolveDumpTablesLocked for the window contract.
	tables, ok, err := txn.resolveDumpTablesLocked(ctx, offset, skipTable, INSERT)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	return txn.dumpWorkspaceTablesLocked(ctx, fs, offset, INSERT, skipTable, tables, pkCount, combined...)
}

func (txn *Transaction) dumpDeleteBatchLocked(ctx context.Context, fs fileservice.FileService, offset int, combined ...*workspaceDumpStage) error {
	tables, ok, err := txn.resolveDumpTablesLocked(ctx, offset, nil, DELETE)
	if err != nil || !ok {
		return err
	}
	return txn.dumpWorkspaceTablesLocked(ctx, fs, offset, DELETE, nil, tables, nil, combined...)
}

type workspaceDumpStage struct {
	anchors   []int
	records   []*stagedWorkspaceRecord
	retired   []int
	persisted []string
}

func (s *workspaceDumpStage) abort(txn *Transaction) {
	for _, st := range s.records {
		txn.retractUnpublishedRecordAdmissionLocked(st)
		if st.bat != nil {
			st.bat.Clean(txn.proc.Mp())
		}
	}
	txn.retainDefiniteOrphansLocked(s.persisted, "workspace dump staging failed")
}
func (s *workspaceDumpStage) publish(txn *Transaction) int {
	rows := 0
	for _, idx := range s.retired {
		if txn.writes[idx].typ == INSERT {
			rows += txn.writes[idx].bat.RowCount()
		}
	}
	anchors := make(map[int]struct{}, len(s.anchors))
	for i, st := range s.records {
		anchor := s.anchors[i]
		anchors[anchor] = struct{}{}
		txn.publishStagedWorkspaceRecordAtLocked(st, anchor)
	}
	for _, idx := range s.retired {
		if _, anchor := anchors[idx]; !anchor {
			txn.releaseWorkspaceEntryBatchLocked(idx)
		}
	}
	s.records = nil
	s.anchors = nil
	s.retired = nil
	s.persisted = nil
	return rows
}

// Sources remain retained until all writers, metadata copies and admissions succeed.
func (txn *Transaction) dumpWorkspaceTablesLocked(ctx context.Context, fs fileservice.FileService, offset, typ int, skip map[uint64]bool, tables map[tableKey]engine.Relation, pkCount *int, combined ...*workspaceDumpStage) (err error) {
	// Keep every replacement inside its captured statement/snapshot prefix.
	type dumpGroup struct {
		workspaceTableKey
		region        int
		afterSnapshot bool
	}
	groups := make(map[dumpGroup][]int)
	snapshot := txn.GetSnapshotWriteOffset()
	for i := offset; i < len(txn.writes); i++ {
		e := &txn.writes[i]
		if e.bat == nil || e.bat.IsEmpty() || e.isCatalog() || e.typ != typ || e.fileName != "" || skip[e.tableId] {
			continue
		}
		key := workspaceTableKey{tableKey: tableKey{accountId: e.accountId, databaseId: e.databaseId, dbName: e.databaseName, name: e.tableName}, autoIncrEpoch: e.autoIncrEpoch, autoIncrEpochKnown: e.autoIncrEpochKnown}
		if _, ok := tables[key.tableKey]; ok {
			region, _ := slices.BinarySearch(txn.offsets, i+1)
			group := dumpGroup{key, region, i >= snapshot}
			groups[group] = append(groups[group], i)
		}
	}
	stages := make([]*stagedWorkspaceRecord, 0, len(groups))
	var anchors []int
	var persisted []string
	var retired []int
	defer func() {
		if err == nil {
			return
		}
		for _, st := range stages {
			txn.retractUnpublishedRecordAdmissionLocked(st)
			if st.bat != nil {
				st.bat.Clean(txn.proc.Mp())
			}
		}
		txn.retainDefiniteOrphansLocked(persisted, "workspace dump staging failed")
	}()
	for key, indexes := range groups {
		e := txn.writes[indexes[0]]
		def := tables[key.tableKey].GetTableDef(ctx)
		var writer *colexec.CNS3Writer
		if typ == INSERT {
			writer = colexec.NewCNS3DataWriter(txn.proc.Mp(), fs, def, -1, false, txn.dumpSinkerCapacityOption())
		} else {
			pk := plan2.PkColByTableDef(def)
			writer = colexec.NewCNS3TombstoneWriter(txn.proc.Mp(), fs, plan2.ExprType2Type(&pk.Typ), -1, txn.dumpSinkerCapacityOption())
		}
		// Each writer is joined and hands back names before its close, including errors.
		stage, writeErr := func() (record *stagedWorkspaceRecord, writeErr error) {
			defer func() { persisted = append(persisted, writer.AbandonAndRetainableNames()...); _ = writer.Close() }()
			for _, idx := range indexes {
				src := txn.writes[idx].bat
				var view batch.Batch
				view.Attrs, view.Vecs = src.Attrs, src.Vecs
				if typ == INSERT {
					view.Attrs, view.Vecs = src.Attrs[1:], src.Vecs[1:]
				}
				view.Recursive, view.ShuffleIDX = src.Recursive, src.ShuffleIDX
				view.SetRowCount(src.RowCount())
				if werr := writer.Write(ctx, &view); werr != nil {
					return nil, werr
				}
			}
			stats, werr := writer.Sync(ctx)
			if werr != nil {
				return nil, werr
			}
			if len(stats) == 0 {
				return nil, moerr.NewInternalErrorNoCtx("workspace dump produced no objects")
			}
			metadata, werr := writer.FillBlockInfoBat()
			if werr != nil {
				return nil, werr
			}
			return txn.stageWorkspaceRecordLocked(typ, e.accountId, e.databaseId, e.tableId, e.databaseName, e.tableName, stats[0].ObjectLocation().String(), metadata, e.tnStore, key.autoIncrEpoch, key.autoIncrEpochKnown, false)
		}()
		if writeErr != nil {
			return writeErr
		}
		stages = append(stages, stage)
		anchors = append(anchors, indexes[0])
		retired = append(retired, indexes...)
	}
	prepared := workspaceDumpStage{records: stages, anchors: anchors, retired: retired, persisted: persisted}
	if len(combined) > 0 {
		combined[0].records = append(combined[0].records, prepared.records...)
		combined[0].anchors = append(combined[0].anchors, prepared.anchors...)
		combined[0].retired = append(combined[0].retired, prepared.retired...)
		combined[0].persisted = append(combined[0].persisted, prepared.persisted...)
		return nil
	}
	rows := prepared.publish(txn)
	if pkCount != nil {
		*pkCount += rows
	}

	return nil
}

// Inert outside deterministic tests of the unlocked resolution boundary.
var dumpResolveBeforeRelock func(int)

func (txn *Transaction) resolveDumpTablesLocked(ctx context.Context, offset int, skipTable map[uint64]bool, typ int, additional ...int) (tables map[tableKey]engine.Relation, ok bool, err error) {
	tables = make(map[tableKey]engine.Relation)
	kinds := append([]int{typ}, additional...)
	for {
		var keys []tableKey
		seen := make(map[tableKey]bool)
		for i := offset; i < len(txn.writes); i++ {
			e := &txn.writes[i]
			if skipTable[e.tableId] || e.isCatalog() || e.bat == nil || e.bat.IsEmpty() || e.fileName != "" || !slices.Contains(kinds, e.typ) {
				continue
			}
			key := tableKey{accountId: e.accountId, databaseId: e.databaseId, dbName: e.databaseName, name: e.tableName}
			if _, resolved := tables[key]; !resolved && !seen[key] {
				seen[key] = true
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 {
			return tables, true, nil
		}
		txn.Unlock()
		for _, key := range keys {
			rel, lookupErr := txn.getTable(ctx, key.accountId, key.dbName, key.name)
			if lookupErr != nil {
				txn.Lock()
				return nil, false, lookupErr
			}
			tables[key] = rel
		}
		if dumpResolveBeforeRelock != nil {
			for _, kind := range kinds {
				dumpResolveBeforeRelock(kind)
			}
		}
		objectio.CNDumpResolveWindowWait()
		txn.Lock()
		if txn.incrMaintenanceActive && txn.incrMaintenanceInvalid {
			return nil, false, context.Canceled
		}
		if offset > len(txn.writes) {
			return nil, false, nil
		}
		// Rollback, truncation or a new source may have changed the set while
		// unlocked. Resolve any missing keys before staging a single source index.
	}
}

func (txn *Transaction) getTable(
	ctx context.Context,
	id uint32,
	dbName string,
	tbName string,
) (engine.Relation, error) {
	if txn.engine == nil {
		return nil, moerr.NewInternalErrorNoCtx("disttae txn engine is nil")
	}
	if ctx == nil {
		return nil, moerr.NewInvalidInputNoCtx("disttae table lookup context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if injected, rogueUpdate, errorOut := objectio.CNReenterSnapshotOffsetOnGetTableInjected(); injected {
		// Test-only fault: deterministically simulate the internal-SQL leg
		// of the issue #25557 deadlock (getTable -> Engine.Database ->
		// loadDatabaseFromStorage -> execReadSql -> NewCompile) without
		// requiring a catalog cache miss. The internal SQL's compile captures
		// the workspace write offset — WriteOffset takes txn.Lock, so this
		// self-deadlocks (and the test times out) whenever getTable is
		// reached with the lock held.
		_ = txn.WriteOffset()
		if rogueUpdate {
			// simulate a rogue statement-boundary advance that internal SQL
			// must never perform; kept to pin down the damage it would cause
			txn.UpdateSnapshotWriteOffset()
		}
		if errorOut {
			return nil, moerr.NewInternalErrorNoCtx(
				"fault injection: reenter snapshot write offset on getTable")
		}
	}

	var txnOp client.TxnOperator
	if txn.op != nil {
		txnOp = txn.op
	} else if txn.proc != nil {
		txnOp = txn.proc.GetTxnOperator()
	}
	if txnOp == nil {
		return nil, moerr.NewInternalErrorNoCtx("disttae txn operator is nil")
	}

	ctx = defines.AttachAccountId(ctx, id)

	database, err := txn.engine.Database(ctx, dbName, txnOp)
	if err != nil {
		return nil, err
	}
	tbl, err := database.Relation(ctx, tbName, nil)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (txn *Transaction) resolvePKCheckPosForWrite(
	ctx context.Context,
	typ int,
	accountId uint32,
	databaseName, tableName string,
	tableId uint64,
	bat *batch.Batch,
) (int, bool, error) {
	if bat == nil || bat.RowCount() == 0 {
		return -1, true, nil
	}

	if typ != INSERT && typ != DELETE {
		return -1, true, nil
	}

	if tableId == catalog.MO_TABLES_ID ||
		tableId == catalog.MO_COLUMNS_ID ||
		tableId == catalog.MO_DATABASE_ID {
		return -1, true, nil
	}
	if txn.engine == nil {
		return -1, false, nil
	}

	tbl, err := txn.getTable(ctx, accountId, databaseName, tableName)
	if err != nil {
		return -1, false, err
	}
	tableDef := tbl.GetTableDef(defines.AttachAccountId(ctx, accountId))
	if tableDef == nil || tableDef.Pkey == nil {
		return -1, true, nil
	}

	pkName := tableDef.Pkey.PkeyColName
	if pkName == "" ||
		pkName == catalog.FakePrimaryKeyColName ||
		pkName == catalog.CPrimaryKeyColName {
		return -1, true, nil
	}

	if typ == DELETE {
		if len(bat.Vecs) < 2 {
			logutil.Warnf("delete has no pk vector, database:%s, table:%s", databaseName, tableName)
			return -1, false, nil
		}
		return 1, true, nil
	}

	for i, attr := range bat.Attrs {
		if attr == pkName {
			return i, true, nil
		}
	}
	for i, attr := range bat.Attrs {
		if strings.EqualFold(attr, pkName) {
			return i, true, nil
		}
	}

	logutil.Warnf("pk column %s not found in write attrs, database:%s, table:%s, attrs:%v",
		pkName, databaseName, tableName, bat.Attrs)
	return -1, false, nil
}

// vec contains block infos.
func (txn *Transaction) registerCNObjects(
	objId types.Objectid,
	accountId uint32,
	objBat *batch.Batch,
	dbName string,
	tbName string,
	autoIncrEpoch uint32,
	autoIncrEpochKnown bool,
) {
	txn.cnObjsSummary[objId] = Summary{
		objBat:             objBat,
		accountId:          accountId,
		dbName:             dbName,
		tbName:             tbName,
		autoIncrEpoch:      autoIncrEpoch,
		autoIncrEpochKnown: autoIncrEpochKnown,
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
	return txn.writeFileLockedWithAutoIncrEpochKnown(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, inputBat, tnStore, 0, false)
}

func (txn *Transaction) writeFileLockedWithAutoIncrEpoch(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	inputBat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
) (err error) {
	return txn.writeFileLockedWithAutoIncrEpochKnown(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, inputBat, tnStore, autoIncrEpoch, true)
}

type stagedWorkspaceRecord struct {
	Entry
	admittedNames              []string
	handoffIDs                 []ioutil.RemoteWorkspaceHandoffID
	handoffTarget              remoteWorkspaceRecordTarget
	handoffReceiptReservations int
}

func (txn *Transaction) stageWorkspaceRecordLocked(typ int, accountId uint32, databaseId, tableId uint64, databaseName, tableName, fileName string, inputBat *batch.Batch, tnStore DNStore, autoIncrEpoch uint32, autoIncrEpochKnown, skipTransfer bool) (*stagedWorkspaceRecord, error) {
	if err := txn.requireAutoIncrEpochFenceCommit(autoIncrEpoch, autoIncrEpochKnown); err != nil {
		return nil, err
	}
	target := remoteWorkspaceRecordTarget{typ: typ, accountID: accountId, databaseID: databaseId, tableID: tableId, autoIncrEpoch: autoIncrEpoch, autoIncrEpochKnown: autoIncrEpochKnown, skipTransfer: skipTransfer}
	keep, handoffIDs, remote, err := txn.selectRemoteWorkspaceRecordsLocked(typ, inputBat, target)
	if err != nil {
		return nil, err
	}
	if remote && len(keep) == 0 {
		return nil, nil
	}
	if err = txn.reserveRemoteCoordinatorReceipts(len(handoffIDs)); err != nil {
		return nil, err
	}
	reservationsOwned := true
	defer func() {
		if reservationsOwned {
			kept := 0
			if remote {
				kept = txn.retainRejectedRemoteRecordsLocked(handoffIDs, target)
			}
			if err := txn.releaseRemoteCoordinatorReceipts(len(handoffIDs) - kept); err != nil {
				logutil.Errorf("remote workspace receipt reservation remains owned: %v", err)
			}
		}
	}()
	if remote && objectio.CNWorkspaceRemoteRecordRefusal(databaseName) {
		return nil, mpool.ErrAllocationAccountCapacity
	}
	copied, err := txn.copyWorkspaceBatchForTable(inputBat, databaseName)
	if err != nil {
		return nil, err
	}
	recordBat := inputBat
	if remote {
		column := 1
		if typ == DELETE {
			column = 0
		}
		if len(keep) != inputBat.Vecs[column].Length() {
			if err = shrinkRemoteWorkspaceMetadata(typ, copied, keep); err != nil {
				copied.Clean(txn.proc.Mp())
				return nil, err
			}
			stats := objectio.ObjectStats(copied.Vecs[column].GetBytesAt(0))
			fileName = stats.ObjectLocation().String()
		}
		copied.ExtraBuf = append(copied.ExtraBuf[:0], inputBat.ExtraBuf...)
		recordBat = copied
	}
	if err = txn.registerUnpublishedObjectRecordLocked(typ, recordBat); err != nil {
		copied.Clean(txn.proc.Mp())
		return nil, err
	}
	admittedNames := txn.recordedUnpublishedObjectNamesLocked(typ, recordBat)
	if remote {
		copied.ExtraBuf = nil
	}
	reservationsOwned = false
	return &stagedWorkspaceRecord{Entry: Entry{typ: typ, accountId: accountId, databaseId: databaseId, tableId: tableId, databaseName: databaseName, tableName: tableName, fileName: fileName, bat: copied, tnStore: tnStore, autoIncrEpoch: autoIncrEpoch, autoIncrEpochKnown: autoIncrEpochKnown, skipTransfer: skipTransfer, pkCheckPos: -1, pkCheckReady: true}, admittedNames: admittedNames, handoffIDs: handoffIDs, handoffTarget: target, handoffReceiptReservations: len(handoffIDs)}, nil
}
func (txn *Transaction) publishStagedWorkspaceRecordLocked(s *stagedWorkspaceRecord) {
	txn.publishStagedWorkspaceRecordAtLocked(s, -1)
}
func (txn *Transaction) publishStagedWorkspaceRecordAtLocked(s *stagedWorkspaceRecord, anchor int) {
	txn.hasS3Op.Store(true)
	if s == nil {
		return
	}

	if s.typ == INSERT {
		server := colexec.MustGetServer(txn.engine.service)
		col, area := vector.MustVarlenaRawData(s.bat.Vecs[1])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			oid := stats.ObjectName().ObjectId()
			sid := oid.Segment()

			server.PutCnSegment(txn.op.Txn().ID,
				s.tableId, sid, colexec.TxnWorkspaceUnCommitType)
			txn.registerCNObjects(*oid, s.accountId,
				s.bat, s.databaseName, s.tableName, s.autoIncrEpoch, s.autoIncrEpochKnown)
		}
	}

	txn.readOnly.Store(false)

	if s.typ == DELETE {
		col, area := vector.MustVarlenaRawData(s.bat.Vecs[0])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			txn.StashFlushedTombstones(stats)
		}
	}

	entry := Entry{
		typ:                s.typ,
		accountId:          s.accountId,
		tableId:            s.tableId,
		databaseId:         s.databaseId,
		tableName:          s.tableName,
		databaseName:       s.databaseName,
		fileName:           s.fileName,
		bat:                s.bat,
		tnStore:            s.tnStore,
		pkCheckPos:         -1,
		pkCheckReady:       true,
		skipTransfer:       s.skipTransfer,
		autoIncrEpoch:      s.autoIncrEpoch,
		autoIncrEpochKnown: s.autoIncrEpochKnown,
	}
	if anchor < 0 {
		txn.appendWorkspaceEntryLocked(entry)
	} else {
		txn.releaseWorkspaceEntryBatchLocked(anchor)
		txn.writes[anchor] = entry
		txn.accountWorkspaceEntryLocked(&txn.writes[anchor])
	}
	if len(s.admittedNames) > 0 {
		txn.engine.unpublishedPending.mu.Lock()
		txn.engine.finishReservationProducersLocked(s.admittedNames)
		txn.engine.unpublishedPending.mu.Unlock()
	}
	if len(s.handoffIDs) > 0 {
		if txn.remoteWorkspaceRecords == nil {
			txn.remoteWorkspaceRecords = make(map[ioutil.RemoteWorkspaceHandoffID]remoteWorkspaceRecordReceipt, len(s.handoffIDs))
		}
		q := &txn.engine.unpublishedPending
		q.mu.Lock()
		if q.remoteCoordinatorDecisions == nil {
			q.remoteCoordinatorDecisions = make(map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision, len(s.handoffIDs))
		}
		for _, id := range s.handoffIDs {
			txn.remoteWorkspaceRecords[id] = remoteWorkspaceRecordReceipt{target: s.handoffTarget, decision: ioutil.RemoteWorkspaceHandoffAdopted}
			q.remoteCoordinatorDecisions[id] = ioutil.RemoteWorkspaceHandoffAdopted
		}
		q.mu.Unlock()
		s.handoffReceiptReservations = 0 // transaction now owns the reservations
	}
	s.bat = nil // ownership passed to the workspace entry
}

func (txn *Transaction) retractUnpublishedRecordAdmissionLocked(st *stagedWorkspaceRecord) {
	if st == nil {
		return
	}
	if st.handoffReceiptReservations > 0 {
		if err := txn.releaseRemoteCoordinatorReceipts(st.handoffReceiptReservations); err != nil {
			logutil.Errorf("remote workspace receipt reservation remains owned: %v", err)
			return
		}
		st.handoffReceiptReservations = 0
	}
	if len(st.admittedNames) == 0 {
		return
	}
	txn.recordedUnpublishedObjects.retract(st.admittedNames)
}
func (txn *Transaction) writeFileLockedWithAutoIncrEpochKnown(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	inputBat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
	autoIncrEpochKnown bool,
) (err error) {
	st, err := txn.stageWorkspaceRecordLocked(typ, accountId, databaseId, tableId, databaseName, tableName, fileName, inputBat, tnStore, autoIncrEpoch, autoIncrEpochKnown, false)
	if err != nil {
		return err
	}
	txn.publishStagedWorkspaceRecordLocked(st)
	return nil
}

func (txn *Transaction) requireAutoIncrEpochFenceCommit(autoIncrEpoch uint32, autoIncrEpochKnown bool) error {
	if !autoIncrEpochKnown || autoIncrEpoch == 0 {
		return nil
	}
	if client.RequireAutoIncrEpochFenceCommit(txn.op) {
		return nil
	}
	return moerr.NewNotSupported(txn.proc.Ctx, "transaction operator cannot enforce AUTO_INCREMENT epochs")
}

// WriteFileLockedSkipTransfer is similar to WriteFileLocked but marks the entry
// to skip transfer processing. Used by CCPR for cross-cluster tombstones.
func (txn *Transaction) WriteFileLockedSkipTransfer(
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
	return txn.writeFileLockedSkipTransferWithAutoIncrEpochKnown(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, inputBat, tnStore, 0, false)
}

func (txn *Transaction) writeFileLockedSkipTransferWithAutoIncrEpoch(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	inputBat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
) (err error) {
	return txn.writeFileLockedSkipTransferWithAutoIncrEpochKnown(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, inputBat, tnStore, autoIncrEpoch, true)
}

func (txn *Transaction) writeFileLockedSkipTransferWithAutoIncrEpochKnown(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	inputBat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
	autoIncrEpochKnown bool,
) (err error) {
	if err := txn.requireAutoIncrEpochFenceCommit(autoIncrEpoch, autoIncrEpochKnown); err != nil {
		return err
	}

	txn.hasS3Op.Store(true)

	var (
		copied *batch.Batch
	)

	if copied, err = txn.copyWorkspaceBatchForTable(inputBat, databaseName); err != nil {
		return err
	}

	if typ == INSERT {
		server := colexec.MustGetServer(txn.engine.service)
		col, area := vector.MustVarlenaRawData(copied.Vecs[1])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			oid := stats.ObjectName().ObjectId()
			sid := oid.Segment()

			server.PutCnSegment(txn.op.Txn().ID, tableId, sid, colexec.TxnWorkspaceUnCommitType)
			txn.registerCNObjects(*oid, accountId, copied, databaseName, tableName, autoIncrEpoch, autoIncrEpochKnown)
		}
	}

	txn.readOnly.Store(false)

	if typ == DELETE {
		col, area := vector.MustVarlenaRawData(copied.Vecs[0])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			txn.StashFlushedTombstones(stats)
		}
	}

	entry := Entry{
		typ:                typ,
		accountId:          accountId,
		tableId:            tableId,
		databaseId:         databaseId,
		tableName:          tableName,
		databaseName:       databaseName,
		fileName:           fileName,
		bat:                copied,
		tnStore:            tnStore,
		skipTransfer:       true,
		autoIncrEpoch:      autoIncrEpoch,
		autoIncrEpochKnown: autoIncrEpochKnown,
	}

	txn.appendWorkspaceEntryLocked(entry)

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
	return txn.writeFileWithAutoIncrEpochKnown(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, bat, tnStore, 0, false)
}

func (txn *Transaction) writeFileWithAutoIncrEpoch(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
) error {
	return txn.writeFileWithAutoIncrEpochKnown(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, bat, tnStore, autoIncrEpoch, true)
}

func (txn *Transaction) writeFileWithAutoIncrEpochKnown(
	typ int,
	accountId uint32,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	tnStore DNStore,
	autoIncrEpoch uint32,
	autoIncrEpochKnown bool,
) error {

	txn.Lock()
	defer txn.Unlock()

	return txn.writeFileLockedWithAutoIncrEpochKnown(
		typ,
		accountId,
		databaseId,
		tableId,
		databaseName,
		tableName,
		fileName,
		bat,
		tnStore,
		autoIncrEpoch,
		autoIncrEpochKnown,
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
	server := colexec.MustGetServer(txn.engine.service)

	for i, rowid := range rowids {

		blkid := rowid.CloneBlockID()
		deleteBlkId[blkid] = true
		mp[rowid] = 0
		rowOffset := rowid.GetRowOffset()

		if server.GetCnSegmentType(rowid.BorrowSegmentID(), tableId, txn.op.Txn().ID) == colexec.TxnWorkspaceUnCommitType {
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

func (txn *Transaction) addBatchSelectionsLocked(bat *batch.Batch, sels []int64) {
	selections := append(txn.batchSelectList[bat], sels...)
	slices.Sort(selections)
	txn.batchSelectList[bat] = slices.Compact(selections)
}

func (txn *Transaction) selectAllBatchRowsLocked(bat *batch.Batch) {
	selections := txn.batchSelectList[bat][:0]
	for i := range bat.RowCount() {
		selections = append(selections, int64(i))
	}
	txn.batchSelectList[bat] = selections
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
		if entry.tableId != tableId || entry.databaseId != databaseId {
			continue
		}
		if entry.typ == ALTER || entry.typ == DELETE {
			continue
		}
		// nil batch will generated by comapction or dumpBatch
		if entry.bat == nil || entry.bat.RowCount() == 0 {
			continue
		}

		// skip BlockMeta
		if entry.bat.Attrs[0] == catalog.BlockMeta_BlockInfo {
			continue
		}

		sels = sels[:0]
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
			txn.addBatchSelectionsLocked(entry.bat, sels)
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
func (txn *Transaction) batchAllocNewRowIds(count int) (ret *vector.Vector, err error) {
	if count == 0 {
		return nil, nil
	}
	ret, err = txn.newWorkspaceVector(types.T_Rowid.ToType())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			ret.Free(txn.proc.Mp())
			ret = nil
		}
	}()
	if err = ret.PreExtend(count, txn.proc.Mp()); err != nil {
		return ret, err
	}
	for ptr := 0; ptr < count; {
		if err = txn.currentRowId.IncrBlk(); err != nil {
			return ret, err
		}
		ll := min(options.DefaultBlockMaxRows, count-ptr)
		for i := 0; i < ll; i++ {
			rid := objectio.NewRowid(txn.currentRowId.BorrowBlockID(), uint32(i))
			if err = vector.AppendFixed(ret, rid, false, txn.proc.Mp()); err != nil {
				return ret, err
			}
		}
		ptr += ll
	}
	return ret, nil
}

// visitWorkspaceMergeGroups partitions ordered write indexes before the
// pairwise merge scan. No group crosses a statement or snapshot boundary.
func visitWorkspaceMergeGroups(idxes []int64, offsets []int, snapshot int64, visit func([]int64) error) error {
	region := func(idx int64) int { r, _ := slices.BinarySearch(offsets, int(idx)+1); return r }
	for start := 0; start < len(idxes); {
		end := start + 1
		r := region(idxes[start])
		beforeSnapshot := idxes[start] < snapshot
		for end < len(idxes) && region(idxes[end]) == r && (idxes[end] < snapshot) == beforeSnapshot {
			end++
		}
		if err := visit(idxes[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (txn *Transaction) mergeTxnWorkspaceLocked(ctx context.Context) error {
	txn.restoreTxnTableFunc = txn.restoreTxnTableFunc[:0]

	if len(txn.batchSelectList) > 0 {
		for i := range txn.writes {
			if sels, ok := txn.batchSelectList[txn.writes[i].bat]; ok {
				txn.shrinkWorkspaceEntryBatchLocked(i, sels)
			}
		}
	}

	if len(txn.tablesInVain) > 0 {
		ignoreTablesInVain := func(entry Entry) bool {
			_, ok := txn.tablesInVain[entry.tableId]
			return ok
		}
		for i, e := range txn.writes {
			if _, ok := txn.tablesInVain[e.tableId]; e.bat != nil && ok {
				// if the entry contains objects, need to clean it from the disk.
				// Skip GC for CCPR transactions - CCPRTxnCache handles GC to avoid deleting shared objects
				if len(e.fileName) != 0 && !txn.isCCPRTxn {
					txn.unprotectUnreferencedTxnLocalSharedFilesLocked(
						collectObjectStatsFromEntry(e),
						ignoreTablesInVain,
					)
					_ = txn.GCObjsByIdxRange(i, i)
				}
				txn.releaseWorkspaceEntryBatchLocked(i)
			}
		}
		txn.assertWorkspaceAccountingLocked()
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

	// Prepare every group and delete sort before publishing any merged entry.
	if inserts.Count()+deletes.Count() >= 30 {
		type preparedMerge struct {
			indexes []int64
			bat     *batch.Batch
		}
		var prepared []preparedMerge
		defer func() {
			for _, p := range prepared {
				if p.bat != nil {
					p.bat.Clean(txn.proc.Mp())
				}
			}
		}()
		used := make(map[int64]bool)
		prepareRegion := func(idxes []int64) error {
			for i, idx := range idxes {
				dst := &txn.writes[idx]
				if used[idx] || dst.bat == nil || dst.bat.RowCount() == objectio.BlockMaxRows {
					continue
				}
				indexes := []int64{idx}
				rows := dst.bat.RowCount()
				for _, j := range idxes[i+1:] {
					src := &txn.writes[j]
					if !used[j] && src.bat != nil && src.tableId == dst.tableId && src.databaseId == dst.databaseId && rows+src.bat.RowCount() <= objectio.BlockMaxRows {
						indexes = append(indexes, j)
						rows += src.bat.RowCount()
					}
				}
				if len(indexes) == 1 && dst.typ != DELETE {
					continue
				}
				staged, err := txn.stageWorkspaceMerge(ctx, indexes, dst.typ == INSERT)
				if err != nil {
					return err
				}
				if dst.typ == DELETE && staged.RowCount() > 1 {
					var sortBuf []int64
					var shuffleBuf []byte
					if err = mergeutil.SortColumnsByIndexWithBuf(staged.Vecs, 0, txn.proc.Mp(), &sortBuf, &shuffleBuf); err != nil {
						staged.Clean(txn.proc.Mp())
						return err
					}
				}
				for _, j := range indexes {
					used[j] = true
				}
				prepared = append(prepared, preparedMerge{indexes, staged})
			}
			return nil
		}
		// Bitmap iteration is ordered by write index. Split at statement boundaries
		// before comparing entries, so unrelated historical statements do not
		// incur a quadratic scan at every statement transition.
		prepare := func(idxes []int64) error {
			return visitWorkspaceMergeGroups(idxes, txn.offsets, int64(txn.GetSnapshotWriteOffset()), prepareRegion)
		}
		arr := vector.GetSels()
		defer vector.PutSels(arr)
		if err := prepare(inserts.ToI64Array(&arr)); err != nil {
			return err
		}
		if err := prepare(deletes.ToI64Array(&arr)); err != nil {
			return err
		}
		for i := range prepared {
			p := &prepared[i]
			dst := &txn.writes[p.indexes[0]]
			old := dst.bat
			txn.unaccountWorkspaceEntryLocked(dst)
			dst.bat = p.bat
			p.bat = nil
			txn.accountWorkspaceEntryLocked(dst)
			delete(txn.batchSelectList, old)
			for _, j := range p.indexes[1:] {
				txn.releaseWorkspaceEntryBatchLocked(int(j))
			}
			old.Clean(txn.proc.Mp())
		}
	}

	txn.assertWorkspaceAccountingLocked()
	return nil
}

func (txn *Transaction) compactNilWorkspaceSlotsLocked() {
	writes := txn.writes
	oldSnapshot, oldAdjust := int(txn.snapshotWriteOffset.Load()), txn.adjustWriteOffset
	newSnapshot, newAdjust := 0, 0
	n, boundary := 0, 0
	for i, e := range writes {
		for boundary < len(txn.offsets) && txn.offsets[boundary] <= i {
			txn.offsets[boundary] = n
			boundary++
		}
		if i == oldSnapshot {
			newSnapshot = n
		}
		if i == oldAdjust {
			newAdjust = n
		}
		if e.bat != nil {
			writes[n] = e
			n++
		}
	}
	for boundary < len(txn.offsets) {
		txn.offsets[boundary] = n
		boundary++
	}
	if oldSnapshot >= len(writes) {
		newSnapshot = n
	}
	if oldAdjust >= len(writes) {
		newAdjust = n
	}
	clear(writes[n:])
	txn.writes = writes[:n]
	txn.snapshotWriteOffset.Store(int64(newSnapshot))
	txn.adjustWriteOffset = newAdjust
}

// resolveCompactTablesLocked resolves the engine.Relation of every table
// that object-deletion compaction will rewrite. The compaction workers must
// never call getTable themselves: they run while this goroutine holds
// txn.Lock and waits for them, and getTable may run internal SQL whose read
// pipeline locks the workspace — the worker would then wait for the lock
// owner that waits for the worker. So all tables are resolved up front, with
// the lock released around the getTable calls (same contract as
// resolveDumpTablesLocked). Because other goroutines may add deletions while
// the lock is released, the scan-resolve cycle repeats until every table
// referenced by txn.deletedBlocks is resolved.
func (txn *Transaction) resolveCompactTablesLocked(ctx context.Context) (map[tableKey]engine.Relation, error) {
	tables := make(map[tableKey]engine.Relation)
	for {
		var missing []tableKey
		seen := make(map[tableKey]bool)
		txn.deletedBlocks.iter(
			func(blkId *types.Blockid, offsets []int64) bool {
				summary := txn.cnObjsSummary[*blkId.Object()]
				k := tableKey{
					accountId: summary.accountId,
					dbName:    summary.dbName,
					name:      summary.tbName,
				}
				if _, ok := tables[k]; !ok && !seen[k] {
					seen[k] = true
					missing = append(missing, k)
				}
				return true
			})
		if len(missing) == 0 {
			return tables, nil
		}

		txn.Unlock()
		for _, k := range missing {
			tbl, err := txn.getTable(ctx, k.accountId, k.dbName, k.name)
			if err != nil {
				txn.Lock()
				return nil, err
			}
			tables[k] = tbl
		}
		txn.Lock()
		if txn.incrMaintenanceActive && txn.incrMaintenanceInvalid {
			return nil, context.Canceled
		}
	}
}

// CN blocks compaction for txn
type compactRepublication struct {
	anchor int
	bat    *batch.Batch
	name   string
}

func (txn *Transaction) compactDeletionOnObjsLocked(ctx context.Context) error {
	if txn.compactionUndo != nil {
		return moerr.NewInvalidStateNoCtx("workspace compaction already has an unresolved publication")
	}

	if txn.deletedBlocks.size() == 0 {
		return nil
	}

	tables, err := txn.resolveCompactTablesLocked(ctx)
	if err != nil {
		return err
	}
	if txn.deletedBlocks.size() == 0 {
		return nil
	}

	objBlkDeletion := make(map[objectio.ObjectId]map[objectio.Blockid][]int64)
	objTables := make(map[objectio.ObjectId]workspaceTableKey)

	affectedBats := make(map[*batch.Batch]struct{})

	txn.deletedBlocks.iter(
		func(blkId *types.Blockid, offsets []int64) bool {
			summary := txn.cnObjsSummary[*blkId.Object()]

			affectedBats[summary.objBat] = struct{}{}

			blkDel := objBlkDeletion[*blkId.Object()]
			if blkDel == nil {
				objBlkDeletion[*blkId.Object()] = make(map[objectio.Blockid][]int64)
				blkDel = objBlkDeletion[*blkId.Object()]
			}

			// The worker runs after iter releases deletedBlocks' read lock. Keep
			// its snapshot independent from concurrent appends to the same block.
			blkDel[*blkId] = slices.Clone(offsets)

			if _, ok := objTables[*blkId.Object()]; !ok {
				objTables[*blkId.Object()] = workspaceTableKey{
					tableKey: tableKey{
						accountId: summary.accountId,
						dbName:    summary.dbName,
						name:      summary.tbName,
					},
					autoIncrEpoch:      summary.autoIncrEpoch,
					autoIncrEpochKnown: summary.autoIncrEpochKnown,
				}
			}

			return true
		})

	type compactRewrite struct {
		anchor    int
		tbl       *txnTable
		tbKey     workspaceTableKey
		writeName string
		bat       *batch.Batch
		fileName  string
		persisted []string
		err       error
	}

	waiter := sync.WaitGroup{}

	rewrites := make([]compactRewrite, 0, 8)

	dirtyObject := make([]objectio.ObjectStats, 0, 1)
	dirtyAnchors := make([]int, 0, 1)

	retired := make([]int, 0, 4)
	staged := make([]compactRepublication, 0, 8)
	cleanStagedRepubs := func() {
		for k := range staged {
			if staged[k].bat != nil {
				staged[k].bat.Clean(txn.proc.Mp())
			}
		}
		staged = staged[:0]
	}

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

		if err := txn.requireAutoIncrEpochFenceCommit(
			entry.autoIncrEpoch, entry.autoIncrEpochKnown); err != nil {
			cleanStagedRepubs()
			return err
		}

		offset := 0
		col, area := vector.MustVarlenaRawData(entry.bat.Vecs[1])
		for j := range len(col) {
			stats := objectio.ObjectStats(col[j].GetByteSlice(area))

			if objBlkDeletion[*stats.ObjectName().ObjectId()] == nil {

				bat := colexec.AllocCNS3ResultBat(false)
				if err := bat.Vecs[0].UnionBatch(entry.bat.Vecs[0],
					int64(offset), int(stats.BlkCnt()), nil, txn.proc.Mp()); err != nil {
					bat.Clean(txn.proc.Mp())
					cleanStagedRepubs()
					return err
				}
				if err := vector.AppendBytes(
					bat.Vecs[1], stats.Marshal(), false, txn.proc.Mp()); err != nil {
					bat.Clean(txn.proc.Mp())
					cleanStagedRepubs()
					return err
				}

				bat.SetRowCount(bat.Vecs[0].Length())

				entryBat, err := txn.copyWorkspaceBatch(bat)
				bat.Clean(txn.proc.Mp())
				if err != nil {
					cleanStagedRepubs()
					return err
				}

				staged = append(staged, compactRepublication{anchor: i, bat: entryBat, name: stats.ObjectName().String()})
			} else {
				dirtyObject = append(dirtyObject, stats)
				dirtyAnchors = append(dirtyAnchors, i)
			}

			offset += int(stats.BlkCnt())
		}

		retired = append(retired, i)
	}

	for i := range dirtyObject {
		stats := dirtyObject[i]
		objId := stats.ObjectName().ObjectId()
		tbKey := objTables[*objId]

		tbl, _ := tables[tbKey.tableKey].(*txnTable)
		if tbl == nil {
			delegate, _ := tables[tbKey.tableKey].(*txnTableDelegate)
			tbl = delegate.origin
		}
		tbl.ensureSeqnumsAndTypesExpectRowid()
		writeName := tbl.tableName
		rewrites = append(rewrites, compactRewrite{
			anchor:    dirtyAnchors[i],
			tbl:       tbl,
			tbKey:     tbKey,
			writeName: writeName,
		})
	}
	if len(rewrites) > 0 {
		if txn.compactWorker == nil {
			txn.compactWorker, err = ants.NewPool(min(runtime.NumCPU(), 4))
			if err != nil {
				cleanStagedRepubs()
				return err
			}
			defer func() {
				txn.compactWorker.Release()
				txn.compactWorker = nil
			}()
		}
		for i := range rewrites {
			waiter.Add(1)
			stats := dirtyObject[i]
			blkDeletion := objBlkDeletion[*dirtyObject[i].ObjectName().ObjectId()]
			submitErr := txn.compactWorker.Submit(func() {
				defer waiter.Done()
				tbl := rewrites[i].tbl
				bat, fileName, persisted, err := tbl.rewriteObjectByDeletion(
					ctx, stats, blkDeletion)
				rewrites[i].bat = bat
				rewrites[i].fileName = fileName
				rewrites[i].persisted = persisted
				rewrites[i].err = err
			})
			if submitErr != nil {
				waiter.Done()
				rewrites[i].err = submitErr
			}
		}
		waiter.Wait()
	}

	var compactErr error
	recordStages := make([]*stagedWorkspaceRecord, 0, len(rewrites))
	recordAnchors := make([]int, 0, len(rewrites))
	for i := range rewrites {
		r := &rewrites[i]
		if r.err != nil {
			if compactErr == nil {
				compactErr = r.err
			}
			continue
		}
		if compactErr != nil {
			continue
		}
		if r.bat == nil {
			if len(r.persisted) > 0 {
				txn.retainDefiniteOrphansLocked(r.persisted,
					"the fully deleted rewrite staged no replacement record: the recording side fail-open-retains the degenerate persisted name so its file never loses its cleanup owner (issue #25590 round-32 review)")
			}
			continue
		}
		st, err := txn.stageWorkspaceRecordLocked(
			INSERT,
			r.tbl.accountId,
			r.tbl.db.databaseId,
			r.tbl.tableId,
			r.tbl.db.databaseName,
			r.writeName,
			r.fileName,
			r.bat,
			txn.tnStores[0],
			r.tbKey.autoIncrEpoch,
			r.tbKey.autoIncrEpochKnown,
			false,
		)
		if err != nil {
			compactErr = err
			continue
		}
		recordStages = append(recordStages, st)
		recordAnchors = append(recordAnchors, r.anchor)
	}

	// Build one replacement per original entry before publishing any of them.
	// This preserves captured prefix and rollback offsets even when an entry
	// contains several clean and rewritten objects.
	groups := make(map[int]*stagedWorkspaceRecord, len(retired))
	addGroup := func(anchor int, src **batch.Batch, name string, names []string) error {
		g := groups[anchor]
		if g == nil {
			g = &stagedWorkspaceRecord{Entry: txn.writes[anchor]}
			g.bat = *src
			*src = nil
			g.fileName = name
			groups[anchor] = g
		} else if _, err := g.bat.AppendWithCopy(ctx, txn.proc.Mp(), *src); err != nil {
			return err
		}
		g.admittedNames = append(g.admittedNames, names...)
		return nil
	}
	if compactErr == nil {
		for i, st := range recordStages {
			if compactErr = addGroup(recordAnchors[i], &st.bat, st.fileName, st.admittedNames); compactErr != nil {
				break
			}
		}
	}
	if compactErr == nil {
		for i := range staged {
			if compactErr = addGroup(staged[i].anchor, &staged[i].bat, staged[i].name, nil); compactErr != nil {
				break
			}
		}
	}

	if compactErr != nil {
		for _, g := range groups {
			g.bat.Clean(txn.proc.Mp())
		}
		for _, st := range recordStages {
			txn.retractUnpublishedRecordAdmissionLocked(st)
			if st.bat != nil {
				st.bat.Clean(txn.proc.Mp())
				st.bat = nil
			}
		}
		cleanStagedRepubs()
		for i := range rewrites {
			r := &rewrites[i]
			if r.bat != nil {
				r.bat.Clean(txn.proc.Mp())
				r.bat = nil
			}
			if len(r.persisted) == 0 {
				continue
			}
			txn.retainDefiniteOrphansLocked(
				r.persisted,
				"object compaction admitted a replacement's persistence but returned before recording any replacement: no append could survive the statement rollback, so the names hand to the cleanup queue and the marks stay for the retried admission (issue #25590 round-19/20 reviews)")
		}
		return compactErr
	}

	undo := &workspaceCompactionUndo{
		entries:    make([]workspaceCompactionOldEntry, 0, len(retired)),
		summaries:  make(map[types.Objectid]workspaceCompactionOldSummary),
		deleted:    make(map[types.Blockid][]int64),
		oldObjects: slices.Clone(dirtyObject),
	}
	for _, blocks := range objBlkDeletion {
		for id, offsets := range blocks {
			undo.deleted[id] = offsets
		}
	}
	// Reserve the journal's retirement slot before replacing any anchor.
	// Failure rollback can then retain the displaced backing without growing
	// the slice while memory is already exhausted.
	txn.retiredCompactions = slices.Grow(txn.retiredCompactions, 1)
	for _, anchor := range retired {
		old := txn.writes[anchor]
		txn.unaccountWorkspaceEntryLocked(&txn.writes[anchor])
		old.accountedSize = 0
		txn.writes[anchor].bat = nil
		held := workspaceCompactionOldEntry{anchor: anchor, entry: old}
		if g := groups[anchor]; g != nil {
			for _, stats := range collectObjectStatsFromEntry(g.Entry) {
				id := *stats.ObjectName().ObjectId()
				if _, exists := undo.summaries[id]; !exists {
					value, present := txn.cnObjsSummary[id]
					undo.summaries[id] = workspaceCompactionOldSummary{value: value, present: present}
				}
			}
			undo.newNames = append(undo.newNames, g.admittedNames...)
			held.replacement = g.bat
			txn.publishStagedWorkspaceRecordAtLocked(g, anchor)
		}
		undo.entries = append(undo.entries, held)
	}
	txn.compactionUndo = undo
	for _, st := range recordStages {
		if st.bat != nil {
			st.bat.Clean(txn.proc.Mp())
		}
		st.bat = nil
	}
	for i := range rewrites {
		r := &rewrites[i]
		if r.bat != nil {
			r.bat.Clean(txn.proc.Mp())
			r.bat = nil
		}
	}
	cleanStagedRepubs()
	txn.assertWorkspaceAccountingLocked()

	return nil
}

func (txn *Transaction) unprotectUnreferencedTxnLocalSharedFilesLocked(
	statsList []objectio.ObjectStats,
	ignoreEntry func(Entry) bool,
) {
	if !txn.isCloneTxn ||
		txn.op == nil ||
		txn.engine == nil ||
		txn.engine.cloneTxnCache == nil {
		return
	}

	txnID := txn.op.Txn().ID
	for i := range statsList {
		name := statsList[i].ObjectName().String()
		if txn.hasLiveObjectStatsRefLocked(name, ignoreEntry) {
			continue
		}
		txn.engine.cloneTxnCache.RemoveTxnLocalSharedFile(txnID, name)
	}
}

func (txn *Transaction) hasLiveObjectStatsRefLocked(
	name string,
	ignoreEntry func(Entry) bool,
) bool {
	for _, entry := range txn.writes {
		if ignoreEntry != nil && ignoreEntry(entry) {
			continue
		}
		if entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}

		for _, stats := range collectObjectStatsFromEntry(entry) {
			if stats.ObjectName().String() == name {
				return true
			}
		}
	}

	return false
}

func collectObjectStatsFromEntry(entry Entry) []objectio.ObjectStats {
	if entry.bat == nil || entry.bat.IsEmpty() {
		return nil
	}

	statsIdx := -1
	for i, attr := range entry.bat.Attrs {
		if attr == catalog.ObjectMeta_ObjectStats {
			statsIdx = i
			break
		}
	}
	if statsIdx == -1 {
		return nil
	}

	vec := entry.bat.Vecs[statsIdx]
	statsList := make([]objectio.ObjectStats, 0, vec.Length())
	for i := range vec.Length() {
		statsList = append(statsList, objectio.ObjectStats(vec.GetBytesAt(i)))
	}
	return statsList
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
			(!isObject && e.fileName != "" || isObject && e.fileName == "") ||
			e.skipTransfer {
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
		if txn.incrMaintenanceActive && txn.incrMaintenanceInvalid {
			return context.Canceled
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
	// defensive: an offset captured before a workspace compaction may exceed
	// the current length; never index past the end
	for i := 0; i < offset && i < len(txn.writes); i++ {
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
	return txn.getCachedTableByKey(ctx, k, k)
}

func (txn *Transaction) getCachedTableByKey(
	_ context.Context,
	k tableKey,
	cacheKey any,
) *txnTableDelegate {
	var tbl *txnTableDelegate
	if v, ok := txn.tableCache.Load(cacheKey); ok {
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

func (txn *Transaction) Commit(ctx context.Context) (reqs []txn.TxnRequest, err error) {
	common.DoIfDebugEnabled(func() {
		logutil.Debug(
			"Transaction.Commit",
			zap.String("txn", txn.op.Txn().DebugString()),
		)
	})

	if txn.readOnly.Load() {
		if txn.haveDDL.Load() {
			if pending := txn.pendingCreatedDatabaseWrites(); len(pending) != 0 {
				return nil, missingCreatedDatabaseWriteError(ctx, pending)
			}
		}
		return nil, nil
	}

	if err := txn.IncrStatementID(ctx, true); err != nil {
		return nil, err
	}

	if err := txn.transferTombstonesByCommit(ctx); err != nil {
		txn.Lock()
		undoErr := txn.restoreCompactionLocked()
		txn.Unlock()
		if undoErr != nil {
			return nil, errors.Join(err, undoErr)
		}
		return nil, err
	}
	txn.Lock()
	err = txn.finishCompactionLocked()
	if err != nil {
		if undoErr := txn.restoreCompactionLocked(); undoErr != nil {
			err = errors.Join(err, undoErr)
		}
		txn.Unlock()
		return nil, err
	}
	txn.compactNilWorkspaceSlotsLocked()
	txn.Unlock()

	// mergeTxnWorkspaceLocked (and the compactDeletionOnObjsLocked call
	// inside it) mutates txn.writes and may release/re-acquire the lock
	// around metadata resolution, so it must run with the lock held — same
	// as the IncrStatementID path.
	txn.Lock()
	err = txn.mergeTxnWorkspaceLocked(ctx)
	if err == nil {
		err = txn.dumpBatchLocked(ctx, -1)
	}
	if err == nil {
		err = txn.finishCompactionLocked()
	}
	if err != nil {
		if undoErr := txn.restoreCompactionLocked(); undoErr != nil {
			err = errors.Join(err, undoErr)
		}
		txn.compactionDeferredOrder = false
		txn.Unlock()
		return nil, err
	}
	if txn.compactionDeferredOrder {
		_ = txn.adjustUpdateOrderLocked(0)
		txn.compactionDeferredOrder = false
	}
	txn.compactNilWorkspaceSlotsLocked()
	txn.Unlock()

	if msg, injected := objectio.CNCommitAfterWorkspaceDumpFailedInjected(); injected {
		if msg == "" {
			msg = "injected commit failure after workspace dump"
		}
		return nil, moerr.NewInternalError(ctx, msg)
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
		if err := txn.checkDup(ctx); err != nil {
			return nil, err
		}
	}
	reqs, err = genWriteReqs(ctx, txn)
	if err != nil {
		return nil, err
	}

	return reqs, nil
}

// pendingCreatedDatabaseWrites returns the physical catalog inserts required
// by the transaction-local database view. It allocates only for transactions
// that have an active CREATE DATABASE operation.
func (txn *Transaction) pendingCreatedDatabaseWrites() map[databaseKey]uint64 {
	if txn.databaseOps == nil {
		return nil
	}

	txn.databaseOps.RLock()
	defer txn.databaseOps.RUnlock()

	var pending map[databaseKey]uint64
	for key, ops := range txn.databaseOps.names {
		if len(ops) == 0 || ops[len(ops)-1].kind != INSERT {
			continue
		}
		if pending == nil {
			pending = make(map[databaseKey]uint64)
		}
		pending[key] = ops[len(ops)-1].databaseId
	}
	return pending
}

// consumeCreatedDatabaseWrites reconciles active transaction-local CREATE
// DATABASE operations with the authoritative catalog tuples that will be sent
// to TN.  It intentionally derives identity from tuple data instead of Entry.note:
// notes are diagnostic metadata and must not be able to certify a wrong tenant,
// name, or allocated database ID.
func consumeCreatedDatabaseWrites(pending map[databaseKey]uint64, bat *batch.Batch) {
	if len(pending) == 0 || bat == nil || bat.RowCount() == 0 {
		return
	}

	findAttr := func(name string) int {
		for i := range bat.Attrs {
			if bat.Attrs[i] == name {
				return i
			}
		}
		return -1
	}
	idIdx := findAttr(catalog.SystemDBAttr_ID)
	nameIdx := findAttr(catalog.SystemDBAttr_Name)
	accountIdx := findAttr(catalog.SystemDBAttr_AccID)
	if idIdx < 0 || nameIdx < 0 || accountIdx < 0 ||
		idIdx >= len(bat.Vecs) || nameIdx >= len(bat.Vecs) || accountIdx >= len(bat.Vecs) ||
		bat.Vecs[idIdx] == nil || bat.Vecs[nameIdx] == nil || bat.Vecs[accountIdx] == nil {
		return
	}

	idVec, nameVec, accountVec := bat.Vecs[idIdx], bat.Vecs[nameIdx], bat.Vecs[accountIdx]
	rows := bat.RowCount()
	if idVec.Length() < rows || nameVec.Length() < rows || accountVec.Length() < rows {
		return
	}
	for row := range rows {
		if idVec.IsNull(uint64(row)) || nameVec.IsNull(uint64(row)) || accountVec.IsNull(uint64(row)) {
			continue
		}
		key := genDatabaseKey(
			vector.GetFixedAtNoTypeCheck[uint32](accountVec, row),
			string(nameVec.GetBytesAt(row)),
		)
		if expectedID, ok := pending[key]; ok &&
			expectedID == vector.GetFixedAtNoTypeCheck[uint64](idVec, row) {
			delete(pending, key)
		}
	}
}

func missingCreatedDatabaseWriteError(ctx context.Context, pending map[databaseKey]uint64) error {
	var key databaseKey
	var id uint64
	found := false
	for candidate, candidateID := range pending {
		if !found || candidate.accountId < key.accountId ||
			(candidate.accountId == key.accountId && candidate.name < key.name) {
			key, id, found = candidate, candidateID, true
		}
	}
	if !found {
		return nil
	}
	return moerr.NewInternalErrorf(
		ctx,
		"cannot commit CREATE DATABASE %q (account %d, id %d): catalog insert is missing or inconsistent",
		key.name,
		key.accountId,
		id,
	)
}

func (txn *Transaction) FinalizeCommit(context.Context) {
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnCommit(txn.op.Txn().ID)
	}
	txn.delTransaction()
}

func (txn *Transaction) FinalizeCommitWithUnknownResult(context.Context) {
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnUnknownResult(txn.op.Txn().ID)
	}
	if txn.compactionUndo != nil {
		logutil.Error("unknown commit has unresolved compaction publication")
		return
	}
	txn.releaseRetiredCompactionBuffersUnknownLocked()
	// The Commit may have reached TN. delTransaction releases only CN-local
	// state and intentionally does not run gcObjsByIdxRange or delete shared
	// object-storage data.
	txn.delTransaction()
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
	txn.Lock()
	undoErr := txn.restoreCompactionLocked()
	txn.compactionDeferredOrder = false
	txn.Unlock()
	if undoErr != nil {
		return undoErr
	}
	if !txn.ReadOnly() && len(txn.writes) > 0 {
		logutil.Info(
			"Transaction.Rollback",
			zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
		)
	}
	_, loadCleanupErr := txn.deleteLoadFiles(ctx, nil)

	// For CCPR transactions, call OnTxnRollback to clean up the cache and GC objects
	// Skip normal GCObjsByIdxRange for CCPR transactions because:
	// 1. CCPR objects may be shared across transactions
	// 2. CCPRTxnCache.OnTxnRollback handles GC properly (only deletes when no other txn references the object)
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnRollback(txn.op.Txn().ID)
	} else {
		//to gc the s3 objs
		if err := txn.gcObjsByIdxRangeToOwner(0, len(txn.writes)-1, cloneGCTxnRollback); err != nil {
			return errors.Join(loadCleanupErr, err)
		}
	}
	txn.delTransaction()
	return loadCleanupErr
}

func (txn *Transaction) delTransaction() {
	if txn.removed {
		return
	}
	if txn.compactionUndo != nil {
		logutil.Error("workspace terminal cleanup has unresolved compaction publication")
		return
	}
	if err := txn.retireCompactionBatchesLocked(); err != nil {
		logutil.Errorf("workspace compaction cleanup remains transaction-owned: %v", err)
		return
	}

	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	if err := txn.handoffPendingObjectCleanupLocked(true); err != nil {
		logutil.Errorf("workspace terminal cleanup remains transaction-owned: %v", err)
		return
	}
	txn.releaseRecordedUnpublishedObjectsLocked()
	if txn.isCloneTxn {
		txn.engine.cloneTxnCache.DeleteTxn(txn.op.Txn().ID)
		txn.isCloneTxn = false
	}

	for i := range txn.writes {
		if txn.writes[i].bat == nil {
			continue
		}
		txn.unaccountWorkspaceEntryLocked(&txn.writes[i])
		txn.writes[i].bat.Clean(txn.proc.Mp())
	}
	txn.assertWorkspaceAccountingLocked()

	txn.closeWorkspaceAllocation()
	if err := txn.releaseRemoteCoordinatorRecordsLocked(); err != nil {
		logutil.Errorf("remote workspace receipts remain transaction-owned: %v", err)
		return
	}
	txn.tableCache = nil
	txn.tableOps = nil
	txn.databaseOps = nil

	txn.cn_flushed_s3_tombstone_object_stats_list = nil
	txn.deletedBlocks = nil
	txn.haveDDL.Store(false)
	colexec.MustGetServer(txn.engine.service).DeleteTxnSegmentIds(txn.op.Txn().ID)
	txn.cnObjsSummary = nil
	txn.loadFiles = nil
	txn.hasS3Op.Store(false)
	txn.removed = true

	//txn.transfer.workerPool.Release()
	txn.transfer.statements = nil
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

// GetSnapshotWriteOffset returns the current statement boundary of
// txn.writes. It is lock-free on purpose: internal SQL spawned while the
// transaction lock is held (e.g. a catalog read inside a workspace dump)
// reads the boundary without re-entering the lock.
func (txn *Transaction) GetSnapshotWriteOffset() int {
	return int(txn.snapshotWriteOffset.Load())
}

// UpdateSnapshotWriteOffset advances the statement boundary to the current
// end of txn.writes. Only statement-boundary callers may use it (a new
// compile of a user statement); internal SQL must never advance the
// boundary, or a mid-statement dump can compact entries covered by it.
func (txn *Transaction) UpdateSnapshotWriteOffset() {
	txn.Lock()
	defer txn.Unlock()
	txn.snapshotWriteOffset.Store(int64(len(txn.writes)))
	txn.adjustWriteOffset = len(txn.writes)
}

// ApproximateInMemInsertSize returns the approximate total size of in-memory
// insert entries in the workspace. Intended for testing and diagnostics.
func (txn *Transaction) ApproximateInMemInsertSize() uint64 {
	txn.Lock()
	defer txn.Unlock()
	return txn.approximateInMemInsertSize
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
