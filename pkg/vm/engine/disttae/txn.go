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
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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

func (txn *Transaction) appendWorkspaceEntryLocked(entry Entry) workspaceMutationID {
	return txn.workspace.append(entry)
}

func (txn *Transaction) removeDroppedTableMutationsLocked() error {
	droppedTables, entries, err := txn.workspace.droppedTableEntries()
	if droppedTables.empty() {
		return nil
	}
	if err != nil {
		return err
	}
	defer entries.Close()

	sources := make([]workspaceMutationTransitionSource, 0)
	gcEntries := make([]workspaceEntryView, 0)
	stats := make([]objectio.ObjectStats, 0)
	for idx := range entries.entries {
		entry := entries.entries[idx]
		if !droppedTables.containsEntry(entry.Entry) {
			continue
		}
		sources = append(sources, workspaceMutationTransitionSource{
			mutationID: entry.workspaceMutationID,
			oldBat:     entry.bat,
			selections: entry.selections,
		})
		if entry.fileName != "" && !txn.isCCPRTxn {
			gcEntries = append(gcEntries, entry)
			stats = append(stats, collectObjectStatsFromEntry(entry.Entry)...)
		}
	}
	if len(sources) == 0 {
		return nil
	}
	if _, err = txn.workspace.transitionMutations(sources, nil); err != nil {
		return err
	}
	if !txn.isCCPRTxn {
		ignoreDroppedTables := func(entry Entry) bool {
			return droppedTables.containsEntry(entry)
		}
		if err = txn.unprotectUnreferencedTxnLocalSharedFilesLocked(
			stats,
			ignoreDroppedTables,
		); err != nil {
			return err
		}
		_ = txn.gcWorkspaceEntries(gcEntries, cloneGCIntermediate)
	}
	return nil
}

type workspaceCompactionKey struct {
	typ         int
	accountID   uint32
	databaseID  uint64
	tableID     uint64
	statementID uint64
	attemptID   uint64
}

func (txn *Transaction) compactWorkspaceMemoryBatchesLocked(ctx context.Context) error {
	entries, err := txn.workspace.compactionCandidateEntries()
	if err != nil {
		return err
	}
	defer entries.Close()

	insertCandidates := make([]int, 0)
	deleteCandidates := make([]int, 0)
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		if entry.bat == nil || entry.bat.IsEmpty() || len(entry.selections) != 0 ||
			len(entry.bat.Attrs) == 0 ||
			entry.bat.Attrs[0] == catalog.BlockMeta_BlockInfo ||
			entry.bat.Attrs[0] == catalog.ObjectMeta_ObjectStats ||
			entry.databaseId == catalog.MO_CATALOG_ID ||
			entry.bat.RowCount() >= objectio.BlockMaxRows/2 {
			continue
		}
		switch entry.typ {
		case INSERT:
			insertCandidates = append(insertCandidates, idx)
		case DELETE:
			deleteCandidates = append(deleteCandidates, idx)
		}
	}
	if len(insertCandidates)+len(deleteCandidates) < 30 {
		return nil
	}

	compactions := make([]workspaceMutationCompaction, 0)
	planned := make(map[workspaceMutationID]struct{})
	cleanup := func() {
		for _, compaction := range compactions {
			compaction.dstNewBat.Clean(txn.proc.Mp())
		}
	}
	buildMerges := func(candidates []int) error {
		consumed := make(map[int]struct{}, len(candidates))
		for pos, entryIdx := range candidates {
			if _, ok := consumed[entryIdx]; ok {
				continue
			}
			dst := &entries.entries[entryIdx]
			key := workspaceCompactionKey{
				typ:         dst.typ,
				accountID:   dst.accountId,
				databaseID:  dst.databaseId,
				tableID:     dst.tableId,
				statementID: dst.statementID,
				attemptID:   dst.attemptID,
			}
			sources := make([]int, 0)
			rows := dst.bat.RowCount()
			for _, sourceIdx := range candidates[pos+1:] {
				if _, ok := consumed[sourceIdx]; ok {
					continue
				}
				source := &entries.entries[sourceIdx]
				sourceKey := workspaceCompactionKey{
					typ:         source.typ,
					accountID:   source.accountId,
					databaseID:  source.databaseId,
					tableID:     source.tableId,
					statementID: source.statementID,
					attemptID:   source.attemptID,
				}
				if sourceKey != key || rows+source.bat.RowCount() > objectio.BlockMaxRows {
					continue
				}
				sources = append(sources, sourceIdx)
				consumed[sourceIdx] = struct{}{}
				rows += source.bat.RowCount()
				if rows == objectio.BlockMaxRows {
					break
				}
			}
			if len(sources) == 0 {
				continue
			}

			newBat, dupErr := dst.bat.Dup(txn.proc.Mp())
			if dupErr != nil {
				return dupErr
			}
			compaction := workspaceMutationCompaction{
				dstMutationID:  dst.workspaceMutationID,
				dstOldBat:      dst.bat,
				dstNewBat:      newBat,
				srcMutationIDs: make([]workspaceMutationID, 0, len(sources)),
				srcOldBats:     make([]*batch.Batch, 0, len(sources)),
			}
			for _, sourceIdx := range sources {
				source := &entries.entries[sourceIdx]
				if _, appendErr := newBat.Append(ctx, txn.proc.Mp(), source.bat); appendErr != nil {
					newBat.Clean(txn.proc.Mp())
					return appendErr
				}
				compaction.srcMutationIDs = append(
					compaction.srcMutationIDs, source.workspaceMutationID)
				compaction.srcOldBats = append(compaction.srcOldBats, source.bat)
				planned[source.workspaceMutationID] = struct{}{}
			}
			if dst.typ == INSERT {
				rowIDVector, allocErr := txn.batchAllocNewRowIds(newBat.RowCount())
				if allocErr != nil {
					if rowIDVector != nil {
						rowIDVector.Free(txn.proc.Mp())
					}
					newBat.Clean(txn.proc.Mp())
					return allocErr
				}
				rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](rowIDVector)
				for row := range newBat.RowCount() {
					if setErr := vector.SetFixedAtWithTypeCheck[objectio.Rowid](
						newBat.Vecs[0], row, rowIDs[row]); setErr != nil {
						rowIDVector.Free(txn.proc.Mp())
						newBat.Clean(txn.proc.Mp())
						return setErr
					}
				}
				rowIDVector.Free(txn.proc.Mp())
			} else if newBat.RowCount() > 1 {
				var sortBuf []int64
				var shuffleBuf []byte
				if sortErr := mergeutil.SortColumnsByIndexWithBuf(
					newBat.Vecs, 0, txn.proc.Mp(), &sortBuf, &shuffleBuf); sortErr != nil {
					newBat.Clean(txn.proc.Mp())
					return sortErr
				}
			}
			planned[dst.workspaceMutationID] = struct{}{}
			compactions = append(compactions, compaction)
		}
		return nil
	}
	if err = buildMerges(insertCandidates); err != nil {
		cleanup()
		return err
	}
	if err = buildMerges(deleteCandidates); err != nil {
		cleanup()
		return err
	}

	// Preserve the existing contract that once small-batch compaction is
	// triggered, every remaining DELETE batch is ordered by row-id. A
	// destination-only compaction is a copy-on-write sort at the same revision.
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		if entry.typ != DELETE || entry.bat == nil || len(entry.selections) != 0 ||
			entry.bat.RowCount() <= 1 {
			continue
		}
		if _, ok := planned[entry.workspaceMutationID]; ok {
			continue
		}
		newBat, dupErr := entry.bat.Dup(txn.proc.Mp())
		if dupErr != nil {
			cleanup()
			return dupErr
		}
		var sortBuf []int64
		var shuffleBuf []byte
		if sortErr := mergeutil.SortColumnsByIndexWithBuf(
			newBat.Vecs, 0, txn.proc.Mp(), &sortBuf, &shuffleBuf); sortErr != nil {
			newBat.Clean(txn.proc.Mp())
			cleanup()
			return sortErr
		}
		compactions = append(compactions, workspaceMutationCompaction{
			dstMutationID: entry.workspaceMutationID,
			dstOldBat:     entry.bat,
			dstNewBat:     newBat,
		})
	}

	if err = txn.workspace.compactMemoryMany(compactions); err != nil {
		cleanup()
		return err
	}
	return nil
}

func (txn *Transaction) checkWorkspaceAccountingLocked() error {
	return txn.workspace.validateUsage()
}

func (txn *Transaction) assertWorkspaceAccountingLocked() {
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

	var pkCheck workspacePKCheck
	if typ == INSERT || typ == DELETE {
		// resolvePKCheckForWrite may reach Engine.Database, which can craft an
		// internal SQL on the current txn and reenter txn.Lock while capturing its
		// read view. Resolve the PK position before taking txn.Lock.
		pkCheck, err = txn.resolvePKCheckForWrite(
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
		if typ == INSERT && pkCheck.enabled {
			// WriteBatch prepends rowid at attr 0 for inserts after this metadata lookup.
			pkCheck.vectorPos++
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

	}

	if injected, logLevel := objectio.LogWorkspaceInjected(
		databaseName, tableName,
	); injected {
		activeMutations := txn.workspace.activeMutationCount()
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
				zap.Int("active-mutations", activeMutations),
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
				zap.Int("active-mutations", activeMutations),
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
		pkCheck:            pkCheck,
	}
	txn.appendWorkspaceEntryLocked(e)
	txn.pkCount += bat.RowCount()

	trace.GetService(txn.proc.GetService()).TxnWrite(txn.op, tableId, typesNames[typ], bat)
	return
}

func (txn *Transaction) dumpBatch(ctx context.Context, scope workspaceDumpScope) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.dumpBatchLocked(ctx, scope)
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

// checkDup checks duplicate primary keys against one immutable workspace view.
// Payload generations remain pinned for the whole validation, so concurrent
// physical rewrites cannot mix old and new batches in one duplicate check.
func (txn *Transaction) checkDup(ctx context.Context) error {
	start := time.Now()
	defer func() {
		v2.TxnCheckPKDupDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	insertPks := make(map[uint64]map[any]bool)
	delPks := make(map[uint64]map[any]bool)

	entrySet, err := txn.workspace.pkCandidateEntries(
		txn.workspace.currentReadView())
	if err != nil {
		return err
	}
	defer entrySet.Close()

	for idx := range entrySet.entries {
		e := entrySet.entries[idx].Entry
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
		if txn.workspace.databaseDeleted(dbkey) {
			continue
		}

		tableKey := genTableKey(e.accountId, e.tableName, e.databaseId, e.databaseName)
		if txn.workspace.tableDeleted(tableKey) {
			continue
		}

		if !e.pkCheck.enabled {
			return moerr.NewInternalErrorNoCtxf(
				"workspace PK candidate has no descriptor for table %s.%s",
				e.databaseName, e.tableName)
		}
		index := e.pkCheck.vectorPos
		if index < 0 || index >= len(e.bat.Vecs) || index >= len(e.bat.Attrs) {
			return moerr.NewInternalErrorNoCtxf(
				"workspace PK descriptor out of range for table %s.%s: position %d, columns %d",
				e.databaseName, e.tableName, index, len(e.bat.Vecs))
		}

		var pks map[any]bool
		switch e.typ {
		case INSERT:
			if insertPks[e.tableId] == nil {
				insertPks[e.tableId] = make(map[any]bool)
			}
			pks = insertPks[e.tableId]
		case DELETE:
			if delPks[e.tableId] == nil {
				delPks[e.tableId] = make(map[any]bool)
			}
			pks = delPks[e.tableId]
		default:
			return moerr.NewInternalErrorNoCtxf(
				"workspace PK descriptor attached to unsupported mutation type %d", e.typ)
		}
		if dup, pk := checkPKDup(pks, e.bat.Vecs[index], 0, e.bat.RowCount()); dup {
			logutil.Errorf("txn:%s has duplicate primary key:%s in table:[%v-%v:%s-%s], mutation:%s",
				hex.EncodeToString(txn.op.Txn().ID),
				pk,
				e.databaseId,
				e.tableId,
				e.databaseName,
				e.tableName,
				typesNames[e.typ])
			return moerr.NewDuplicateEntryNoCtx(pk, e.bat.Attrs[index])
		}
	}
	return nil
}

// scanInMemInsertSize sums the in-memory INSERT payloads selected by one
// logical workspace scope. Selection is based on stable mutation identity,
// never on a physical mutation position.
func (txn *Transaction) scanInMemInsertSize(scope workspaceDumpScope) (uint64, error) {
	entries, err := txn.workspace.entriesForDumpScope(scope)
	if err != nil {
		return 0, err
	}
	defer entries.Close()

	var size uint64
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		if entry.isCatalog() {
			continue
		}
		if entry.bat == nil || entry.bat.RowCount() == 0 {
			continue
		}
		if entry.typ == INSERT && entry.fileName == "" {
			size += uint64(entry.bat.Size())
		}
	}
	return size, nil
}

// dumpBatchLocked spills mutations selected by scope when the configured
// workspace thresholds require it. A commit scope uses the commit thresholds
// and may spill tombstones; an all-but-non-commit scope is used at statement
// finalization and retains the normal write threshold semantics.
func (txn *Transaction) dumpBatchLocked(ctx context.Context, scope workspaceDumpScope) error {
	var size uint64
	var pkCount int
	usage := txn.workspace.usageSnapshot()

	// Check fault injection first - if enabled, force flush
	forceFlush := objectio.CNWorkspaceForceFlushInjected()

	if scope.commit {
		if !forceFlush && usage.inMemoryInsertBytes < txn.commitWorkspaceThreshold &&
			usage.inMemoryInsertRows < txn.engine.config.insertEntryMaxCount &&
			usage.inMemoryDeleteRows < txn.engine.config.insertEntryMaxCount {
			return nil
		}
	} else {
		if !forceFlush && usage.inMemoryInsertBytes < txn.writeWorkspaceThreshold {
			return nil
		}
	}

	if !scope.commit && !forceFlush {
		forceDump := false
		var err error
		size, err = txn.scanInMemInsertSize(scope)
		if err != nil {
			return err
		}
		if size < txn.writeWorkspaceThreshold {
			// IncrStatementID can be disabled during internal RunSql work. The
			// StatementJournal still owns the complete current attempt, so the same
			// logical scope is the safety-valve scope as well.
			if usage.inMemoryInsertBytes >= txn.engine.config.extraWorkspaceThreshold {
				logutil.Info(
					"WORKSPACE-FORCE-DUMP",
					zap.Uint64("approximateInMemInsertSize", usage.inMemoryInsertBytes),
					zap.Uint64("extraWorkspaceThreshold", txn.engine.config.extraWorkspaceThreshold),
					zap.String("txn", txn.op.Txn().DebugString()),
				)
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
	var (
		err error
		fs  fileservice.FileService
	)

	if fs, err = colexec.GetSharedFSFromProc(txn.proc); err != nil {
		return err
	}

	if err := txn.dumpInsertBatchLocked(ctx, fs, scope, &pkCount); err != nil {
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

	if scope.commit {
		usage = txn.workspace.usageSnapshot()
		if usage.inMemoryDeleteRows >= txn.engine.config.insertEntryMaxCount {
			if err := txn.dumpDeleteBatchLocked(ctx, fs, scope); err != nil {
				return err
			}
		}
		txn.pkCount -= pkCount
	} else {
		txn.pkCount -= pkCount
	}
	txn.assertWorkspaceAccountingLocked()
	return nil
}

func (txn *Transaction) dumpInsertBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	scope workspaceDumpScope,
	pkCount *int,
) error {
	// Preserve the existing flush-selection policy. The spill protocol below
	// changes only ownership and publication: selected memory mutations remain
	// visible until their replacement objects are atomically published.
	forceFlush := objectio.CNWorkspaceForceFlushInjected()
	entries, err := txn.workspace.entriesForDumpScope(scope)
	if err != nil {
		return err
	}
	tbSize := make(map[workspaceOverlayKey]int)
	tbCount := make(map[workspaceOverlayKey]int)
	skipTable := make(map[workspaceOverlayKey]bool)
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		if entry.isCatalog() || entry.bat == nil || entry.bat.RowCount() == 0 {
			continue
		}
		if entry.typ == INSERT && entry.fileName == "" {
			key := workspaceOverlayKey{
				accountID:  entry.accountId,
				databaseID: entry.databaseId,
				tableID:    entry.tableId,
			}
			tbSize[key] += entry.bat.Size()
			tbCount[key] += entry.bat.RowCount()
		}
	}
	entries.Close()
	keys := make([]workspaceOverlayKey, 0, len(tbSize))
	for k := range tbSize {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b workspaceOverlayKey) int {
		if bySize := cmp.Compare(tbSize[a], tbSize[b]); bySize != 0 {
			return bySize
		}
		if byAccount := cmp.Compare(a.accountID, b.accountID); byAccount != 0 {
			return byAccount
		}
		if byDatabase := cmp.Compare(a.databaseID, b.databaseID); byDatabase != 0 {
			return byDatabase
		}
		return cmp.Compare(a.tableID, b.tableID)
	})

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
	rows, err := txn.dumpWorkspaceMutationsLocked(ctx, fs, scope, INSERT, skipTable)
	*pkCount += rows
	return err
}

func (txn *Transaction) dumpDeleteBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	scope workspaceDumpScope,
) error {
	_, err := txn.dumpWorkspaceMutationsLocked(ctx, fs, scope, DELETE, nil)
	return err
}

// resolveDumpTablesLocked resolves relations for the exact immutable sources
// captured by a spill attempt. getTable may run internal SQL, so txn.Lock is
// released around resolution; the caller validates the attempt after this
// method returns before it starts remote object IO.
func (txn *Transaction) resolveDumpTablesLocked(
	ctx context.Context,
	attempt *workspaceSpillAttempt,
) (tables map[tableKey]engine.Relation, err error) {
	var keys []tableKey
	seen := make(map[tableKey]bool)
	for _, source := range attempt.sources {
		e := &source.entry
		k := tableKey{
			accountId:  e.accountId,
			databaseId: e.databaseId,
			dbName:     e.databaseName,
			name:       e.tableName,
		}
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}

	tables = make(map[tableKey]engine.Relation, len(keys))
	if len(keys) == 0 {
		return tables, nil
	}

	txn.Unlock()
	for _, k := range keys {
		tbl, terr := txn.getTable(ctx, k.accountId, k.dbName, k.name)
		if terr != nil {
			txn.Lock()
			return nil, terr
		}
		tables[k] = tbl
	}
	// Test-only orchestration: park here (lock released) so a test can mutate
	// the workspace while the resolution window is open.
	objectio.CNDumpResolveWindowWait()
	txn.Lock()
	return tables, nil
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

	if injected, rogueUpdate, errorOut := objectio.CNReenterWorkspaceReadViewOnGetTableInjected(); injected {
		// Test-only fault: deterministically simulate the internal-SQL leg
		// of the issue #25557 deadlock (getTable -> Engine.Database ->
		// loadDatabaseFromStorage -> execReadSql -> NewCompile) without
		// requiring a catalog cache miss. The internal SQL's compile captures
		// the workspace visibility boundary. Read views are owned by the
		// workspace and do not re-enter txn.Lock.
		_ = txn.CurrentReadView()
		if rogueUpdate {
			// simulate a rogue statement-boundary advance that internal SQL
			// must never perform; kept to pin down the damage it would cause
			txn.PublishReadView()
		}
		if errorOut {
			return nil, moerr.NewInternalErrorNoCtx(
				"fault injection: reenter workspace read view on getTable")
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

func (txn *Transaction) resolvePKCheckForWrite(
	ctx context.Context,
	typ int,
	accountId uint32,
	databaseName, tableName string,
	tableId uint64,
	bat *batch.Batch,
) (workspacePKCheck, error) {
	if bat == nil || bat.RowCount() == 0 {
		return workspacePKCheck{}, nil
	}

	if typ != INSERT && typ != DELETE {
		return workspacePKCheck{}, nil
	}

	if tableId == catalog.MO_TABLES_ID ||
		tableId == catalog.MO_COLUMNS_ID ||
		tableId == catalog.MO_DATABASE_ID {
		return workspacePKCheck{}, nil
	}
	if txn.engine == nil {
		return workspacePKCheck{}, moerr.NewInternalErrorNoCtx(
			"cannot resolve workspace PK descriptor without transaction engine")
	}

	tbl, err := txn.getTable(ctx, accountId, databaseName, tableName)
	if err != nil {
		return workspacePKCheck{}, err
	}
	tableDef := tbl.GetTableDef(defines.AttachAccountId(ctx, accountId))
	if tableDef == nil {
		return workspacePKCheck{}, moerr.NewInternalErrorNoCtxf(
			"cannot resolve workspace PK descriptor: table definition is nil for %s.%s",
			databaseName, tableName)
	}
	if tableDef.Pkey == nil {
		return workspacePKCheck{}, nil
	}

	pkName := tableDef.Pkey.PkeyColName
	if pkName == "" ||
		pkName == catalog.FakePrimaryKeyColName ||
		pkName == catalog.CPrimaryKeyColName {
		return workspacePKCheck{}, nil
	}

	if typ == DELETE {
		if len(bat.Vecs) < 2 {
			logutil.Warnf("delete has no pk vector, database:%s, table:%s", databaseName, tableName)
			return workspacePKCheck{}, moerr.NewInternalErrorNoCtxf(
				"delete batch for primary-key table %s.%s has no primary-key vector",
				databaseName, tableName)
		}
		return workspacePKCheck{vectorPos: 1, enabled: true}, nil
	}

	for i, attr := range bat.Attrs {
		if attr == pkName {
			return workspacePKCheck{vectorPos: i, enabled: true}, nil
		}
	}
	for i, attr := range bat.Attrs {
		if strings.EqualFold(attr, pkName) {
			return workspacePKCheck{vectorPos: i, enabled: true}, nil
		}
	}

	return workspacePKCheck{}, moerr.NewInternalErrorNoCtxf(
		"primary-key column %s not found in write batch for %s.%s: attrs %v",
		pkName, databaseName, tableName, bat.Attrs)
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
	if err := txn.requireAutoIncrEpochFenceCommit(autoIncrEpoch, autoIncrEpochKnown); err != nil {
		return err
	}

	txn.hasS3Op.Store(true)

	var (
		copied *batch.Batch
	)

	if copied, err = inputBat.Dup(txn.proc.Mp()); err != nil {
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
		}
	}

	txn.readOnly.Store(false)

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
		autoIncrEpoch:      autoIncrEpoch,
		autoIncrEpochKnown: autoIncrEpochKnown,
	}

	txn.appendWorkspaceEntryLocked(entry)

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

	if copied, err = inputBat.Dup(txn.proc.Mp()); err != nil {
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
		}
	}

	txn.readOnly.Store(false)

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
	accountID uint32,
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
			txn.workspace.appendObjectDelete(
				accountID,
				databaseId,
				tableId,
				blkid,
				[]int64{int64(rowOffset)},
			)
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
	txn.deleteTableWrites(
		accountID,
		databaseId,
		tableId,
		sels,
		deleteBlkId,
		min1,
		max1,
		mp,
	)

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

func (txn *Transaction) addWorkspaceEntrySelectionsLocked(entry *Entry, sels []int64) {
	if err := txn.workspace.addMutationSelections(entry.workspaceMutationID, sels); err != nil {
		panic(err)
	}
}

func (txn *Transaction) selectAllWorkspaceEntryRowsLocked(entry *Entry) {
	if err := txn.workspace.selectAllMutationRows(
		entry.workspaceMutationID,
		entry.bat.RowCount(),
	); err != nil {
		panic(err)
	}
}

// Delete rows belongs to uncommitted raw data batch in txn's workspace.
func (txn *Transaction) deleteTableWrites(
	accountID uint32,
	databaseId uint64,
	tableId uint64,
	sels []int64,
	deleteBlkId map[types.Blockid]bool,
	min, max uint32,
	mp map[types.Rowid]uint8,
) {
	txn.Lock()
	defer txn.Unlock()
	entries, err := txn.workspace.tableEntries(
		txn.workspace.currentReadView(),
		accountID,
		databaseId,
		tableId,
	)
	if err != nil {
		panic(err)
	}
	defer entries.Close()

	// txn worksapce will have four batch type:
	// 1.RawBatch 2.DN Block RowId(mixed rowid from different block)
	// 3.CN block Meta batch(record block meta generated by cn insert write s3)
	// 4.DN delete Block Meta batch(record block meta generated by cn delete write s3)
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		if entry.typ == ALTER || entry.typ == DELETE {
			continue
		}
		// nil batch will generated by comapction or dumpBatch
		if entry.visibleRowCount() == 0 {
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
			if err = txn.workspace.addMutationSelections(entry.workspaceMutationID, sels); err != nil {
				panic(err)
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
	if err := txn.removeDroppedTableMutationsLocked(); err != nil {
		return err
	}
	if !txn.workspace.droppedTablesSnapshot().empty() {
		txn.assertWorkspaceAccountingLocked()
	}

	if err := txn.compactDeletionOnObjsLocked(ctx); err != nil {
		return err
	}

	if err := txn.compactWorkspaceMemoryBatchesLocked(ctx); err != nil {
		return err
	}

	txn.assertWorkspaceAccountingLocked()
	return nil
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
// referenced by the workspace pending-object-delete snapshot is resolved.
func (txn *Transaction) resolveCompactTablesLocked(
	ctx context.Context,
	snapshot workspaceObjectDeleteSnapshot,
) (map[tableKey]engine.Relation, error) {
	tables := make(map[tableKey]engine.Relation)
	missing := make([]tableKey, 0)
	seen := make(map[tableKey]struct{})
	for _, metadata := range snapshot.objects {
		key := tableKey{
			accountId:  metadata.accountID,
			databaseId: metadata.databaseID,
			dbName:     metadata.databaseName,
			name:       metadata.tableName,
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		missing = append(missing, key)
	}

	txn.Unlock()
	for _, key := range missing {
		table, err := txn.getTable(ctx, key.accountId, key.dbName, key.name)
		if err != nil {
			txn.Lock()
			return nil, err
		}
		tables[key] = table
	}
	txn.Lock()
	return tables, nil
}

type stagedObjectCompaction struct {
	sources   []workspaceMutationTransitionSource
	targets   []workspaceMutationTransitionTarget
	dirtyOld  []objectio.ObjectStats
	dirtyNew  []objectio.ObjectStats
	ownedBats []*batch.Batch
}

func (staged *stagedObjectCompaction) cleanMetadata(mp *mpool.MPool) {
	for _, bat := range staged.ownedBats {
		if bat != nil {
			bat.Clean(mp)
		}
	}
	staged.ownedBats = nil
}

func workspaceObjectEntryFrom(source Entry, fileName string, bat *batch.Batch) Entry {
	source.workspaceMutationID = 0
	source.fileName = fileName
	source.bat = bat
	source.pkCheck = workspacePKCheck{}
	return source
}

type dirtyObjectCompaction struct {
	stats          objectio.ObjectStats
	table          *txnTable
	key            workspaceTableKey
	sourceMutation workspaceMutationID
}

// compactDeletionOnObjsLocked prepares every replacement object without
// changing logical visibility, then publishes the complete source-to-target
// transition at one workspace revision while consuming exactly the deletion
// intent that it compacted. On any preparation or publication error the old
// mutations and pending deletes remain visible and retryable.
func (txn *Transaction) compactDeletionOnObjsLocked(ctx context.Context) (err error) {
	snapshot, err := txn.workspace.snapshotObjectDeleteCompaction()
	if err != nil {
		return err
	}
	if len(snapshot.ids) == 0 {
		return nil
	}
	tables, err := txn.resolveCompactTablesLocked(ctx, snapshot)
	if err != nil {
		return err
	}

	objBlkDeletion := make(map[objectio.ObjectId]map[objectio.Blockid][]int64)
	objTables := make(map[objectio.ObjectId]workspaceTableKey)
	for blockID, offsets := range snapshot.blocks {
		objectID := *blockID.Object()
		metadata, ok := snapshot.objects[objectID]
		if !ok {
			return moerr.NewInternalErrorNoCtx(
				"workspace object metadata not found during deletion compaction")
		}
		if objBlkDeletion[objectID] == nil {
			objBlkDeletion[objectID] = make(map[objectio.Blockid][]int64)
		}
		objBlkDeletion[objectID][blockID] = offsets
		objTables[objectID] = workspaceTableKey{
			tableKey: tableKey{
				accountId:  metadata.accountID,
				databaseId: metadata.databaseID,
				dbName:     metadata.databaseName,
				name:       metadata.tableName,
			},
			autoIncrEpoch:      metadata.autoIncrEpoch,
			autoIncrEpochKnown: metadata.autoIncrEpochKnown,
		}
	}

	entries, err := txn.workspace.blockMetaEntries()
	if err != nil {
		return err
	}
	defer entries.Close()

	staged := stagedObjectCompaction{}
	published := false
	defer func() {
		if published {
			return
		}
		staged.cleanMetadata(txn.proc.Mp())
		if len(staged.dirtyNew) != 0 {
			txn.Unlock()
			_ = txn.GCObjsByStats(staged.dirtyNew...)
			txn.Lock()
		}
	}()

	dirty := make([]dirtyObjectCompaction, 0, 1)
	for idx := range entries.entries {
		entry := entries.entries[idx]
		if entry.bat == nil || entry.bat.IsEmpty() || entry.typ != INSERT ||
			len(entry.bat.Attrs) == 0 || entry.bat.Attrs[0] != catalog.BlockMeta_BlockInfo {
			continue
		}
		col, area := vector.MustVarlenaRawData(entry.bat.Vecs[1])
		affected := false
		for row := range col {
			stats := objectio.ObjectStats(col[row].GetByteSlice(area))
			if objBlkDeletion[*stats.ObjectName().ObjectId()] != nil {
				affected = true
				break
			}
		}
		if !affected {
			injected, _ := objectio.LogWorkspaceInjected(entry.databaseName, entry.tableName)
			if !injected {
				continue
			}
		}
		if err = txn.requireAutoIncrEpochFenceCommit(
			entry.autoIncrEpoch, entry.autoIncrEpochKnown); err != nil {
			return err
		}
		staged.sources = append(staged.sources, workspaceMutationTransitionSource{
			mutationID: entry.workspaceMutationID,
			oldBat:     entry.bat,
			selections: entry.selections,
		})

		offset := 0
		for row := range col {
			stats := objectio.ObjectStats(col[row].GetByteSlice(area))
			objectID := *stats.ObjectName().ObjectId()
			if objBlkDeletion[objectID] == nil {
				bat := colexec.AllocCNS3ResultBat(false)
				staged.ownedBats = append(staged.ownedBats, bat)
				if err = bat.Vecs[0].UnionBatch(
					entry.bat.Vecs[0], int64(offset), int(stats.BlkCnt()), nil, txn.proc.Mp()); err != nil {
					return err
				}
				if err = vector.AppendBytes(
					bat.Vecs[1], stats.Marshal(), false, txn.proc.Mp()); err != nil {
					return err
				}
				bat.SetRowCount(bat.Vecs[0].Length())
				staged.targets = append(staged.targets,
					workspaceMutationTransitionTarget{
						entry: workspaceObjectEntryFrom(
							entry.Entry, stats.ObjectName().String(), bat),
						replacementOf: entry.workspaceMutationID,
					})
			} else {
				key, ok := objTables[objectID]
				if !ok {
					return moerr.NewInternalErrorNoCtx(
						"object table not found during deletion compaction")
				}
				relation, ok := tables[key.tableKey]
				if !ok {
					return moerr.NewInternalErrorNoCtx(
						"table not pre-resolved for object compaction")
				}
				table, ok := relation.(*txnTable)
				if !ok {
					delegate, delegateOK := relation.(*txnTableDelegate)
					if !delegateOK {
						return moerr.NewInternalErrorNoCtx(
							"unsupported table relation during object compaction")
					}
					table = delegate.origin
				}
				dirty = append(dirty, dirtyObjectCompaction{
					stats:          stats,
					table:          table,
					key:            key,
					sourceMutation: entry.workspaceMutationID,
				})
			}
			offset += int(stats.BlkCnt())
		}
	}

	for idx := range dirty {
		dirty[idx].table.ensureSeqnumsAndTypesExpectRowid()
	}
	type rewriteResult struct {
		entry Entry
		stats []objectio.ObjectStats
		err   error
	}
	results := make([]rewriteResult, len(dirty))
	limit := make(chan struct{}, min(runtime.NumCPU(), 4))
	var waiter sync.WaitGroup
	for idx := range dirty {
		idx := idx
		waiter.Add(1)
		go func() {
			defer waiter.Done()
			limit <- struct{}{}
			defer func() { <-limit }()
			item := dirty[idx]
			objectID := *item.stats.ObjectName().ObjectId()
			bat, fileName, rewriteErr := item.table.rewriteObjectByDeletion(
				ctx, item.stats, objBlkDeletion[objectID])
			if rewriteErr != nil {
				results[idx].err = rewriteErr
				return
			}
			entry := Entry{
				typ:                INSERT,
				accountId:          item.table.accountId,
				databaseId:         item.table.db.databaseId,
				tableId:            item.table.tableId,
				databaseName:       item.table.db.databaseName,
				tableName:          item.table.tableName,
				fileName:           fileName,
				bat:                bat,
				tnStore:            txn.tnStores[0],
				autoIncrEpoch:      item.key.autoIncrEpoch,
				autoIncrEpochKnown: item.key.autoIncrEpochKnown,
			}
			results[idx] = rewriteResult{
				entry: entry,
				stats: collectObjectStatsFromEntry(entry),
			}
		}()
	}
	waiter.Wait()
	for idx := range results {
		if results[idx].entry.bat != nil {
			staged.ownedBats = append(staged.ownedBats, results[idx].entry.bat)
			staged.dirtyNew = append(staged.dirtyNew, results[idx].stats...)
		}
		if results[idx].err != nil {
			return results[idx].err
		}
		staged.targets = append(staged.targets,
			workspaceMutationTransitionTarget{
				entry:         results[idx].entry,
				replacementOf: dirty[idx].sourceMutation,
			})
		staged.dirtyOld = append(staged.dirtyOld, dirty[idx].stats)
	}

	if len(staged.sources) == 0 {
		return moerr.NewInternalErrorNoCtx(
			"deleted CN object has no active workspace mutation")
	}
	_, err = txn.workspace.transitionMutationsAndConsumeObjectDeletes(
		staged.sources,
		staged.targets,
		snapshot,
	)
	if err != nil {
		return err
	}
	published = true
	for idx := range staged.targets {
		txn.publishSpilledObjectLocked(&staged.targets[idx].entry)
	}
	txn.hasS3Op.Store(true)
	txn.readOnly.Store(false)
	staged.ownedBats = nil // ownership moved to the workspace

	if err = txn.unprotectUnreferencedTxnLocalSharedFilesLocked(staged.dirtyOld, nil); err != nil {
		logutil.Warn("failed to release clone protection after object compaction",
			zap.String("txn", txn.op.Txn().DebugString()), zap.Error(err))
	}
	_ = txn.GCObjsByStats(staged.dirtyOld...)
	return nil
}

func (txn *Transaction) unprotectUnreferencedTxnLocalSharedFilesLocked(
	statsList []objectio.ObjectStats,
	ignoreEntry func(Entry) bool,
) error {
	if !txn.isCloneTxn ||
		txn.op == nil ||
		txn.engine == nil ||
		txn.engine.cloneTxnCache == nil {
		return nil
	}
	txnID := txn.op.Txn().ID
	names := make([]string, len(statsList))
	for i := range statsList {
		names[i] = statsList[i].ObjectName().String()
	}
	liveNames := txn.workspace.liveObjectReferences(names, ignoreEntry)
	for i := range statsList {
		name := statsList[i].ObjectName().String()
		if _, live := liveNames[name]; live {
			continue
		}
		txn.engine.cloneTxnCache.RemoveTxnLocalSharedFile(txnID, name)
	}
	return nil
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

func (txn *Transaction) forEachTableHasDeletesLocked(
	isObject bool,
	f func(tbl *txnTable) error) error {
	candidates, err := txn.workspace.deleteTableCandidates(isObject)
	if err != nil {
		return err
	}

	tables := make(map[uint64]*txnTable, len(candidates))
	for _, entry := range candidates {
		ctx := context.WithValue(txn.proc.Ctx, defines.TenantIDKey{}, entry.accountId)
		// Database might craft a sql on the current txn to get the table,
		// so we need to unlock the txn
		txn.Unlock()
		db, err := txn.engine.Database(ctx, entry.databaseName, txn.op)
		if err != nil {
			txn.Lock()
			return err
		}
		rel, err := db.Relation(ctx, entry.tableName, nil)
		if err != nil {
			txn.Lock()
			return err
		}
		txn.Lock()
		if v, ok := rel.(*txnTableDelegate); ok {
			tables[entry.tableId] = v.origin
		} else {
			tables[entry.tableId] = rel.(*txnTable)
		}
	}
	for _, tbl := range tables {
		if err := f(tbl); err != nil {
			return err
		}
	}
	return nil
}

func (txn *Transaction) ForEachTableMutation(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
	f func(workspaceEntryView),
) error {
	entries, err := txn.workspace.tableEntries(view, accountID, databaseID, tableID)
	if err != nil {
		return err
	}
	defer entries.Close()
	for _, entry := range entries.entries {
		f(entry)
	}
	return nil
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
		return nil, nil
	}

	if err := txn.IncrStatementID(ctx, true); err != nil {
		return nil, err
	}

	if err := txn.transferTombstonesByCommit(ctx); err != nil {
		return nil, err
	}

	// mergeTxnWorkspaceLocked (and the compactDeletionOnObjsLocked call
	// inside it) mutates the workspace and may release/re-acquire the lock
	// around metadata resolution, so it must run with the lock held — same
	// as the IncrStatementID path.
	txn.Lock()
	if err := txn.mergeTxnWorkspaceLocked(ctx); err != nil {
		txn.Unlock()
		return nil, err
	}
	if err := txn.dumpBatchLocked(ctx, workspaceDumpAll(true)); err != nil {
		txn.Unlock()
		return nil, err
	}
	txn.Unlock()

	if msg, injected := objectio.CNCommitAfterWorkspaceDumpFailedInjected(); injected {
		if msg == "" {
			msg = "injected commit failure after workspace dump"
		}
		return nil, moerr.NewInternalError(ctx, msg)
	}

	if err := txn.traceWorkspaceLocked(true); err != nil {
		return nil, err
	}

	usage := txn.workspace.usageSnapshot()
	if usage.totalBytes > 10*mpool.MB {
		logutil.Info(
			"BIG-TXN",
			zap.Uint64("workspace-size", usage.totalBytes),
			zap.String("txn", txn.op.Txn().DebugString()),
		)
	}

	if usage.totalBytes > 100*mpool.MB {
		size := 0
		entries, err := txn.workspace.commitEntries()
		if err != nil {
			return nil, err
		}
		for _, e := range entries.entries {
			if e.bat == nil || e.bat.RowCount() == 0 {
				continue
			}
			size += e.bat.Size()
		}
		entries.Close()
		logutil.Warn(
			"BIG-TXN",
			zap.Uint64("statistical-size", usage.totalBytes),
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
	commitBuilder, err := txn.newWorkspaceCommitBuilder()
	if err != nil {
		return nil, err
	}
	defer commitBuilder.Close()
	reqs, err = commitBuilder.Build(ctx)
	if err != nil {
		return nil, err
	}

	return reqs, nil
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
	// The Commit may have reached TN. delTransaction releases only CN-local
	// state and intentionally does not run gcObjsByIdxRange or delete shared
	// object-storage data.
	txn.delTransaction()
}

func (txn *Transaction) transferTombstonesByCommit(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	if !txn.op.Txn().IsRCIsolation() {
		return nil
	}

	rcState := txn.workspace.rcBoundaryState()
	if rcState.pendingTransfer ||
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
	start := txn.workspace.rcBoundaryState().lastTransferred
	end := types.TimestampToTS(txn.op.SnapshotTS())
	if err = txn.transferTombstonesRange(
		ctx,
		start,
		end,
		false,
		rcBoundaryPublication{
			lastTransferred: end,
			pendingTransfer: false,
		},
	); err != nil {
		return err
	}
	return nil
}

// transferTombstonesRange prepares every in-memory row-id rewrite and every
// remote tombstone object before publishing one Workspace transition. Remote
// objects are garbage-collected if preparation or publication fails; logical
// state and the RC transfer cursor remain unchanged.
func (txn *Transaction) transferTombstonesRange(
	ctx context.Context,
	start, end types.TS,
	advanceStatement bool,
	boundary rcBoundaryPublication,
) (err error) {
	staged := stagedTombstoneTransfer{}
	published := false
	defer func() {
		if published {
			return
		}
		staged.cleanMetadata(txn.proc.Mp())
		if len(staged.objectStats) != 0 {
			txn.Unlock()
			_ = txn.GCObjsByStats(staged.objectStats...)
			txn.Lock()
		}
	}()

	if err = prepareInmemTombstones(ctx, txn, start, end, &staged); err != nil {
		return err
	}
	if err = prepareTombstoneObjects(ctx, txn, start, end, &staged); err != nil {
		return err
	}
	_, err = txn.workspace.publishRCBoundaryWithTransition(
		staged.sources,
		staged.targets,
		advanceStatement,
		boundary,
	)
	if err != nil {
		return err
	}
	published = true
	for _, idx := range staged.objectTargets {
		txn.publishSpilledObjectLocked(&staged.targets[idx].entry)
	}
	if len(staged.objectTargets) != 0 {
		txn.hasS3Op.Store(true)
		txn.readOnly.Store(false)
	}
	for _, fields := range staged.logs {
		logutil.Info("CN-TRANSFER-TOMBSTONE-OBJ", fields...)
	}
	staged.ownedBatches = nil
	return nil
}

func forceTransfer(ctx context.Context) bool {
	return ctx.Value(UT_ForceTransCheck{}) != nil
}

func skipTransfer(ctx context.Context, txn *Transaction) bool {
	return time.Since(txn.start) < txn.engine.config.cnTransferTxnLifespanThreshold
}

func (txn *Transaction) Rollback(ctx context.Context) error {
	entries, err := txn.workspace.commitEntries()
	if err != nil {
		return err
	}
	defer entries.Close()
	if !txn.ReadOnly() && len(entries.entries) > 0 {
		logutil.Info(
			"Transaction.Rollback",
			zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
		)
	}
	_, loadCleanupErr := txn.deleteLoadFiles(ctx, txn.workspace.allLoadFiles())

	// For CCPR transactions, call OnTxnRollback to clean up the cache and GC objects
	// Skip ordinary workspace-entry GC for CCPR transactions because:
	// 1. CCPR objects may be shared across transactions
	// 2. CCPRTxnCache.OnTxnRollback handles GC properly (only deletes when no other txn references the object)
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnRollback(txn.op.Txn().ID)
	} else {
		//to gc the s3 objs
		if err := txn.gcWorkspaceEntries(entries.entries, cloneGCTxnRollback); err != nil {
			panic("Rollback txn failed: to gc objects generated by CN failed")
		}
	}
	// commitEntries pins every payload generation used by rollback GC. Release
	// that read snapshot before destroying the workspace; delTransaction must
	// never invalidate a payload that is still owned by a live lease.
	entries.Close()
	txn.delTransaction()
	return loadCleanupErr
}

func (txn *Transaction) delTransaction() {
	if txn.removed {
		return
	}

	if txn.isCloneTxn {
		txn.engine.cloneTxnCache.DeleteTxn(txn.op.Txn().ID)
		txn.isCloneTxn = false
	}

	txn.assertWorkspaceAccountingLocked()
	if err := txn.workspace.close(txn.proc.Mp()); err != nil {
		panic(err)
	}
	txn.pkCount = 0

	txn.tableCache = nil

	txn.haveDDL.Store(false)
	colexec.MustGetServer(txn.engine.service).DeleteTxnSegmentIds(txn.op.Txn().ID)
	txn.hasS3Op.Store(false)
	txn.removed = true

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

func (txn *Transaction) clearTableCache() {
	txn.tableCache.Range(func(key, value any) bool {
		txn.tableCache.Delete(key)
		return true
	})
}

// ApproximateInMemInsertSize returns the approximate total size of in-memory
// insert entries in the workspace. Intended for testing and diagnostics.
func (txn *Transaction) ApproximateInMemInsertSize() uint64 {
	txn.Lock()
	defer txn.Unlock()
	return txn.workspace.usageSnapshot().inMemoryInsertBytes
}

func (txn *Transaction) CloneSnapshotWS() client.Workspace {
	ws := &Transaction{
		proc:     txn.proc,
		engine:   txn.engine,
		tnStores: txn.tnStores,
		idGen:    txn.idGen,

		tableCache: new(sync.Map),
		workspace:  newTxnWorkspace(),

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
