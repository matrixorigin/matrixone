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
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
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
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

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
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	txn.accountWorkspaceEntryLocked(&entry)
	txn.writes = append(txn.writes, entry)
}

// transferWorkspaceWritesForAlterLocked duplicates every live in-memory
// write entry of the given table onto the table's post-ALTER identity (the
// ALTER recreation drops the metadata rows of the in-txn creation, so the
// writes must survive under the new name) and marks the originals fully
// deleted for the statement-boundary shrink. The duplicate owns no key
// index: it inherits neither the original's hydrated map nor its charged
// bytes, and hydrates its own on first probe. Copying either would leave
// two refund owners for one charge, so the first invalidation of either
// copy would refund the same bytes twice and underflow the uint64 budget
// totals.
func (txn *Transaction) transferWorkspaceWritesForAlterLocked(
	tableId uint64,
	tableName string,
	renameColMap map[string]string,
) error {
	for i, n := 0, len(txn.writes); i < n; i++ {
		if cur := txn.writes[i]; cur.tableId == tableId && cur.bat != nil && cur.bat.RowCount() > 0 {
			if sels, exist := txn.batchSelectList[cur.bat]; exist && len(sels) == cur.bat.RowCount() {
				continue
			}
			var err error
			transferred := cur
			transferred.tableName = tableName // in case renaming
			transferred.bat, err = cur.bat.Dup(txn.proc.Mp())
			transferred.accountedSize = 0
			// Ownership of the key index and its budget bytes stays with the
			// original entry until its rows are shrunk away: the duplicate
			// starts unindexed and uncharged.
			transferred.keyOffsets = nil
			transferred.keyIndexAccountedSize = 0
			if err != nil {
				return err
			}
			if len(renameColMap) > 0 {
				for i, attr := range transferred.bat.Attrs {
					if newName, ok := renameColMap[attr]; ok {
						transferred.bat.Attrs[i] = newName
					}
				}
			}
			txn.appendWorkspaceEntryLocked(transferred)
			txn.selectAllBatchRowsLocked(cur.bat)
		}
	}
	return nil
}

// computeInsertKeyRangeLocked encodes the entry's primary-key column once and
// records the byte-wise min/max envelope on the entry, if it can be resolved.
// Whenever anything prevents a sound envelope, the range stays unset and the
// entry keeps the unpruned FilterVector path.
//
// The envelope shares one key space with MemPKFilter: EncodePrimaryKeyVector
// is the same encoder readers run per batch, and both resolve the same
// workspace-batch layout invariant (rowid prefixed, then schema-ordered
// columns) that filterInMemUnCommittedInserts already depends on.
func (txn *Transaction) computeInsertKeyRangeLocked(entry *Entry) {
	if entry.typ != INSERT || !entry.pkCheckReady || entry.pkCheckPos < 0 ||
		entry.bat == nil || txn.engine == nil || txn.engine.packerPool == nil {
		return
	}
	if entry.pkCheckPos >= len(entry.bat.Vecs) {
		return
	}
	pkVec := entry.bat.Vecs[entry.pkCheckPos]
	if pkVec == nil || pkVec.Length() == 0 || pkVec.HasNull() {
		return
	}

	var packer *types.Packer
	put := txn.engine.packerPool.Get(&packer)
	defer put.Put()

	// EncodePrimaryKeyVector is the same encoder MemPKFilter.FilterVector
	// runs per read, so envelope and filter share one key space.
	keys := readutil.EncodePrimaryKeyVector(pkVec, packer)
	if len(keys) == 0 {
		return
	}
	minKey, maxKey := keys[0], keys[0]
	for _, key := range keys[1:] {
		if bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
	}
	entry.pkRange = insertKeyRange{
		inited: true,
		min:    bytes.Clone(minKey),
		max:    bytes.Clone(maxKey),
	}
}

// ensureInsertKeyOffsetsLocked returns the entry's exact-key probe index,
// building it on first use. The map describes the entry's CURRENT batch
// layout; every batching mutation must nil keyOffsets so stale layouts are
// never answered. A false return keeps the caller on the FilterVector path.
func (txn *Transaction) ensureInsertKeyOffsetsLocked(entry *Entry) (*insertKeyOffsets, bool) {
	if entry.keyOffsets != nil {
		return entry.keyOffsets, true
	}
	if !entry.pkRange.inited {
		return nil, false
	}
	if entry.bat == nil || entry.bat.RowCount() == 0 {
		// A nil-batch or zero-row entry's index is provably empty: fail
		// closed to the FilterVector path instead of charging a map that
		// can never surface a row (a nil slot is the fully-superseded
		// entry's retired state until the post-dump compaction, issue
		// #25590).
		return nil, false
	}
	var packer *types.Packer
	put := txn.engine.packerPool.Get(&packer)
	defer put.Put()

	keys := readutil.EncodePrimaryKeyVector(entry.bat.Vecs[entry.pkCheckPos], packer)
	// Charge the retained-index estimate to the workspace budget BEFORE the
	// map is allocated (issue 26796 review): the spill thresholds consuming
	// workspaceSize and approximateInMemInsertSize must see the memory that
	// construction is about to retain, and refunds on invalidation return
	// exactly this many bytes, so the budget never drifts.
	txn.accountInsertKeyOffsetsLocked(entry, insertKeyOffsetsEstimateSize(keys))
	offsets := make(map[string][]uint32, len(keys))
	for i, key := range keys {
		offsets[string(key)] = append(offsets[string(key)], uint32(i))
	}
	entry.keyOffsets = &insertKeyOffsets{offsets: offsets}
	readutil.RecordIndexBuildForTest()
	return entry.keyOffsets, true
}

// insertKeyOffsetsEstimateSize is the deterministic upper-bound estimate of
// the bytes an insertKeyOffsets retains for keys: one copied string key plus
// one amortized map bucket per row (distinct keys never exceed the row count),
// one uint32 offset slot per row, plus the map base. Charging and refunding
// exactly this value keeps entry.keyIndexAccountedSize stable without walking
// the built map.
func insertKeyOffsetsEstimateSize(keys [][]byte) uint64 {
	size := uint64(64) // map base
	for _, key := range keys {
		size += uint64(len(key)) + 48 // copied key bytes + amortized bucket
		size += 4                     // one uint32 offset slot per row
	}
	return size
}

// accountInsertKeyOffsetsLocked charges retained key-index bytes to the
// workspace budget so the spill thresholds see them. The approximate insert
// size follows the same ownership rule as batch accounting: only in-memory
// entries of user tables count toward the insert thresholds.
func (txn *Transaction) accountInsertKeyOffsetsLocked(entry *Entry, size uint64) {
	entry.keyIndexAccountedSize = size
	txn.workspaceSize += size
	if entry.fileName == "" && !catalog.IsSystemTable(entry.tableId) && entry.typ == INSERT {
		txn.approximateInMemInsertSize += size
	}
}

// unaccountInsertKeyOffsetsLocked invalidates an entry's exact-key probe
// index: the hydrated map and its budget bytes retire together (refund plus
// pointer drop) through this single choke point, used by batching mutations,
// entry release, workspace truncation, ALTER transfer, and transaction
// teardown, so no path can leave a hydrated map without its charge or a
// refunded map reachable from its entry. It is a no-op for entries whose
// index was never hydrated.
func (txn *Transaction) unaccountInsertKeyOffsetsLocked(entry *Entry) {
	entry.keyOffsets = nil
	if entry.keyIndexAccountedSize == 0 {
		return
	}
	size := entry.keyIndexAccountedSize
	entry.keyIndexAccountedSize = 0
	txn.workspaceSize -= size
	if entry.fileName == "" && !catalog.IsSystemTable(entry.tableId) && entry.typ == INSERT {
		txn.approximateInMemInsertSize -= size
	}
}

// unionInsertKeyRange folds the merged-away entry's envelope into the
// surviving batch entry. Merged rows span both envelopes, so the new envelope
// is the byte-wise union. Losing either side's envelope disables pruning for
// the merged batch.
func unionInsertKeyRange(dst *Entry, src *Entry) {
	if dst.typ != INSERT {
		return
	}
	if !src.pkRange.inited {
		// The merged rows are not known to stay inside dst's envelope.
		dst.pkRange = insertKeyRange{}
		return
	}
	if !dst.pkRange.inited {
		return
	}
	if bytes.Compare(src.pkRange.min, dst.pkRange.min) < 0 {
		dst.pkRange.min = src.pkRange.min
	}
	if bytes.Compare(src.pkRange.max, dst.pkRange.max) > 0 {
		dst.pkRange.max = src.pkRange.max
	}
}

func (txn *Transaction) releaseWorkspaceEntryBatchLocked(idx int) {
	entry := &txn.writes[idx]
	if entry.bat == nil {
		return
	}
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()

	bat := entry.bat
	txn.unaccountWorkspaceEntryLocked(entry)
	txn.unaccountInsertKeyOffsetsLocked(entry)
	delete(txn.batchSelectList, bat)
	entry.pkRange = insertKeyRange{}
	bat.Clean(txn.proc.GetMPool())
	entry.bat = nil
}

// shrinkWorkspaceEntryBatchLocked applies pending delete selections to the
// entry's batch. It reports whether the batch became empty and was retired:
// every row superseded within the txn (UPDATE/REPLACE/DELETE of rows
// inserted by earlier statements) leaves an entry with no live rows. Such
// an entry is retired instead of lingering as a zero-row slot, which keeps
// txn.writes proportional to the live working set: every workspace consumer
// (reads, statement merging, tombstone transfer, delete resolution) rescans
// txn.writes per statement and must not pay for superseded history
// (issue #25590). IncrStatementID and Commit compact the retired slot away
// AFTER the fallible dump of the same admission, in the critical section
// that appends the new statement boundary, so dump errors never reuse a
// pre-compaction rollback boundary; until then the nil slot never moves, so
// recorded statement boundaries stay valid.
func (txn *Transaction) shrinkWorkspaceEntryBatchLocked(idx int, sels []int64) (retired bool) {
	entry := &txn.writes[idx]
	if entry.bat == nil {
		return false
	}
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	bat := entry.bat
	if len(sels) == 0 {
		delete(txn.batchSelectList, bat)
		return false
	}

	slices.Sort(sels)
	sels = slices.Compact(sels)
	txn.unaccountWorkspaceEntryLocked(entry)
	txn.unaccountInsertKeyOffsetsLocked(entry)
	shrinkBatchWithRowids(bat, sels)
	txn.accountWorkspaceEntryLocked(entry)
	delete(txn.batchSelectList, bat)
	if bat.RowCount() == 0 {
		// All rows were superseded; the empty batch serves no reader,
		// merge, dump or commit payload (all of which skip zero-row
		// batches), so release it. accountWorkspaceEntryLocked has just
		// reconciled the accounting and releaseWorkspaceEntryBatchLocked
		// unaccounts it again, Clean()s the empty batch and nils the
		// slot for the compaction pass below.
		txn.releaseWorkspaceEntryBatchLocked(idx)
		return true
	}
	return false
}

// mergeWorkspaceEntryBatchesLocked historically merged workspace entries in
// place and only regenerated the combined batch's rowids afterwards; the
// round-6 review of issue #25590 showed that a rowid-allocation failure at
// that point left a mixed-block batch with its sources already destroyed, so
// the merge pass in mergeTxnWorkspaceLocked now stages the combined batch
// and publishes it only after the copy, every column append and the rowid
// regeneration all succeeded. The production pass keeps that staged flow;
// this in-place helper stays as the isolated two-entry merge unit that
// validates the shared key-index and envelope ownership rules
// (issue #26796) — drop its hydrated key index up front, fail the envelope
// open on the append error path, and union the sources' envelopes on
// success; the staged pass carries the same invariants at its publication.

func (txn *Transaction) mergeWorkspaceEntryBatchesLocked(
	ctx context.Context,
	dstIdx int64,
	srcIdx int64,
) error {
	dst := &txn.writes[dstIdx]
	src := &txn.writes[srcIdx]
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	// The append below mutates the destination batch layout. Drop its stale
	// key index (and refund its budget bytes) up front so the index can never
	// answer offsets for a half-mutated layout, not even on the append error
	// path.
	txn.unaccountInsertKeyOffsetsLocked(dst)
	txn.unaccountWorkspaceEntryLocked(dst)
	txn.unaccountWorkspaceEntryLocked(src)

	if _, err := dst.bat.Append(ctx, txn.proc.Mp(), src.bat); err != nil {
		// Append may have mutated some destination vectors before failing.
		// Restore accounting from the two batches' actual states, and stop
		// trusting the destination envelope: a partially appended batch can
		// hold rows outside the old envelope, so fail open to the unpruned
		// path for the rest of this entry's life.
		dst.pkRange = insertKeyRange{}
		txn.accountWorkspaceEntryLocked(dst)
		txn.accountWorkspaceEntryLocked(src)
		return err
	}

	txn.accountWorkspaceEntryLocked(dst)
	unionInsertKeyRange(dst, src)
	txn.unaccountInsertKeyOffsetsLocked(src)
	delete(txn.batchSelectList, src.bat)
	src.bat.Clean(txn.proc.GetMPool())
	src.bat = nil
	return nil
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
			if entry.keyIndexAccountedSize != 0 {
				return moerr.NewInternalErrorNoCtxf("entry %d: nil batch accounts %d index bytes", i, entry.keyIndexAccountedSize)
			}
			continue
		}
		if (entry.keyOffsets != nil) != (entry.keyIndexAccountedSize != 0) {
			return moerr.NewInternalErrorNoCtxf(
				"entry %d: hydrated key index and its %d accounted bytes are inconsistent",
				i, entry.keyIndexAccountedSize)
		}
		size := uint64(entry.bat.Size())
		if entry.accountedSize != size {
			return moerr.NewInternalErrorNoCtxf("entry %d: accounted size %d, batch size %d", i, entry.accountedSize, size)
		}
		workspaceSize += size + entry.keyIndexAccountedSize
		if entry.fileName != "" || catalog.IsSystemTable(entry.tableId) {
			continue
		}
		switch entry.typ {
		case INSERT:
			insertSize += size + entry.keyIndexAccountedSize
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
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	if err := txn.requireAutoIncrEpochFenceCommit(autoIncrEpoch, autoIncrEpochKnown); err != nil {
		return nil, err
	}

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
	txn.computeInsertKeyRangeLocked(&e)
	txn.appendWorkspaceEntryLocked(e)
	txn.pkCount += bat.RowCount()

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
// clearVacatedWriteSlots drops the retired Entry references that forward
// compaction leaves in the vacated tail of the writes backing array. The
// compactions move live entries toward the front and the caller truncates
// the slice; without this, the stale copies would keep refunded key indexes
// and cleaned batches reachable until a later append happens to reuse the
// slot. A caller may pass a slice already shortened to the kept prefix
// (the dumpAll compaction's append target): the vacated slots live beyond
// its length inside the shared backing array, which indexing a Go slice
// cannot reach, so extend within capacity before clearing.
func clearVacatedWriteSlots(writes []Entry, keep, oldLen int) {
	if oldLen > len(writes) {
		if oldLen > cap(writes) {
			oldLen = cap(writes)
		}
		writes = writes[:oldLen]
	}
	for j := keep; j < oldLen; j++ {
		writes[j] = Entry{}
	}
}

// inMemEntryDataSize returns the workspace-memory bytes a live in-memory
// entry occupies: its batch plus any hydrated key-index charge. Spill sizing
// (scanInMemInsertSize), spill quota calculation, and per-table spill
// selection must all see the same total the workspace budget charges, or a
// workspace whose retained key indexes push it over a spill threshold can
// stay below that threshold in every batching decision and never spill.
func inMemEntryDataSize(entry *Entry) uint64 {
	return uint64(entry.bat.Size()) + entry.keyIndexAccountedSize
}

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
			size += inMemEntryDataSize(&txn.writes[i])
		}
	}
	return size
}

// retryPendingObjectCleanup was removed by the round-14 review (issue
// #25590): its one synchronous deletion round ran inside dumpBatchLocked's
// pre-gate step — statement admission under the transaction's locks — so a
// stalled file service parked admission and the request's cancellation
// behind it. The pre-gate step hands the names to the engine's lossless
// queue instead (handoffPendingObjectCleanup), and the engine's scheduled
// drain and GC-scheduler tick own every retry; the accounting moves with
// the names in the queue-mutex section they already share.

// handoffPendingObjectCleanup transfers the ownership of orphaned storage
// objects to the engine's durable pending-unpublished queue — the same
// pool that collects the workspace's recorded files at rollback drains
// it. It runs once — as the whole transaction-destruction cleanup, see
// finalizePendingObjectCleanup — because nothing of the transaction will
// survive to retry the deletion, so the files must leave with the engine
// or be lost. The round-8 review (issue #25590) pinned that a
// fire-and-forget pool task was NOT a transfer of ownership: its failed
// delete merely logged the captured names and the last owner was gone.
// The queue retains every name until a delete succeeds, so the
// transaction may end here without losing them — not to a busy pool that
// rejects the submission, not to a dirty file service that makes the
// asynchronous attempt fail. The round-12 review moved the transfer
// BEFORE every storage I/O: it holds no lock but the queue's and performs
// no file-service call, so the terminal path terminates independently of
// any storage fate. The live-orphan accounting the admission bound
// consults moves with the names: the enqueue may not double-count them,
// and the accounting is preserved across the ownership transfer and
// released only by a successful deletion (issue #25590 round-12 review).
// The transfer and its accounting release run inside one queue-mutex
// section — the round-13 review's linearization point — so nothing sees
// a torn admission total while the names are on their way. Round 16
// extends that one section across the WHOLE transfer window: the clear,
// the seam below and the enqueue run together under the queue mutex, so
// Engine.Close's snapshot of (producers, queued) — taken under the same
// queue mutex — can no longer observe the two sides of a transfer
// separately and conclude behind a registration that landed between two
// separately-read observations (issue #25590 round-16 review).
// orphanHandoffAfterClear is a nil-in-production test seam invoked with
// the orphan handoff mid-window: the names have LEFT the transaction's
// list — they are visible to no owner yet — and the enqueue has not
// run. The round-15 and round-16 regressions park real orphan transfers
// here to prove Engine.Close's final drain cannot conclude on the empty
// queue while the transfer is registered — including one registered
// AFTER the drain had already observed producers==0 (round 16): a
// parked window HOLDS the queue mutex, so the drain's next snapshot
// waits behind it instead of deciding on a stale count.
var orphanHandoffAfterClear func()

// registerOrphanHandoffProducer counts an in-flight orphan transfer as a
// producer of queue names for Engine.Close's shutdown ownership protocol
// (issue #25590 round-15 review): between the moment a handoff clears
// the transaction's list and the moment its names are visible on the
// engine queue, the names are enumerable in NO owner, and a shutdown
// sampling producers==0 alongside an empty queue would conclude that no
// obligation exists while live accounting still counts the transfer in
// flight. Registering the window on the same counter the Close drain
// reads closes exactly the interval the round-14 producer protocol left
// open for GC callers (round 15), and round 16 makes the registration
// COORDINATED instead of merely counted: the drain reads producers and
// queued as one atomic pair — under the queue mutex this transfer's
// window also holds — so a transfer that registers while the drain is
// mid-flight can never fall behind a decision the drain already made.
// The returned release drops the registration; it runs only AFTER the
// enqueue, so producers==0 implies every transferring name is already
// owned by the queue.
func (e *Engine) registerOrphanHandoffProducer() func() {
	e.gcMu.Lock()
	e.gcProducers++
	e.gcMu.Unlock()
	return func() {
		e.gcMu.Lock()
		e.gcProducers--
		e.gcMu.Unlock()
	}
}

// pendingObjectCleanupItem couples one retained storage-object name with
// its statement-epoch classification (issue #25590 round-21 review,
// finding G1). A name an operator teardown retains while its downstream
// recording consumer can still run — a publication in flight inside the
// recording statement — carries retainedIfRecordingLater with the retaining
// epoch; a definite orphan (a failed dump's epilogue, the compaction
// back-out, a refused record's fail-open retain) carries false and keeps
// the round-14 immediate-transfer semantics.
type pendingObjectCleanupItem struct {
	name                     string
	retainedIfRecordingLater bool
	retainedAt               int
}

// pendingObjectCleanupNames returns the plain-name snapshot of the
// transaction's retained cleanup list (tests and logging).
func (txn *Transaction) pendingObjectCleanupNames() []string {
	names := make([]string, 0, len(txn.pendingObjectCleanup))
	for i := range txn.pendingObjectCleanup {
		names = append(names, txn.pendingObjectCleanup[i].name)
	}
	return names
}

// PendingObjectCleanupNamesForTest exposes the recording transaction's
// retained cleanup names to an external-package intake regression
// driver (issue #25590 round-44 review): the compile-side transport
// tests pin WHICH intake branch a confirmed fragment version selected —
// a refusal on a retaining fragment must book nothing, a refusal on a
// non-retaining fragment must retain the received names — without
// reaching into workspace internals. Production code never calls this.
func (txn *Transaction) PendingObjectCleanupNamesForTest() []string {
	txn.Lock()
	defer txn.Unlock()
	return txn.pendingObjectCleanupNames()
}

// handoffPendingObjectCleanup moves every boundary-eligible retained name
// to the engine's lossless queue at a QUIESCED boundary. Callers HOLD the
// transaction's lock — the wrapper's contract since round 12: the
// terminal callers and the tiered regression sites run under the
// already-held transaction lock, so taking the mutex here would
// self-deadlock them (the exact shape the round-21 full-package run
// pinned with a goroutine dump at a pre-existing call site that ran
// handoffPendingObjectCleanup inside its own txn.Lock() section).
func (txn *Transaction) handoffPendingObjectCleanup() {
	txn.handoffPendingObjectCleanupLocked(true)
}

// handoffPendingObjectCleanupLocked moves the transaction's retained
// cleanup names toward the engine's lossless queue (issue #25590
// rounds 12–16 protocol), under the r5 boundary classification (round-21
// review, finding G1):
//
//   - a name whose record has landed since the retain stays behind — its
//     record's lifecycle owns the decision (publish on commit, the
//     rollback's queue transfer), and a live or committed object may
//     never re-enter the deletion queue through the pending handoff;
//   - a publication-in-flight entry (retainedIfRecordingLater) of the
//     CURRENT statement epoch defers: its downstream recording consumer
//     still runs, so a mid-statement handoff (quiesced=false, a dump
//     attempt inside the recording statement) may not move it;
//   - everything else — older epochs, definite orphans, any entry at a
//     quiesced boundary (the next statement's admission, the commit's
//     dump, a terminal) — moves through the round-15/16 transfer
//     window.
//
// Callers hold the transaction lock.
func (txn *Transaction) handoffPendingObjectCleanupLocked(quiesced bool) {
	if len(txn.pendingObjectCleanup) == 0 {
		return
	}
	e := txn.engine
	if e == nil {
		logutil.Warn(
			"WORKSPACE-PENDING-CLEANUP-DROPPED",
			zap.Strings("files", txn.pendingObjectCleanupNames()),
			zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
		)
		return
	}
	epoch := txn.statementLifecycle.Load()
	var moved []string
	var kept []pendingObjectCleanupItem
	for i := range txn.pendingObjectCleanup {
		it := &txn.pendingObjectCleanup[i]
		if txn.hasRecordedUnpublishedObjectName(it.name) {
			continue // owned by its record's lifecycle since it landed
		}
		// INV1 (issue #25590 round-24 finding F1, design r11 D1'): the
		// handoff's moved split applies the same predicate as the
		// retain funnels, so a committed-borrowed name can never reach
		// the engine's unpublished queue through any path — it is
		// dropped here without moving, in the exact class gcFiles'
		// filter already protects everywhere. Txn-local shared names
		// and every fresh name keep the r21/r22 handoff semantics.
		if borrowsCommittedCloneShare(e, txn, it.name) {
			continue
		}
		if !quiesced && it.retainedIfRecordingLater && int64(it.retainedAt) == epoch {
			kept = append(kept, *it)
			continue
		}
		moved = append(moved, it.name)
	}
	if len(moved) == 0 {
		txn.pendingObjectCleanup = kept
		return
	}
	// The transfer window joins the shutdown ownership protocol BEFORE
	// the names leave the transaction's list: from the write below until
	// the enqueue the moved names are visible to no owner — neither the
	// transaction's list nor the queue — so a descheduled handoff would
	// otherwise outlive an Engine.Close whose final drain concluded on
	// producers==0 and an empty queue, and its resumed enqueue would land
	// on a queue the stopped scheduler never drains again (issue
	// #25590 round-15 review). Round 16 runs the whole window INSIDE the
	// queue mutex: the drain's snapshot of the two shutdown obligations
	// also holds that mutex, so its (producers, queued) pair can never
	// straddle this transfer — a transfer registering while a drain
	// decision is abroad shows up in the drain's very next
	// decision instead of behind its stale captured count (issue
	// #25590 round-16 review). Ownership moves BEFORE anything
	// fallible: whatever the scheduling or the file service does next,
	// the names are the engine's now. The KEPT entries (r5: the
	// recording statement's in-flight publications) stay
	// transaction-owned in the same write.
	releaseProducer := e.registerOrphanHandoffProducer()
	e.unpublishedPending.mu.Lock()
	txn.pendingObjectCleanup = kept
	if orphanHandoffAfterClear != nil {
		orphanHandoffAfterClear()
	}
	queued := e.handoffUnpublishedPendingLocked(moved)
	e.unpublishedPending.mu.Unlock()
	releaseProducer()
	if queued > 0 {
		e.scheduleUnpublishedPendingDrain()
	}
}

// handoffUnpublishedPendingLocked is the queue-locked ownership-transfer
// body — the orphan-handoff wrapper's clear-and-enqueue window runs whole
// inside ONE queue-mutex section so Engine.Close's obligation snapshot
// cannot straddle it (issue #25590 round-16 review). Returns the number
// of names enqueued fresh.
func (e *Engine) handoffUnpublishedPendingLocked(names []string) int {
	if e.unpublishedPending.nameSet == nil {
		e.unpublishedPending.nameSet = make(map[string]struct{}, len(names))
	}
	queued := 0
	for _, n := range names {
		if _, ok := e.unpublishedPending.nameSet[n]; !ok {
			e.unpublishedPending.nameSet[n] = struct{}{}
			e.unpublishedPending.names = append(
				e.unpublishedPending.names, n)
			queued++
		}
	}
	if queued > 0 {
		e.unpublishedPending.arrivalSeq++
	}
	// The inbound names leave the owned-name ledger here: a name that
	// reaches this queue was counted as owned exactly once at
	// creation - as a failed publication's orphan, a record entry's
	// recorded object, or a converted reservation - and its move to
	// queued ownership retires exactly that count (the move either
	// drops the name from ownedNames, or the name never entered the
	// ledger, as with test-seeded queue names). The set move keeps
	// duplicate names from corrupting the counter the admission bound
	// reads (issue #25590 round-18 review). Queued capacity is one
	// slot per name, so the drop retires the whole membership with
	// its claim set (issue #25590 round-57 review): any other
	// claimant's later terminal finds no claim left to release.
	for _, n := range names {
		if _, ok := e.unpublishedPending.ownedNames[n]; ok {
			delete(e.unpublishedPending.ownedNames, n)
			e.unpublishedPending.live--
		}
	}
	return queued
}

// unpublishedPendingQueue retains storage-object names whose deletion has
// not succeeded yet, keyed by nothing but membership: each drain attempt
// deletes every batch whose names the storage accepts and keeps the
// rest. drainScheduled coalesces those attempts: at most one pipeline —
// a submission waiting for GC capacity plus the running task itself —
// exists per engine, so a handoff storm or the GC-scheduler tick cannot
// stack one worker-occupying ladder per trigger onto the shared GC pool
// (issue #25590 round-9 review). The drain context/cancel carry the
// engine-owned cancellation every background drain runs under, and
// drainIdle is non-nil exactly while a pipeline exists — Close cancels
// and joins it (issue #25590 round-10 review). live counts every
// proven-unpublished object an ACTIVE transaction still owns — the
// accounting that survives every ownership transfer intact and that
// only a successful deletion releases — and inFlight counts the dumps
// admitted but not yet finished persisting, so concurrent admissions
// reserve the boundary capacity instead of racing one another past the
// bound (issue #25590 round-12 review). The round-13 review turns that
// pair into a shared linearization point with the names themselves:
// names, live and inFlight all live under THE SAME mutex — every
// retaken append, the failed-retry release, the handoff's enqueue plus
// release, the drain's retirement, and the admission slot's take and
// release run inside one mutex section, so a concurrent admission
// observes the whole three-owner total atomically and can never miss
// the names riding a transfer between a transaction's list and this
// queue.
type unpublishedPendingQueue struct {
	mu    sync.Mutex
	names []string
	// nameSet is the membership index of names; it must stay in lockstep
	// with the slice so an enqueuer answers a duplicate in constant time
	// instead of rescanning the whole retained backlog under the mutex
	// (issue #25590 round-11 review).
	nameSet        map[string]struct{}
	drainScheduled bool
	drainCtx       context.Context
	drainCancel    context.CancelFunc
	drainIdle      chan struct{}
	// ownedNames is the live-name ledger of the admission bound: every
	// name that CREATES an obligation on this engine is a member for
	// exactly as long as it is neither published at commit, physically
	// deleted nor enqueued here. live is len(ownedNames), kept in
	// lockstep under this mutex so the counter the bound reads and the
	// set the ownership transitions move can never disagree. The
	// members are failed-publication orphan names, recorded workspace
	// objects (direct-S3 records and successfully dumped objects
	// alike), and names an admitted GC caller holds on the way to a
	// delete or an enqueue - every name that could yet need this
	// queue's space (issue #25590 round-18 review).
	// The membership value is the name's CLAIM SET (issue #25590
	// round-57 review): the map records every WORKSPACE whose record
	// or retain created the obligation, idempotent per (workspace,
	// name), so two workspaces of one engine claiming the same
	// transferred object - the same-CN re-record the r55 design
	// documents - spend exactly one capacity unit together. A claim
	// holder's published-class settlement releases exactly ITS claim,
	// and the membership plus its capacity retire only when the name's
	// FINAL claim does; a queue entry (the enqueue below) or a storage
	// deletion retires the whole membership, the queue and the deleted
	// file being the obligation then. The member key exists exactly
	// while its claim set is non-empty OR an accepted GC delete batch
	// holds the name — the round-58 rule gcAccepted below states.
	ownedNames map[string]map[*Transaction]struct{}
	live       int64
	inFlight   int64
	// gcAccepted is the engine-owned GC capacity claim — the per-name
	// refcount of outstanding POOL-WORKER delete batches gcFiles
	// submitted and the pool accepted (issue #25590 round-58 review):
	// the moment the pool accepts a batch, its names' admission
	// obligation belongs to that delete job, so an ACCEPTED but
	// unsettled name keeps its owned-member capacity through every
	// terminal's published-class claim release. Compaction and
	// dropped-table cleanup submit these deletions on an OPEN
	// transaction's behalf, so a transaction that commits while its
	// worker stalls inside DeleteUnpublishedObjects would otherwise
	// release the obsolete object's final claim as published and free
	// its unit while the deletion obligation is still outstanding —
	// the review's overshoot: one stalled obsolete object beside two
	// newly admitted names behind a two-unit bound (I4 counts queued,
	// owned AND admitted-in-flight obligations alike). The claim is
	// keyed by NAME alone — the round-40/41 doctrine the settle state
	// already follows — so the completed transaction's graph stays
	// collectible exactly as before, and the worker's settle retires
	// the refcount together with the membership in the
	// storage-deletion whole-membership class. The refcount exists
	// because one name can appear in several in-flight or retried
	// batches; it is bounded by the accepted batch population, which
	// the pool's worker bound and the producers counter already cap.
	gcAccepted map[string]int
	// arrivalSeq counts enqueue events (one bump per call that added at
	// least one name) under the same mutex as the queue itself: two
	// readers of the same sequence value saw the exact same set of
	// arrivals, so Engine.Close's final drain can tell an attempt that
	// genuinely retired nothing from one whose retirable names were
	// compensated by names enqueued mid-attempt — only the first may
	// feed the no-progress conclusion, the second is owed a fresh
	// decision the arrivals never got (issue #25590 round-16 review).
	arrivalSeq uint64
	// recordedOwners is the per-name side channel of the recorded-name
	// queue handoffs (issue #25590 round-38 review): it maps a QUEUED
	// name to the recorded-name HOLDER whose member still tracks it —
	// the rolled-back statement's gcFilesToOwner transfer, the gcFiles
	// pool-failure tail and the gcFiles worker's failed-delete transfer
	// (round-60), every producer that moves names a recording
	// transaction still records toward the queue. Round-60 keys the
	// entry by the holder itself — never the *Transaction — because
	// the accepted worker transfers a failed batch after the submitting
	// transaction may have already ended (round-40/41): the retire the
	// drain performs needs only the holder, and no queued obligation
	// may retain the completed transaction's graph. The entry lives
	// exactly as long as the name's queue membership OWNERSHIP: the
	// once-drain pops it in the same compaction pass that retires the
	// name and retires the owning holder's recorded-name copy in the
	// same worker step — with no transaction lock, exactly the
	// round-37 gcFiles rule — and the TERMINAL that drops the
	// recorded-name copy detaches the expired transaction's entries
	// with it, so an outage parking the queue can never retain a
	// completed transaction's graph behind a name waiting for its
	// delete (issue #25590 round-40 review, finding 1). Owner-less
	// queued names (orphan funnels, writer tails, test enqueues)
	// simply carry no entry. A duplicate delivery while the name is
	// still queued refreshes the owner — the retire semantic is per
	// FILE, so the last transfer wins.
	recordedOwners map[string]*txnRecordedUnpublished
}

// unpublishedPendingDrainRounds bounds one drain task's retry ladder; the
// queue keeps every name that survives the ladder, and the engine's GC
// scheduler — not the arrival of a new orphan — schedules the backlog's
// next attempt (issue #25590 round-9 review).
const unpublishedPendingDrainRounds = 8

// unpublishedPendingDeleteBatch partitions one drain round into
// independently attempted batches: a failing batch keeps exactly its own
// names and never walls the later queued names away from every retry
// (issue #25590 round-10 review). It matches the storage-side delete
// batch bound of DeleteUnpublishedObjects.
const unpublishedPendingDeleteBatch = 1000

// gcUnpublishedPendingObjectTicker paces the engine GC scheduler's retry
// of this queue (engine.go). The tick costs one mutex round-trip while
// the queue is empty and is coalesced behind an in-flight drain, so a
// one-minute pace makes storage recovery produce cleanup without any new
// orphan.
var gcUnpublishedPendingObjectTicker = time.Minute

// unpublishedPendingDrainBudget bounds one whole drain task — every round
// of it, not just one storage attempt — on the engine-owned drain
// context: a slow or stalling storage may not keep a GC worker for
// ladder after ladder while the shutdown waits behind it. Names the
// budget strands stay queued for the tick's next attempt, so the bound
// costs nothing but time (issue #25590 round-10 review).
var unpublishedPendingDrainBudget = 10 * time.Minute

// unpublishedPendingCloseDrainTimeout bounds the shutdown-time drain
// work Close performs: the bounded join of an already-running drain and
// its single final delete attempt. Test-pinnable as a package var.
var unpublishedPendingCloseDrainTimeout = 10 * time.Second

// workspaceDumpWriteS3Threshold is the memory threshold the workspace
// dump's spilling writers run with; -1 keeps the production default.
// It exists as a package var because the round-18 regressions make one
// dump spill several objects deterministically by pinning it to spill
// per batch, while production stays byte-identical to the previous
// fixed default.
var workspaceDumpWriteS3Threshold = -1

// unpublishedPendingAdmissionLimit bounds every obligation name this
// engine retains toward the cleanup queue: the queue's own names, every
// transaction-owned orphan and recorded workspace object (direct-S3
// inserts and successfully dumped objects alike — anything a rollback
// could hand to the queue), and the per-object reservations of spilling
// writers in flight. Refusal is fail-closed: only NEW obligations stop
// (a spilling writer's object, a recording write), while everything
// already owned stays exactly as owned — the handoffs stay lossless —
// and the refusal ends the moment a drain retires work
// (issue #25590 round-12/18 reviews). Test-pinnable as a package var.
var unpublishedPendingAdmissionLimit = 500_000

// PinUnpublishedPendingAdmissionLimitForTest pins the admission bound for
// an external-package regression driver — the engine-level saturation
// regressions round 19 adds — and returns its restore func. Nested pins
// restore the value they found; production code never calls this.
func PinUnpublishedPendingAdmissionLimitForTest(limit int) (restore func()) {
	old := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = limit
	return func() {
		unpublishedPendingAdmissionLimit = old
	}
}

// NewEngineForTest builds a bare engine fixture attached to a real file
// service for an external-package regression driver (issue #25590 round-31
// review, finding 3): the unpublished-cleanup queue's accounting and its
// bounded delete pass then run exactly as production against that
// service, so a settlement driver can prove the exact queue handoff AND
// the physical retirement. Production code never constructs an engine
// this way.
func NewEngineForTest(fs fileservice.FileService) *Engine {
	return &Engine{fs: fs}
}

// InspectUnpublishedPendingForTest returns the cleanup queue's exact
// snapshot — the retire-pending names in queue order plus the owned and
// in-flight capacity counters — so an external-package settlement
// regression can assert which names reached engine cleanup and that the
// accounting balances (issue #25590 round-31 review, finding 3).
// Production code never calls this.
func (e *Engine) InspectUnpublishedPendingForTest() (queued []string, live int, inFlight int) {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	queued = slices.Clone(e.unpublishedPending.names)
	return queued, int(e.unpublishedPending.live), int(e.unpublishedPending.inFlight)
}

// DrainUnpublishedPendingForTest applies the queue's bounded delete pass
// once — the same unit the asynchronous ladder and the shutdown-time
// drain share — so an external-package settlement regression can prove
// the physical deletion and the exact queue retirement. Deleting
// tolerates an already-absent file exactly as production (issue #25590
// round-31 review, finding 3). Production code never calls this.
func (e *Engine) DrainUnpublishedPendingForTest(ctx context.Context) error {
	return e.deleteQueuedUnpublishedPendingOnce(ctx)
}

// UnsettledCleanupOwnershipForTest returns the cleanup names this
// transaction still owns — the pending cleanup list plus the outstanding
// reservation map — so an external-package settlement regression can
// assert that a terminal transfer or handoff cleared the transaction's
// ownership instead of leaving it stranded (issue #25590 round-31
// review, finding 3). Production code never calls this.
func (txn *Transaction) UnsettledCleanupOwnershipForTest() (pending []string, reserved []string) {
	pending = append(pending, txn.pendingObjectCleanupNames()...)
	txn.unpublishedDumpReservationsMu.Lock()
	reserved = make([]string, 0, len(txn.unpublishedDumpReservations))
	for name := range txn.unpublishedDumpReservations {
		reserved = append(reserved, name)
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	slices.Sort(pending)
	slices.Sort(reserved)
	return pending, reserved
}

// enqueueUnpublishedPending takes the engine-level ownership of names,
// immediately and losslessly: the queue append is the transfer. A
// duplicate name is a no-op — a later transaction can hand the same file
// only once. Draining is scheduled opportunistically; every failure mode
// of the schedule leaves the names owned here, retried by the engine's
// GC scheduler on its next tick.
func (e *Engine) enqueueUnpublishedPending(names []string) {
	e.enqueueUnpublishedPendingFrom(nil, names)
}

// enqueueUnpublishedPendingFrom is the owner-carrying twin of the
// enqueue (issue #25590 round-38 review): a handoff that moves names a
// recording transaction still records — the rolled-back statement's
// gcFilesToOwner transfer and the gcFiles pool-failure tail — registers
// the transaction's recorded-name HOLDER per name INSIDE the same
// queue-mutex section as the transfer, so a queued recorded name can
// never sit without its recorded-copy owner between the handoff and the
// drain's settle. Round-60 re-keys the registration by the holder alone,
// so the accepted worker's failed-delete transfer can register exactly
// the independently owned settle state it captured (round-40/41). The
// registration only ever ADDS an owner; an owner-less enqueue never
// clears an existing entry, so the test and orphan enqueues cannot
// dissolve the protection of a still-recorded member.
func (e *Engine) enqueueUnpublishedPendingFrom(
	owner *Transaction, names []string,
) {
	var holder *txnRecordedUnpublished
	if owner != nil {
		holder = owner.recordedUnpublishedObjects
	}
	e.unpublishedPending.mu.Lock()
	e.enqueueUnpublishedPendingHolderLocked(holder, names)
	e.unpublishedPending.mu.Unlock()
	e.scheduleUnpublishedPendingDrain()
}

// enqueueUnpublishedPendingHolderLocked moves names into the lossless
// queue under the queue mutex: a name new to the queue takes its slot
// and drops any owned-ledger membership in the same section (capacity
// moves, never freed), a duplicated name keeps exactly one queue slot,
// and a live holder registers per name for the drain's settle to retire
// the recorded copy at proof of deletion. The caller holds the queue
// mutex.
func (e *Engine) enqueueUnpublishedPendingHolderLocked(
	holder *txnRecordedUnpublished, names []string,
) {
	if len(names) == 0 {
		return
	}
	if e.unpublishedPending.nameSet == nil {
		e.unpublishedPending.nameSet = make(map[string]struct{}, len(names))
	}
	added := 0
	for _, n := range names {
		if _, ok := e.unpublishedPending.nameSet[n]; !ok {
			e.unpublishedPending.nameSet[n] = struct{}{}
			e.unpublishedPending.names = append(e.unpublishedPending.names, n)
			added++
			// A name entering the queue retires exactly its owned-ledger
			// membership - a recorded object handed off at rollback, a GC
			// caller's tail, or a failed publication's orphan - so the
			// inbound move can never double-count a name that enters
			// queued ownership while already counted as owned
			// (issue #25590 round-18 review). The delete retires the
			// WHOLE membership, claim set included: queued capacity is
			// one slot per name whatever the claims were, the queue owns
			// the obligation now, and a later published-class release
			// held by another claimant simply finds no claim left to
			// release (issue #25590 round-57 review).
			if e.unpublishedPending.ownedNames != nil {
				if _, ok := e.unpublishedPending.ownedNames[n]; ok {
					delete(e.unpublishedPending.ownedNames, n)
					e.unpublishedPending.live--
				}
			}
		}
		if holder != nil {
			if e.unpublishedPending.recordedOwners == nil {
				e.unpublishedPending.recordedOwners = make(
					map[string]*txnRecordedUnpublished, len(names))
			}
			e.unpublishedPending.recordedOwners[n] = holder
		}
	}
	if added > 0 {
		e.unpublishedPending.arrivalSeq++
	}
}

// requeueFailedGCUnpublishedPending hands a FAILED accepted delete batch
// back to the engine's bounded retry owner (issue #25590 round-60
// review): ioutil.DeleteUnpublishedObjects stops at its own first
// failing storage sub-batch and proves nothing about the rest, so the
// settled treatment — retiring the ledger membership and the
// recorded-name copy — would free capacity and drop the last enumerable
// owner while the objects may still exist, the exact loss class the
// ledger's lossless-ownership contract (design I2) forbids. The whole
// batch transfers in ONE queue-mutex section: the accepted job's
// engine-owned claim releases exactly there (otherwise the settle the
// queue performs never reads the refcount and the claim would linger),
// each name's owned membership moves whole into the queue's own slot
// (the unit stays counted through the outage), and the holder the
// worker captured registers per name so the drain's settle retires the
// recorded copy only at a proven deletion or not-found. Recovery — the
// drain's ladder and the GC tick behind it — deletes what remains,
// tolerates the names the failure had already taken, and retires the
// unit and the copy per name at the same proof point. The bounded
// section runs on the pool worker without the transaction.
func (e *Engine) requeueFailedGCUnpublishedPending(
	holder *txnRecordedUnpublished, batch []string,
) {
	if len(batch) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	// The queue takes over the obligation, so no phantom accepted-job
	// claim may stay behind names the queue now owns (issue #25590
	// round-58 review, the same unwind the refused submission takes) -
	// and the release shares the section with the enqueue below, so a
	// concurrent published-class release cannot observe a claim-less
	// membership between the two and free the retained unit (the
	// single-section linearization every accounting transition shares,
	// round-13).
	e.releaseUnpublishedPendingGCNamesLocked(batch)
	e.enqueueUnpublishedPendingHolderLocked(holder, batch)
	e.unpublishedPending.mu.Unlock()
	e.scheduleUnpublishedPendingDrain()
}

// reserveUnpublishedPendingObject is the fail-closed admission gate for
// ONE storage object a workspace spilling writer is about to persist:
// the sinker's capacity hook calls it right before the object would go
// durable, holding one in-flight reservation in OBJECT units rather
// than writer units - a dump whose writer spills three objects takes
// three reservations and can no longer push the ledger past the bound
// by spilling several objects behind one slot (issue #25590 round-18
// review, finding 1). The reservation, the snapshot and the decision all
// run inside ONE queue-mutex section - the linearization point every
// accounting transition shares (issue #25590 round-13 review) - so a
// refusal never observes a torn total and CONCURRENT spilling writers
// share the remaining capacity through their per-name reservations
// instead of racing one another past the bound. Only the spilling
// writer stops: everything already held stays exactly as owned, and
// every drain that retires work re-opens admission. A refused
// reservation aborts the spill before the object persists, so nothing
// half-persisted can appear behind it.
func (e *Engine) reserveUnpublishedPendingObject(name string) error {
	e.unpublishedPending.mu.Lock()
	e.unpublishedPending.inFlight++
	queued := len(e.unpublishedPending.names)
	owned := int(e.unpublishedPending.live)
	inFlight := int(e.unpublishedPending.inFlight)
	if queued+owned+inFlight > unpublishedPendingAdmissionLimit {
		e.unpublishedPending.inFlight--
		e.unpublishedPending.mu.Unlock()
		logutil.Warn(
			"WORKSPACE-UNPUBLISHED-PENDING-ADMISSION",
			zap.String("object", name),
			zap.Int("retained", queued+owned),
			zap.Int("limit", unpublishedPendingAdmissionLimit),
			zap.Int("live", owned),
			zap.Int("inflight", inFlight),
		)
		return moerr.NewInternalErrorf(
			context.Background(),
			"workspace admission paused: %d retained unpublished storage objects reached the admission bound %d",
			queued+owned, unpublishedPendingAdmissionLimit,
		)
	}
	e.unpublishedPending.mu.Unlock()
	return nil
}

// convertUnpublishedPendingReservations turns the spilled dump's
// per-object reservations into RETAINED OWNERSHIP in one queue-mutex
// section: every name the dump persisted - successfully recorded or
// taken over the failed-publication epilogue - releases exactly its
// reservation and joins the owned-name ledger, so the inbound move is
// atomic against every admission snapshot and the ledger can never
// hold a reservation whose owning dump already ended (issue #25590
// round-18 review, finding 1). The names are unique per object, but the
// conversion still consumes exactly one reservation per input name -
// the hook fired once per persisted object - independent of any
// membership the name already has elsewhere. The CONVERTING workspace
// claims each name (issue #25590 round-57 review): a name another
// workspace already counted keeps its one capacity unit and only gains
// this claim in the set, so the last claim's release is the only one
// that can retire the capacity.
func (e *Engine) convertUnpublishedPendingReservations(
	txn *Transaction, names []string,
) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	e.unpublishedPending.inFlight -= int64(len(names))
	if e.unpublishedPending.ownedNames == nil {
		e.unpublishedPending.ownedNames = make(map[string]map[*Transaction]struct{}, len(names))
	}
	for _, n := range names {
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			e.unpublishedPending.ownedNames[n] = make(map[*Transaction]struct{}, 1)
			e.unpublishedPending.live++
		}
		e.unpublishedPending.ownedNames[n][txn] = struct{}{}
	}
	e.unpublishedPending.mu.Unlock()
}

// admitUnpublishedPendingRecordedNames admits recorded objects into the
// ledger AT CREATION - every write that records S3 object names a
// rollback could hand to this queue (the direct-S3 metadata branches of
// txnTable.Write among them) counts its names here first, so obligations
// that can reach the queue from ROLLBACK no longer pass behind the
// bound (issue #25590 round-18 review, finding 2). Refusal fails the
// recording write fail-closed: only new obligations stop, every owner
// keeps exactly what it holds, and a drain retiring work re-opens
// admission. Already-counted names keep the duplicate-record rule -
// no SECOND capacity unit - but the recording workspace gains its CLAIM
// in the set (issue #25590 round-57 review): the same-CN re-record of
// a transferred object registers the recording side's claim so a later
// fragment settlement cannot release capacity this claim still needs,
// and the membership retires only with the name's final claim.
func (e *Engine) admitUnpublishedPendingRecordedNames(
	txn *Transaction, names []string,
) error {
	if len(names) == 0 {
		return nil
	}
	e.unpublishedPending.mu.Lock()
	if e.unpublishedPending.ownedNames == nil {
		e.unpublishedPending.ownedNames = make(map[string]map[*Transaction]struct{}, len(names))
	}
	fresh := 0
	for _, n := range names {
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			if _, ok := e.unpublishedPending.nameSet[n]; ok {
				continue // queued names are already counted and bound
			}
			fresh++
		}
	}
	queued := len(e.unpublishedPending.names)
	owned := int(e.unpublishedPending.live)
	inFlight := int(e.unpublishedPending.inFlight)
	if queued+owned+inFlight+fresh > unpublishedPendingAdmissionLimit {
		e.unpublishedPending.mu.Unlock()
		logutil.Warn(
			"WORKSPACE-UNPUBLISHED-PENDING-ADMISSION",
			zap.Strings("objects", names),
			zap.Int("retained", queued+owned),
			zap.Int("limit", unpublishedPendingAdmissionLimit),
			zap.Int("live", owned),
			zap.Int("inflight", inFlight),
		)
		return moerr.NewInternalErrorf(
			context.Background(),
			"workspace admission paused: %d retained unpublished storage objects reached the admission bound %d",
			queued+owned, unpublishedPendingAdmissionLimit,
		)
	}
	for _, n := range names {
		if _, ok := e.unpublishedPending.nameSet[n]; ok {
			continue // queued names are already counted and bound
		}
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			e.unpublishedPending.ownedNames[n] = make(map[*Transaction]struct{}, 1)
			e.unpublishedPending.live++
		}
		e.unpublishedPending.ownedNames[n][txn] = struct{}{}
	}
	e.unpublishedPending.mu.Unlock()
	return nil
}

// releaseUnpublishedPendingPublished retires the CALLING workspace's
// claims on names whose fate left the engine's obligation set this way -
// a committed workspace object is published data, not a cleanup
// obligation, and must return the capacity the moment the commit brings
// it into the visible dataset (issue #25590 round-18 review, finding 2).
// The release is per-claim (issue #25590 round-57 review): a name whose
// claim set still holds ANOTHER workspace's claim - the same-CN
// re-record of a transferred object - keeps its membership and its
// capacity counted until that final claim retires, so a transferred
// fragment's settlement cannot free capacity a live recording workspace
// still owes. Round-58 (issue #25590 review of 2026-10-28) adds the
// engine-owned GC claim to the same retirement condition: a name whose
// delete a POOL WORKER accepted keeps its membership and capacity while
// its claim set empties, because that accepted job — not any workspace,
// and never a completed transaction (round-40/41) — owns the obligation
// until the worker's settle retires it with the whole membership
// (compaction and dropped-table cleanup submit such deletes on an open
// transaction's behalf, whose commit payload no longer carries the
// obsolete object; freeing its unit at the terminal admitted the review's
// overshoot — one stalled obsolete object beside up to a full bound of
// new names, violating I4's retained-population cap). The release stays
// idempotent per (workspace, name) for duplicate pending or repeated
// deliveries, and names already moved out - handed off, deleted, or
// never claimed - simply no-op, so commit after any intermediate
// transfer cannot double-release.
func (e *Engine) releaseUnpublishedPendingPublished(
	txn *Transaction, names []string,
) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	if e.unpublishedPending.ownedNames != nil {
		for _, n := range names {
			claims, ok := e.unpublishedPending.ownedNames[n]
			if !ok {
				continue
			}
			if _, mine := claims[txn]; !mine {
				continue
			}
			delete(claims, txn)
			if len(claims) > 0 {
				continue
			}
			if e.unpublishedPending.gcAccepted[n] > 0 {
				// The accepted delete batch holds the name's capacity:
				// the membership retires whole at the batch's settle.
				continue
			}
			delete(e.unpublishedPending.ownedNames, n)
			e.unpublishedPending.live--
		}
	}
	e.unpublishedPending.mu.Unlock()
}

// detachTerminalRecordedUnpublishedObjectOwners ends the recorded-owner
// registrations of a TERMINAL transaction (issue #25590 round-40
// review, finding 1): the release that dropped the recorded-name copy
// has just ended every obligation the entries could still retire, so no
// queued name may keep the completed transaction strongly reachable
// until the drain happens to delete its file - an outage parking the
// queue would otherwise retain the whole terminated graph
// (statement-offset history, the writes backing slice and the process
// reference) behind every name it handed off, memory the admitted-name
// bound does not count. The pop is conditional per name: only an entry
// THIS transaction owns detaches - a name two transactions recorded
// keeps the LAST handoff's owner exactly as the round-38 registration
// rule pinned it, and an entry the drain's settle already popped is a
// no-op. The pass is bounded by the release's own live-set bound (the
// names the admission bound ever counted this transaction, the same
// list the release already copies) and runs under the queue mutex
// alone, the established txn.Lock -> queue.mu order every enqueue path
// shares - no new lock, state or hot-path scan.
func (e *Engine) detachTerminalRecordedUnpublishedObjectOwners(
	txn *Transaction, names []string,
) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	for _, n := range names {
		// Round-60: the entry registered the recorded-name HOLDER, so
		// the identity condition reads the transaction's own holder -
		// a transaction whose workspaces never installed one can own no
		// entry to detach.
		if owner, ok := e.unpublishedPending.recordedOwners[n]; ok &&
			owner == txn.recordedUnpublishedObjects {
			delete(e.unpublishedPending.recordedOwners, n)
		}
	}
	e.unpublishedPending.mu.Unlock()
}

// retainUnpublishedPendingNames gains owned-ledger membership for names
// that are already persisted obligations of a failed dump but whose
// per-object reservations the record choke or the epilogue never saw —
// the membership is the invariant (issue #25590 round-18 review); a
// name that IS already a member joins exactly once (fresh names from a
// spilling writer are impossible, this is the robustness path), and the
// retaining workspace claims it like every membership creator does
// (issue #25590 round-57 review), so its terminal is the release that
// retired exactly this claim.
func (e *Engine) retainUnpublishedPendingNames(
	txn *Transaction, names []string,
) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	if e.unpublishedPending.ownedNames == nil {
		e.unpublishedPending.ownedNames = make(map[string]map[*Transaction]struct{}, len(names))
	}
	for _, n := range names {
		if _, queued := e.unpublishedPending.nameSet[n]; queued {
			continue // queued names already hold the bound's space
		}
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			e.unpublishedPending.ownedNames[n] = make(map[*Transaction]struct{}, 1)
			e.unpublishedPending.live++
		}
		e.unpublishedPending.ownedNames[n][txn] = struct{}{}
	}
	e.unpublishedPending.mu.Unlock()
}

// abandonUnpublishedPendingReservations closes the given outstanding
// reservations without any obligation behind them: the transaction's
// storage work ended without the objects ever being recorded or swept,
// so their in-flight units return to the bound exactly once
// (issue #25590 round-18 review).
func (e *Engine) abandonUnpublishedPendingReservations(names []string) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	e.unpublishedPending.inFlight -= int64(len(names))
	e.unpublishedPending.mu.Unlock()
}

// retainUnpublishedPendingGCNames takes the ENGINE-OWNED capacity claim
// of names whose delete batch gcFiles is about to hand to the pool
// (issue #25590 round-58 review): from the moment the submission can no
// longer fail back to the caller, the obligation of these names belongs
// to the accepted delete job, so the ledger must keep their membership
// counted through the submitting transaction's own terminal — the claim
// is keyed by name alone, never by the Transaction, so the completed
// transaction's graph stays collectible exactly as round-40/41 pinned.
// Registration runs on the submitting goroutine BEFORE pool.Submit, so
// no worker settle can race the refcount in; the settle, the
// failed-delete transfer's claim release and the submit-failure unwind
// are the only paths that retire it (round-60 adds the transfer: a
// worker whose delete failed may no longer settle, so it hands the
// claim back with the names).
func (e *Engine) retainUnpublishedPendingGCNames(names []string) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	e.retainUnpublishedPendingGCNamesLocked(names)
	e.unpublishedPending.mu.Unlock()
}

// releaseUnpublishedPendingGCNames settles the engine-owned GC claim of
// batch names whose submission FAILED before any worker could owe them
// a delete: the names transfer to the lossless queue tail in the same
// breath, whose enqueue retires the whole membership into the queue's
// own slot, so the accepted-job class must not keep a phantom claim on
// names the queue now owns (issue #25590 round-58 review).
func (e *Engine) releaseUnpublishedPendingGCNames(names []string) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	e.releaseUnpublishedPendingGCNamesLocked(names)
	e.unpublishedPending.mu.Unlock()
}

// retainUnpublishedPendingGCNamesLocked registers one accepted-batch
// reference per name; the caller holds the queue mutex.
func (e *Engine) retainUnpublishedPendingGCNamesLocked(names []string) {
	if e.unpublishedPending.gcAccepted == nil {
		e.unpublishedPending.gcAccepted = make(map[string]int, len(names))
	}
	for _, n := range names {
		e.unpublishedPending.gcAccepted[n]++
	}
}

// releaseUnpublishedPendingGCNamesLocked retires one reference per name
// and drops the entry at zero; settling an unregistered name — a name
// the queue's own drain deleted, or one never submitted to the pool - is
// the documented no-op. The caller holds the queue mutex.
func (e *Engine) releaseUnpublishedPendingGCNamesLocked(names []string) {
	for _, n := range names {
		if refs, ok := e.unpublishedPending.gcAccepted[n]; ok {
			if refs <= 1 {
				delete(e.unpublishedPending.gcAccepted, n)
			} else {
				e.unpublishedPending.gcAccepted[n] = refs - 1
			}
		}
	}
}

// settleUnpublishedPendingDeleted retires the ledger membership of
// names an admitted GC caller PROVED finished — every name deleted or
// returned not-found by the storage call that just returned nil (round-
// 60 removed the failure class: a failed delete now requeues through
// requeueFailedGCUnpublishedPending and never settles here) — so
// capacity held for a name a workspace recorded cannot stay frozen
// behind a caller whose whole batch already left the caller's hands
// (issue #25590 round-18 review). The retire is the WHOLE membership
// with its claim set: the file is gone for every claimant, so no later
// published-class release may resurrect or retain capacity behind it
// (issue #25590 round-57 review). Round-58 adds the settle's own
// guarantee to the same section: the accepted delete job that held the
// name's engine-owned GC claim is exactly what just completed, so its
// refcount retires here together with the membership — the terminal
// release can never be needed to return the stalled name's capacity,
// and the claim map keeps no batch the workers already finished.
func (e *Engine) settleUnpublishedPendingDeleted(names []string) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	if e.unpublishedPending.ownedNames != nil {
		for _, n := range names {
			if _, ok := e.unpublishedPending.ownedNames[n]; ok {
				delete(e.unpublishedPending.ownedNames, n)
				e.unpublishedPending.live--
			}
		}
	}
	e.releaseUnpublishedPendingGCNamesLocked(names)
	e.unpublishedPending.mu.Unlock()
}

// scheduleUnpublishedPendingDrain hands the drain of the queue to the
// engine's GC pool — the pool that also collects workspace GC batches at
// rollback. The round-9 review pinned two defects of a synchronous
// Submit: the production GC pool is a five-worker blocking pool shared
// with other GC work, so a finalization scheduling with every worker
// occupied parked the commit itself inside ants' unbounded worker wait;
// and every handoff could stack another worker-occupying ladder for the
// same backlog. The schedule therefore coalesces — one pipeline per
// engine — and never waits on the caller's goroutine: the submission runs
// in a background goroutine, so Rollback and the commit finalizers return
// with the GC pool fully occupied and the parked submission starts the
// drain on the first freed worker. Whatever refuses the submission — a
// released pool, a missing engine dependency — leaves the names owned by
// the queue, and the GC-scheduler tick schedules the retry (issue #25590
// round-9 review).
func (e *Engine) scheduleUnpublishedPendingDrain() {
	e.unpublishedPending.mu.Lock()
	if e.unpublishedPending.drainScheduled || len(e.unpublishedPending.names) == 0 {
		e.unpublishedPending.mu.Unlock()
		return
	}
	e.unpublishedPending.drainScheduled = true
	// Each pipeline gets a join handle Close can wait on, and the engine
	// owns one cancellation context shared by every background drain: an
	// in-flight deletion must become abortable at shutdown instead of
	// outliving its own attempt timer (issue #25590 round-10 review).
	e.unpublishedPending.drainIdle = make(chan struct{})
	if e.unpublishedPending.drainCtx == nil {
		e.unpublishedPending.drainCtx, e.unpublishedPending.drainCancel =
			context.WithCancel(context.Background())
	}
	drainCtx := e.unpublishedPending.drainCtx
	e.unpublishedPending.mu.Unlock()
	e.gcMu.Lock()
	pool := e.gcPool
	closed := e.dynamicCtx.closed.Load()
	e.gcMu.Unlock()
	if pool == nil || closed || e.fs == nil || drainCtx == nil {
		e.clearUnpublishedPendingSchedule()
		logutil.Debug("WORKSPACE-UNPUBLISHED-PENDING-NOPOOL")
		return
	}
	go func() {
		if err := pool.Submit(func() { e.drainUnpublishedPending() }); err != nil {
			// The submission was refused: the names stay owned by the
			// queue, and the GC-scheduler tick or a later handoff
			// schedules the retry of the whole backlog.
			e.clearUnpublishedPendingSchedule()
			logutil.Warn(
				"WORKSPACE-UNPUBLISHED-CLEANUP-SCHED",
				zap.Error(err),
			)
		}
	}()
}

// clearUnpublishedPendingSchedule releases the coalescing slot so the next
// GC-scheduler tick or enqueue can schedule a fresh drain of the backlog,
// and closes the pipeline's join handle.
func (e *Engine) clearUnpublishedPendingSchedule() {
	e.unpublishedPending.mu.Lock()
	e.unpublishedPending.drainScheduled = false
	if e.unpublishedPending.drainIdle != nil {
		close(e.unpublishedPending.drainIdle)
		e.unpublishedPending.drainIdle = nil
	}
	e.unpublishedPending.mu.Unlock()
}

// cancelUnpublishedPendingDrains cancels the engine-owned context under
// which every background drain runs, so a deletion already blocked inside
// the file service aborts promptly instead of running out its own
// attempt timer while Close waits behind it (issue #25590 round-10
// review). Ownership is untouched: only successfully deleted names ever
// leave the queue.
func (e *Engine) cancelUnpublishedPendingDrains() {
	e.unpublishedPending.mu.Lock()
	cancel := e.unpublishedPending.drainCancel
	e.unpublishedPending.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// waitUnpublishedPendingDrains joins a drain pipeline that exists when
// shutdown begins, bounded by the cleanup budget: a drain that ignores
// the cancellation (no cooperative context in the storage path) must not
// hold Close longer than that.
func (e *Engine) waitUnpublishedPendingDrains(timeout time.Duration) {
	e.unpublishedPending.mu.Lock()
	idle := e.unpublishedPending.drainIdle
	e.unpublishedPending.mu.Unlock()
	if idle == nil {
		return
	}
	select {
	case <-idle:
	case <-time.After(timeout):
		logutil.Warn("WORKSPACE-UNPUBLISHED-PENDING-DRAIN-JOIN-TIMEOUT",
			zap.Duration("budget", timeout))
	}
}

// deleteQueuedUnpublishedPendingOnce attempts one bounded delete of the
// queued snapshot as INDEPENDENT batches — the unit shared by the
// asynchronous ladder and the shutdown-time drain. A batch the storage
// accepts is retired from the queue exactly; a batch whose deletion
// fails keeps exactly its own names and never walls the later queued
// names behind it, so one poisoned object can no longer keep every
// healthy name stranded from every retry and GC tick (issue #25590
// round-10 review) — the storage-side DeleteUnpublishedObjects stops at
// its own first failing batch, so the partitioning here is what restores
// per-batch liveness. The return is nil only when every batch succeeded.
//
// round-37 (issue #25590 review of 2026-10-24, finding 2): every
// successful batch's names accumulate and ONE in-place compaction below
// removes them all under a single mutex section, replacing the
// per-batch rebuild that cloned and rescanned the WHOLE remaining queue
// per 1,000-name batch — quadratic work at the GC worker's pace that
// held the mutex shared with admission and ownership transfers for
// seconds at the 500,000-name bound. The deferral changes no contract:
// the membership index keeps each done name until the compaction, so a
// duplicate delivery mid-invocation dedupes against the still-visible
// member exactly as before, a concurrent enqueue only ever APPENDS to
// the queue (the arrivals never appear in this snapshot and survive the
// compaction untouched), and every name the storage still refuses keeps
// its place exactly.
func (e *Engine) deleteQueuedUnpublishedPendingOnce(ctx context.Context) error {
	fs := e.fs
	if fs == nil {
		return moerr.NewInternalErrorNoCtx("file service is unavailable")
	}
	e.unpublishedPending.mu.Lock()
	pending := slices.Clone(e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	var (
		failed error
		done   []string
	)
	for start := 0; start < len(pending); start += unpublishedPendingDeleteBatch {
		end := min(start+unpublishedPendingDeleteBatch, len(pending))
		batch := pending[start:end]
		// The engine outlives the transaction: the delete runs on the given
		// context and keeps no transaction state alive.
		if _, err := ioutil.DeleteUnpublishedObjects(ctx, fs, batch...); err != nil {
			logutil.Warn(
				"WORKSPACE-UNPUBLISHED-PENDING-KEEP",
				zap.Strings("files", batch),
				zap.Error(err),
			)
			if failed == nil {
				failed = err
			}
			continue
		}
		done = append(done, batch...)
	}
	if len(done) > 0 {
		e.unpublishedPending.mu.Lock()
		settledOwners := e.retireQueuedUnpublishedPendingLocked(done)
		e.unpublishedPending.mu.Unlock()
		// The settle ended every cleanup ownership the recorded copies
		// of these names were protecting — the files are gone for good —
		// so the drain retires the owning recorded-name holders' members
		// in the same worker step, through the recorded-name guard
		// alone and NO transaction lock (issue #25590 round-38 review,
		// holder-keyed by round-60): the open transaction that keeps
		// failing statements keeps its set at the LIVE obligations,
		// exactly the round-37 gcFiles rule, now on the queue's settle
		// too.
		for holder, holderNames := range settledOwners {
			holder.retireSettled(holderNames)
		}
	}
	return failed
}

// retireQueuedUnpublishedPendingLocked removes from the queue exactly the
// names this invocation's batches deleted, in ONE in-place compaction over
// the live slice instead of a per-batch rebuild of the whole backlog
// (issue #25590 round-37 review, finding 2): the survivors keep their
// queue order, the membership index drops every retired name in the same
// pass — a later delivery of the same name is accepted again exactly as
// after the per-batch shape — and one O(live+done) hold of the queue
// mutex replaces the per-batch O(live) holds admission used to wait
// behind. The removal is by NAME against the LIVE slice: a copy of a
// retired name that a fresh delivery appended between the compactions of
// two invocations was already deduped by the still-indexed member until
// this pass, and the file its producer names was just deleted by this
// very drain, so the queue keeps receiving exactly one live member per
// existing obligation. Callers hold the queue mutex.
//
// round-38 (issue #25590 review of 2026-10-24, finding 1): the same pass
// pops every retired name's recordedOwners entry and returns the
// per-holder done batches, so the caller can retire the owning holder's
// recorded-name copy in the same settle — the queue-side handoffs of
// STILL-RECORDED names carried no gcFiles worker whose settle could
// retire the copy before, and an open transaction would keep the whole
// failed-statement history in its recorded set until termination
// despite the empty ledger, empty queue and deleted files. The map is
// returned, never applied here: the retire takes the recorded-name
// guard WITHOUT the queue mutex.
func (e *Engine) retireQueuedUnpublishedPendingLocked(
	done []string,
) map[*txnRecordedUnpublished][]string {
	doneSet := make(map[string]struct{}, len(done))
	for _, n := range done {
		doneSet[n] = struct{}{}
	}
	var settledOwners map[*txnRecordedUnpublished][]string
	kept := e.unpublishedPending.names[:0]
	for _, n := range e.unpublishedPending.names {
		if _, ok := doneSet[n]; ok {
			if e.unpublishedPending.nameSet != nil {
				delete(e.unpublishedPending.nameSet, n)
			}
			if owner, ok := e.unpublishedPending.recordedOwners[n]; ok {
				delete(e.unpublishedPending.recordedOwners, n)
				if settledOwners == nil {
					settledOwners = make(map[*txnRecordedUnpublished][]string)
				}
				settledOwners[owner] = append(settledOwners[owner], n)
			}
			continue
		}
		kept = append(kept, n)
	}
	e.unpublishedPending.names = kept
	return settledOwners
}

// drainUnpublishedPending deletes the queued backlog with a bounded
// retry ladder; only a successful delete removes a batch, and every
// name the ladder does not delete stays owned by the queue. The round-9
// review pinned that this task was the queue's only trigger: once the
// ladder exhausted on a dirty file service or the submission was refused,
// nothing but a new orphan's enqueue could ever schedule the deletions
// again. The engine's GC scheduler now schedules the drain on every tick,
// so storage recovery alone completes the cleanup — this ladder first,
// the tick after it. The round-10 review added that the ladder must run
// inside the engine's own cancellation context with an overall budget —
// Close cancels the context, so a deletion already blocked inside the
// file service aborts promptly and the worker returns, instead of
// holding the pool worker until its attempt timer expires while Close
// waits behind it (issue #25590 round-10 review).
func (e *Engine) drainUnpublishedPending() {
	// The coalescing slot is free again when the task ends. A backlog that
	// survived the ladder stays queued for the GC-scheduler tick; a
	// success reschedules immediately for the names a concurrent enqueue
	// parked behind this drain's coalescing slot.
	resched := false
	defer func() {
		e.clearUnpublishedPendingSchedule()
		if resched {
			e.scheduleUnpublishedPendingDrain()
		}
	}()
	e.unpublishedPending.mu.Lock()
	drainCtx := e.unpublishedPending.drainCtx
	e.unpublishedPending.mu.Unlock()
	if e.fs == nil || drainCtx == nil {
		return
	}
	ctx, cancel := context.WithTimeout(drainCtx, unpublishedPendingDrainBudget)
	defer cancel()
	delay := 25 * time.Millisecond
	for round := 0; round < unpublishedPendingDrainRounds; round++ {
		if e.dynamicCtx.closed.Load() || ctx.Err() != nil {
			return
		}
		if err := e.deleteQueuedUnpublishedPendingOnce(ctx); err != nil {
			if ctx.Err() != nil {
				// Canceled or budget-spent: hands the names back to the
				// queue for the tick after shutdown or the tick after the
				// budget.
				return
			}
			time.Sleep(delay)
			if delay < time.Second {
				delay *= 2
			}
			continue
		}
		e.unpublishedPending.mu.Lock()
		remaining := len(e.unpublishedPending.names) > 0
		e.unpublishedPending.mu.Unlock()
		resched = remaining
		return
	}
	// The ladder ran out on a still-failing file service: the names stay
	// owned by the queue — the engine's registry — until the GC-scheduler
	// tick schedules the next attempt.
	logutil.Warn(
		"WORKSPACE-UNPUBLISHED-PENDING-HELD",
		zap.Int("names", func() int {
			e.unpublishedPending.mu.Lock()
			defer e.unpublishedPending.mu.Unlock()
			return len(e.unpublishedPending.names)
		}()),
	)
}

// finalizePendingObjectCleanup is the one terminal owner of the proven-
// unpublished storage-object list, shared by every transaction-destruction
// path: Rollback, FinalizeCommit and FinalizeCommitWithUnknownResult
// (issue #25590 round-7 review). The list only ever holds objects whose
// publication failed before any workspace entry recorded them, so no
// commit payload — even one that already reached TN under an unknown
// result — can ever reference a name on it, and handing the names over
// under every finalization path is always safe. The round-11 bound ran
// one synchronous deletion attempt on a short overall budget before the
// transfer; the round-12 review (issue #25590) pinned that this still
// parked the transaction's final moment whenever the storage ignores
// cancellation — the LocalFS deleteSingle shape performs synchronous
// filesystem work no deadline or the 10-minute sinker bound can move —
// and behind it, through rollbackOwned, the transaction locks. The
// terminal round therefore performs NO storage I/O at all: the
// ownership transfer alone — a mutex-guarded queue append that never
// touches the file service — carries every name to the engine, and the
// engine's own scheduled drain performs every deletion afterwards.
// Transaction destruction thus terminates independently of any storage
// fate, for an alive caller and an already-dead caller alike, and after
// delTransaction nothing of this transaction exists to retry with — the
// queue is the only owner left.
func (txn *Transaction) finalizePendingObjectCleanup() {
	txn.handoffPendingObjectCleanup()
}

func (txn *Transaction) dumpBatchLocked(ctx context.Context, offset int) error {
	var size uint64
	var pkCount int

	// Orphans kept from a failed publication transfer here to the engine's
	// lossless queue, before the gates below can return early — commit
	// included (issue #25590 round-14 review). A dump attempt is part of
	// statement admission under the transaction's own locks, so the
	// transfer must not touch the file service: the engine's scheduled
	// drain retries every retained deletion with the r10 ladder and
	// GC-tick machinery, which keeps a stalled file service from parking
	// statement admission — or behind it the request's cancellation —
	// while the ownership, the accounting and every retry obligation move
	// to the queue in one section. The r5 boundary classification (round-
	// 21 review, finding G1) rides the same call: a mid-statement dump
	// (offset >= 0, the statement still executing) moves only entries
	// older than the recording statement plus the recording consumer's
	// definite orphans — an in-flight publication of the current epoch
	// cannot be orphaned yet — while the commit-prepare dump (offset < 0,
	// every statement quiesced) hands the whole classified list over.
	txn.handoffPendingObjectCleanupLocked(offset < 0)

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
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	if offset < txn.adjustWriteOffset {
		txn.adjustWriteOffset = offset
	}
	txn.hasS3Op.Store(true)

	var (
		err error
		fs  fileservice.FileService
	)

	if fs, err = colexec.GetSharedFSFromProc(txn.proc); err != nil {
		return err
	}

	// Admission control in OBJECT units over additional persistence
	// (issue #25590 round-11/12/18 reviews): the gates above already
	// returned the workspaces that dump nothing, so only dumps that
	// will really persist reach this point. The writers below carry
	// the engine's per-object capacity hook: every object a spilling
	// writer is about to persist reserves one in-flight unit under the
	// admission bound before it persists, a refusal aborts that spill
	// before anything persists, the record choke converts successful
	// reservations into retained ownership, and the failed-publication
	// epilogue sweeps the rest - no writer can hold a unit beyond its
	// dump's end. A dump can no longer push the ledger past the bound
	// by spilling several objects behind one unit, concurrent writers
	// share the remaining capacity through their per-name reservations,
	// and no name is dropped by a refusal: every owner keeps exactly
	// what it owns, only the stream that would add more obligations
	// stops, and it resumes the moment a drain retires work.

	if err := txn.dumpInsertBatchLocked(ctx, fs, offset, &pkCount); err != nil {
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
			if err := txn.dumpDeleteBatchLocked(ctx, fs, offset); err != nil {
				return err
			}
			//After flushing inserts/deletes in memory into S3, the entries in txn.writes will be unordered,
			//should adjust the order to make sure deletes are in front of the inserts.
			if err := txn.adjustUpdateOrderLocked(0); err != nil {
				return err
			}
		}
		txn.pkCount -= pkCount
		// modifies txn.writes.
		writes := txn.writes[:0]
		for i, write := range txn.writes {
			if write.bat != nil {
				writes = append(writes, txn.writes[i])
			}
		}
		// Clear the vacated slots through the original full-length slice:
		// the shortened append target writes cannot index past its own
		// length (Go slices panic on len, not on cap) whenever this
		// compaction dropped at least one entry.
		clearVacatedWriteSlots(txn.writes, len(writes), len(txn.writes))
		txn.writes = writes
	} else {
		txn.pkCount -= pkCount
	}
	txn.assertWorkspaceAccountingLocked()
	return nil
}

// dumpInsertTableSelectionLocked sizes every dumpable in-memory insert table
// from writes[offset:], for per-table spill selection. The per-table size is
// the same in-memory total the workspace budget charges (inMemEntryDataSize:
// batch plus hydrated key-index bytes), so a table retained above the
// spill thresholds cannot be selected as if it were small enough to stay.
// Tables come back sorted ascending by size; skipTable holds the tables
// small enough to stay in memory under the commit threshold, so the commit
// dump spills only what thresholds demand.
func (txn *Transaction) dumpInsertTableSelectionLocked(
	offset int,
	forceFlush bool,
) (
	tbSize map[uint64]int,
	tbCount map[uint64]int,
	skipTable map[uint64]bool,
) {
	tbSize = make(map[uint64]int)
	tbCount = make(map[uint64]int)
	skipTable = make(map[uint64]bool)

	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].isCatalog() {
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			tbSize[txn.writes[i].tableId] += int(inMemEntryDataSize(&txn.writes[i]))
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
	return tbSize, tbCount, skipTable
}

func (txn *Transaction) dumpInsertBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	offset int,
	pkCount *int,
) error {

	// Check if force flush is enabled
	forceFlush := objectio.CNWorkspaceForceFlushInjected()

	_, _, skipTable := txn.dumpInsertTableSelectionLocked(offset, forceFlush)

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

	// Stage the dump candidates WITHOUT retiring them: the sources stay
	// live — they back the staged views the publication below writes out —
	// and each one is retired only after its replacement file entries are
	// recorded (issue #25590). A storage failure mid-publication must
	// leave the earlier successful statements' rows available to the
	// statement rollback the frontend issues on the admission error,
	// instead of having staged them away before the failure.
	mp := make(map[workspaceTableKey][]*batch.Batch)
	mpIdxs := make(map[workspaceTableKey][]int)
	mpRows := make(map[workspaceTableKey]int)
	for i := offset; i < len(txn.writes); i++ {
		if skipTable[txn.writes[i].tableId] {
			continue
		}
		if txn.writes[i].isCatalog() {
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].typ != INSERT || txn.writes[i].fileName != "" {
			continue
		}

		tbKey := workspaceTableKey{
			tableKey: tableKey{
				accountId:  txn.writes[i].accountId,
				databaseId: txn.writes[i].databaseId,
				dbName:     txn.writes[i].databaseName,
				name:       txn.writes[i].tableName,
			},
			autoIncrEpoch:      txn.writes[i].autoIncrEpoch,
			autoIncrEpochKnown: txn.writes[i].autoIncrEpochKnown,
		}
		// Tables not resolved in the pre-resolution pass were appended
		// to the workspace while the lock was released; keep them in
		// memory for the next dump instead of calling getTable with
		// the lock held.
		if _, ok := tables[tbKey.tableKey]; !ok {
			continue
		}
		bat := txn.writes[i].bat
		// skip rowid
		newBatch := batch.NewWithSize(len(bat.Vecs) - 1)
		newBatch.SetAttributes(bat.Attrs[1:])
		newBatch.Vecs = bat.Vecs[1:]
		newBatch.SetRowCount(bat.Vecs[0].Length())
		mp[tbKey] = append(mp[tbKey], newBatch)
		mpIdxs[tbKey] = append(mpIdxs[tbKey], i)
		mpRows[tbKey] += bat.RowCount()
	}
	// Entries appended at or after stagedEnd — the block-info entries a
	// publication records — are not eligible for this dump round and must
	// survive the retirement compaction below.
	stagedEnd := len(txn.writes)

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

	// publishTable flushes one staged table out and records its replacement
	// file entries. It is fallible without side effects on staging: on a
	// pre-recording error, exactly this table's publication is abandoned
	// and nobody touches the workspace before the epilogue below. A
	// published report means the replacement entry is recorded — such a
	// table retires its sources even if a post-recording error follows.
	publishTable := func(tbKey workspaceTableKey) (published bool, err error) {
		defer func() {
			if published || err == nil || s3Writer == nil {
				return
			}
			// This publication stopped before its replacement file entry
			// was recorded, so its storage objects are recorded nowhere:
			// the file entry the workspace GC walks was the recording, and
			// s3Writer.Close below drops the sinker's persisted references
			// without deleting the files. Take their names over here — the
			// only remaining owner — and delete NOTHING on this path: this
			// epilogue runs inside the statement's admission with the
			// transaction lock held, and a file service whose deletion a
			// stalled context cannot move must not park the statement, the
			// statements behind the lock, or rollback with it (issue
			// #25590 round-15 review). The names transfer to the
			// asynchronous cleanup owner: the transaction lists them and
			// counts them into the engine's live-orphan accounting now, and
			// the next statement boundary's handoff — or the terminal one —
			// moves them to the engine queue whose own drain deletes them.
			files := s3Writer.PersistedNames()
			s3Writer.Close()
			s3Writer = nil
			if len(files) > 0 {
				// r5 (round-21 review): a failed dump epilogue's names are
				// DEFINITE orphans — the publication failed and the retry
				// replaces them with fresh objects, so no recording consumer
				// can ever land for these names — they keep the round-14
				// immediate-transfer semantics (retainedIfRecordingLater
				// false).
				for _, n := range files {
					txn.pendingObjectCleanup = append(txn.pendingObjectCleanup,
						pendingObjectCleanupItem{name: n})
				}
				// The names enter the engine's live-orphan accounting the
				// admission bound consults: orphans still owned by an
				// ACTIVE transaction are part of the retained backlog
				// long before any handoff, or an explicit transaction's
				// repeated failures could grow its own backlog without
				// limit while the engine queue stays empty (issue #25590
				// round-12 review). The count joins, like every other
				// accounting transition, under the queue's mutex (issue
				// #25590 round-13 review). Round 18 pairs the retention
				// with its reservations: every persisted name below
				// consumed one per-object capacity reservation when its
				// file sinker spilled, so the same one-section move
				// retires those reservations and retains the names —
				// the ledger never keeps a reservation whose dump has
				// ended, and never counts a name whose spilling writer
				// was never admitted (issue #25590 round-18 review).
				if txn.engine != nil {
					txn.convertDumpedReservationsLocked(files)
				}
				logutil.Warn(
					"WORKSPACE-PENDING-CLEANUP-RETAKE",
					zap.Strings("files", files),
					zap.String("reason", "publication failed: the statement path performs no deletion (issue #25590 round-15 review)"),
					zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
				)
			}
		}()
		if objectio.CNDumpInsertWriteFailedInjected() {
			return false, moerr.NewInternalErrorNoCtx(
				"fault injection: insert dump write failed")
		}

		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl := tables[tbKey.tableKey]

		tableDef := tbl.GetTableDef(txn.proc.Ctx)
		s3Writer = colexec.NewCNS3DataWriterForService(
			txn.proc.GetService(), txn.proc.GetMPool(), fs, tableDef,
			workspaceDumpWriteS3Threshold, false, txn.dumpSinkerCapacityOption(),
		)

		for _, bat = range mp[tbKey] {
			if err = s3Writer.Write(txn.proc.Ctx, bat); err != nil {
				return false, err
			}
		}

		if objectio.CNDumpInsertSyncFailedInjected() {
			return false, moerr.NewInternalErrorNoCtx(
				"fault injection: insert dump sync failed")
		}
		if stats, err = s3Writer.Sync(txn.proc.Ctx); err != nil {
			return false, err
		}

		fileName = stats[0].ObjectLocation().String()
		if bat, err = s3Writer.FillBlockInfoBat(); err != nil {
			return false, err
		}

		var table *txnTable
		if v, ok := tbl.(*txnTableDelegate); ok {
			table = v.origin
		} else {
			table = tbl.(*txnTable)
		}

		if err = table.getTxn().writeFileLockedWithAutoIncrEpochKnown(
			INSERT,
			table.accountId,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			bat,
			table.getTxn().tnStores[0],
			tbKey.autoIncrEpoch,
			tbKey.autoIncrEpochKnown,
		); err != nil {
			return false, err
		}

		s3Writer.Close()

		s3Writer = nil
		return true, nil
	}

	published := make(map[int]bool)
	var publishErr error
	for tbKey := range mp {
		publishedNow, perr := publishTable(tbKey)
		if publishedNow {
			for _, idx := range mpIdxs[tbKey] {
				published[idx] = true
			}
			// count rows that actually left the in-memory workspace; the
			// rows of unpublished tables never leave it.
			*pkCount += mpRows[tbKey]
		}
		if perr != nil {
			publishErr = perr
			break
		}
	}

	// Retire exactly the published sources, compacting in place: every
	// other entry — an unpublished staging's source, or one a publication
	// appended — stays raw and accounted, so the workspace a failed dump
	// leaves behind is the one the statements actually produced.
	if len(published) > 0 {
		writes := txn.writes
		w := offset
		for i := offset; i < stagedEnd; i++ {
			if published[i] {
				// The published source leaves the workspace entirely:
				// refund its hydrated key index through the same
				// batching-mutation choke point the shrink and entry
				// release paths use before its batch is cleaned
				// (issue #26796 ownership).
				txn.unaccountInsertKeyOffsetsLocked(&writes[i])
				txn.unaccountWorkspaceEntryLocked(&writes[i])
				writes[i].bat.Clean(txn.proc.GetMPool())
				continue
			}
			writes[w] = writes[i]
			w++
		}
		if drop := stagedEnd - w; drop > 0 {
			copy(writes[w:], writes[stagedEnd:])
			txn.writes = writes[:len(writes)-drop]
			// Drop the vacated tail's stale Entry references through the
			// original full-length slice so the truncated workspace cannot
			// pin retired batches (same contract as clearVacatedWriteSlots
			// callers on main, issue #26796).
			clearVacatedWriteSlots(txn.writes, len(txn.writes), len(writes))
		}
	}

	return publishErr
}

func (txn *Transaction) dumpDeleteBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	offset int,
) error {

	// See the comment in dumpInsertBatchLocked: tables must be resolved
	// while txn.writes is still consistent, with the lock released.
	tables, ok, err := txn.resolveDumpTablesLocked(ctx, offset, nil, DELETE)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	// Stage the dump candidates WITHOUT retiring them, mirroring
	// dumpInsertBatchLocked (issue #25590): delete sources stay live until
	// their replacement tombstone entries are published, so a storage
	// failure mid-publication never drops deletes that succeeded earlier.
	mp := make(map[workspaceTableKey][]*batch.Batch)
	mpIdxs := make(map[workspaceTableKey][]int)
	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].isCatalog() {
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].typ != DELETE || txn.writes[i].fileName != "" {
			continue
		}

		tbKey := workspaceTableKey{
			tableKey: tableKey{
				accountId:  txn.writes[i].accountId,
				databaseId: txn.writes[i].databaseId,
				dbName:     txn.writes[i].databaseName,
				name:       txn.writes[i].tableName,
			},
			autoIncrEpoch:      txn.writes[i].autoIncrEpoch,
			autoIncrEpochKnown: txn.writes[i].autoIncrEpochKnown,
		}
		// Tables not resolved in the pre-resolution pass were appended
		// to the workspace while the lock was released; keep them in
		// memory for the next dump instead of calling getTable with
		// the lock held.
		if _, ok := tables[tbKey.tableKey]; !ok {
			continue
		}
		bat := txn.writes[i].bat

		newBat := batch.NewWithSize(len(bat.Vecs))
		newBat.SetAttributes(bat.Attrs)
		newBat.Vecs = bat.Vecs
		newBat.SetRowCount(bat.Vecs[0].Length())

		mp[tbKey] = append(mp[tbKey], newBat)
		mpIdxs[tbKey] = append(mpIdxs[tbKey], i)
	}
	// Entries appended at or after stagedEnd — the tombstone entries a
	// publication records — are not eligible for this dump round and must
	// survive the retirement compaction below.
	stagedEnd := len(txn.writes)

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

	// publishTable flushes one staged table's tombstones out and records
	// the replacement entries. It is fallible without side effects on
	// staging, mirroring dumpInsertBatchLocked (issue #25590): a
	// pre-recording error abandons exactly this table's publication. Once
	// the replacement entry is recorded the table counts as published —
	// a Close error after that point cannot retire to its sources without
	// double-applying the tombstones at commit, so the sources retire and
	// the dump still reports the error.
	publishTable := func(tbKey workspaceTableKey) (published bool, err error) {
		defer func() {
			if published || err == nil || s3Writer == nil {
				return
			}
			// Mirrors the insert path: a publication stopped before its
			// replacement file entry was recorded leaves storage objects
			// that no workspace slot can ever collect — Close drops the
			// sinker's persisted references without deleting the files —
			// so the failed writer hands its objects' names over here and
			// deletes NOTHING on this path, which runs inside the
			// statement's admission under the transaction lock: a stalled
			// context-blind deletion must park neither this statement nor
			// rollback (issue #25590 round-15 review). The names transfer
			// to the asynchronous cleanup owner — the transaction lists
			// them and counts them into the engine's live-orphan
			// accounting now, and the next statement boundary's handoff —
			// or the terminal one — moves them to the engine queue whose
			// own drain deletes them. A recorded publication returns
			// published=true even with a Close error, so legitimately
			// recorded objects are never touched by this cleanup.
			files := s3Writer.PersistedNames()
			s3Writer.Close()
			s3Writer = nil
			if len(files) > 0 {
				// r5 (round-21 review): a failed dump epilogue's names are
				// DEFINITE orphans — the publication failed and the retry
				// replaces them with fresh objects, so no recording consumer
				// can ever land for these names — they keep the round-14
				// immediate-transfer semantics (retainedIfRecordingLater
				// false).
				for _, n := range files {
					txn.pendingObjectCleanup = append(txn.pendingObjectCleanup,
						pendingObjectCleanupItem{name: n})
				}
				// The names enter the engine's live-orphan accounting the
				// admission bound consults: orphans still owned by an
				// ACTIVE transaction are part of the retained backlog
				// long before any handoff, or an explicit transaction's
				// repeated failures could grow its own backlog without
				// limit while the engine queue stays empty (issue #25590
				// round-12 review). The count joins, like every other
				// accounting transition, under the queue's mutex (issue
				// #25590 round-13 review). Round 18 pairs the retention
				// with its reservations: every persisted name below
				// consumed one per-object capacity reservation when its
				// file sinker spilled, so the same one-section move
				// retires those reservations and retains the names —
				// the ledger never keeps a reservation whose dump has
				// ended, and never counts a name whose spilling writer
				// was never admitted (issue #25590 round-18 review).
				if txn.engine != nil {
					txn.convertDumpedReservationsLocked(files)
				}
				logutil.Warn(
					"WORKSPACE-PENDING-CLEANUP-RETAKE",
					zap.Strings("files", files),
					zap.String("reason", "publication failed: the statement path performs no deletion (issue #25590 round-15 review)"),
					zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
				)
			}
		}()
		if objectio.CNDumpDeleteWriteFailedInjected() {
			return false, moerr.NewInternalErrorNoCtx(
				"fault injection: delete dump write failed")
		}

		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl := tables[tbKey.tableKey]

		pkCol = plan2.PkColByTableDef(tbl.GetTableDef(txn.proc.Ctx))
		s3Writer = colexec.NewCNS3TombstoneWriterForService(
			txn.proc.GetService(), txn.proc.GetMPool(), fs,
			plan2.ExprType2Type(&pkCol.Typ), workspaceDumpWriteS3Threshold,
			txn.dumpSinkerCapacityOption(),
		)

		for i := 0; i < len(mp[tbKey]); i++ {
			if err = s3Writer.Write(txn.proc.Ctx, mp[tbKey][i]); err != nil {
				return false, err
			}
		}

		if objectio.CNDumpDeleteSyncFailedInjected() {
			return false, moerr.NewInternalErrorNoCtx(
				"fault injection: delete dump sync failed")
		}
		if stats, err = s3Writer.Sync(txn.proc.Ctx); err != nil {
			return false, err
		}

		fileName = stats[0].ObjectLocation().String()

		if bat, err = s3Writer.FillBlockInfoBat(); err != nil {
			return false, err
		}

		var table *txnTable
		if v, ok := tbl.(*txnTableDelegate); ok {
			table = v.origin
		} else {
			table = tbl.(*txnTable)
		}

		if err = table.getTxn().writeFileLockedWithAutoIncrEpochKnown(
			DELETE,
			table.accountId,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			bat,
			table.getTxn().tnStores[0],
			tbKey.autoIncrEpoch,
			tbKey.autoIncrEpochKnown,
		); err != nil {
			return false, err
		}

		closeErr := s3Writer.Close()
		s3Writer = nil
		if objectio.CNDumpDeleteCloseFailedInjected() {
			closeErr = moerr.NewInternalErrorNoCtx(
				"fault injection: delete dump close failed")
		}
		return true, closeErr
	}

	published := make(map[int]bool)
	var publishErr error
	for tbKey := range mp {
		publishedNow, perr := publishTable(tbKey)
		if publishedNow {
			for _, idx := range mpIdxs[tbKey] {
				published[idx] = true
			}
		}
		if perr != nil {
			publishErr = perr
			break
		}
	}

	// Retire exactly the published sources, compacting in place, mirroring
	// dumpInsertBatchLocked: unpublished staging's sources stay raw and
	// accounted, as do the entries a publication appended.
	if len(published) > 0 {
		writes := txn.writes
		w := offset
		for i := offset; i < stagedEnd; i++ {
			if published[i] {
				// Same choke point as the insert path: DELETE entries
				// carry no exact-key index, and the refund below is the
				// documented no-op for them while clearing an INSERT
				// entry could never reach this delete-dump retirement.
				txn.unaccountInsertKeyOffsetsLocked(&writes[i])
				txn.unaccountWorkspaceEntryLocked(&writes[i])
				writes[i].bat.Clean(txn.proc.GetMPool())
				continue
			}
			writes[w] = writes[i]
			w++
		}
		if drop := stagedEnd - w; drop > 0 {
			copy(writes[w:], writes[stagedEnd:])
			txn.writes = writes[:len(writes)-drop]
			// Same vacated-tail contract as the insert dump's retirement
			// compaction (issue #26796): the truncated slice must not pin
			// the retired sources' cleaned batches.
			clearVacatedWriteSlots(txn.writes, len(txn.writes), len(writes))
		}
	}

	return publishErr
}

// resolveDumpTablesLocked resolves the engine.Relation of every table whose
// workspace entries (of the given type, not yet flushed to a file) a dump is
// about to flush. It must run BEFORE the dump mutates txn.writes: getTable
// may run internal SQL (Engine.Database -> execReadSql -> NewCompile) whose
// read pipeline locks the workspace, so the lock is released around the
// getTable calls, and that is only safe while the workspace is still
// consistent. The lock is re-acquired before returning, also on error.
//
// While the lock is released other goroutines may mutate the workspace. The
// caller must therefore treat the resolved map as the complete allowlist of
// what this dump may flush (entries of unresolved tables stay raw), and must
// give up when ok is false: a rollback truncated txn.writes below the dump
// offset during the window, so there is nothing left this dump round may
// safely touch.
func (txn *Transaction) resolveDumpTablesLocked(
	ctx context.Context,
	offset int,
	skipTable map[uint64]bool,
	typ int,
) (tables map[tableKey]engine.Relation, ok bool, err error) {
	var keys []tableKey
	seen := make(map[tableKey]bool)
	for i := offset; i < len(txn.writes); i++ {
		e := &txn.writes[i]
		if skipTable != nil && skipTable[e.tableId] {
			continue
		}
		if e.isCatalog() {
			continue
		}
		if e.bat == nil || e.bat.RowCount() == 0 {
			continue
		}
		if e.typ != typ || e.fileName != "" {
			continue
		}
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
		return tables, true, nil
	}

	txn.Unlock()
	for _, k := range keys {
		tbl, terr := txn.getTable(ctx, k.accountId, k.dbName, k.name)
		if terr != nil {
			txn.Lock()
			return nil, false, terr
		}
		tables[k] = tbl
	}
	// Test-only orchestration: park here (lock released) so a test can mutate
	// the workspace while the resolution window is open.
	objectio.CNDumpResolveWindowWait()
	txn.Lock()

	if offset > len(txn.writes) {
		// the workspace was truncated below the dump offset while the lock
		// was released (statement rollback): nothing to dump this round
		return nil, false, nil
	}
	return tables, true, nil
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

func physicalCatalogTableName(databaseId, tableId uint64, tableName string) string {
	if databaseId == catalog.MO_CATALOG_ID && tableId == catalog.MO_COLUMNS_ID &&
		tableName == catalog.MO_COLUMNS_UPDATE {
		return catalog.MO_COLUMNS
	}
	return tableName
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
	databaseId, tableId uint64,
	objBat *batch.Batch,
	dbName string,
	tbName string,
	autoIncrEpoch uint32,
	autoIncrEpochKnown bool,
) {
	txn.cnObjsSummary[objId] = Summary{
		objBat:             objBat,
		accountId:          accountId,
		databaseId:         databaseId,
		tableId:            tableId,
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

// stagedWorkspaceRecord holds one S3 metadata record between its
// complete fallible preparation and its infallible publication (issue
// #25590 round-20 reviews, findings 1 and 3). Every fallible step of the
// record — the epoch fence, the workspace's own batch copy, the record's
// admission choke — runs while STAGING, so a pass that publishes several
// records at once (object compaction) can prepare all of them before the
// first workspace mutation; a record in this shape has committed nothing
// its rollback could observe until publish runs.
type stagedWorkspaceRecord struct {
	typ                int
	accountId          uint32
	databaseId         uint64
	tableId            uint64
	databaseName       string
	tableName          string
	fileName           string
	bat                *batch.Batch
	tnStore            DNStore
	skipTransfer       bool
	autoIncrEpoch      uint32
	autoIncrEpochKnown bool

	// admittedNames are the distinct storage-object names the staged
	// record's choke registered. A back-out before publication retracts
	// them from the transaction's recorded-name set so the unwinding
	// retainer can hand every UNRECORDED persisted object to the cleanup
	// queue — the names would otherwise sit invisible to every
	// leftover-object enumeration while no workspace entry owns their
	// lifecycle (issue #25590 round-20 review, finding 1), and a name
	// whose copy failed after its registration would strand ownerless
	// (round-20 review, finding 3).
	admittedNames []string
}

// stageWorkspaceRecordLocked completes EVERY fallible step of an S3
// metadata record without mutating the workspace (issue #25590
// round-20 reviews, findings 1 and 3). Callers hold the transaction
// lock. The step order is part of the ownership contract: the
// workspace's own batch copy comes BEFORE the admission choke, so a
// copy failure leaves no recorded-name membership and consumes no
// reservation — the persisted objects a failed copy leaves behind stay
// reachable for the unwinding retainer and their in-flight units
// settle at the terminal drain exactly like an unrecorded writer's.
// retainFreshRecordPreparationFailureLocked is the record
// preparation's failed-side reachability guard (issue #25590 round-22
// review, finding H1): every early failure of the record's own
// preparation — the epoch fence, the keyed stage refusal, the batch
// copy — leaves the record's persisted objects behind with no one left
// to unwind them when their names carry no reservation on this side
// (the legacy-relayed class: its reservation sat on the fragment
// engine) and the record's choke therefore never runs its own refusal
// retain. The guard retains EXACTLY the record's extracted names that
// no reservation covers — the same fresh-name filter the choke's
// refusal retain applies one checkpoint later — so producers whose
// writers already reserved (every in-tree class) keep their r19
// epilogue and terminal-drain owners untouched, and a caller-specific
// fileName variant (a compaction delta location) never becomes a
// phantom cleanup member. CCPR holds the shared-file exemption.
// Callers hold the transaction lock.
func (txn *Transaction) retainFreshRecordPreparationFailureLocked(
	typ int, inputBat *batch.Batch, reason string,
) {
	if txn.isCCPRTxn {
		return
	}
	names := txn.recordedUnpublishedObjectNamesLocked(typ, inputBat)
	var fresh []string
	for _, n := range names {
		if n == "" || txn.hasUnpublishedDumpReservation(n) {
			continue
		}
		fresh = append(fresh, n)
	}
	txn.retainDefiniteOrphansLocked(fresh, reason)
}

func (txn *Transaction) stageWorkspaceRecordLocked(
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
	skipTransfer bool,
) (*stagedWorkspaceRecord, error) {
	if err := txn.requireAutoIncrEpochFenceCommit(autoIncrEpoch, autoIncrEpochKnown); err != nil {
		txn.retainFreshRecordPreparationFailureLocked(typ, inputBat,
			"record preparation failed at the epoch fence: the recording side fail-open-retains the reservation-less names so a remote fragment's persisted files keep their cleanup owner (issue #25590 round-22 review)")
		return nil, err
	}

	// Test-only refusal point for the staged record (issue #25590
	// round-20 review, finding 1): armed for the named table it fails a
	// record's preparation AFTER an earlier replacement of the same pass
	// already staged, with nothing consumed — the deterministic shape of
	// a pass whose LATER record fails while an earlier one is ready.
	if objectio.CNWorkspaceRecordStageFailedInjected(tableName) {
		txn.retainFreshRecordPreparationFailureLocked(typ, inputBat,
			"record staging refused by the keyed fault: the recording side fail-open-retains the reservation-less names so a remote fragment's persisted files keep their cleanup owner (issue #25590 round-22 review)")
		return nil, moerr.NewInternalErrorNoCtx(
			"fault injection: workspace record staging failed")
	}

	// The record's spill already made S3 part of this transaction's
	// commit path; the flag is monotonic and conservative, so setting it
	// before the remaining steps keeps a refused record flagged exactly
	// as before.
	txn.hasS3Op.Store(true)

	copied, err := inputBat.Dup(txn.proc.Mp())
	if err != nil {
		txn.retainFreshRecordPreparationFailureLocked(typ, inputBat,
			"record preparation failed at its own batch copy: the recording side fail-open-retains the reservation-less names so a remote fragment's persisted files keep their cleanup owner (issue #25590 round-22 review)")
		return nil, err
	}

	// Admission of the objects this record creates (issue #25590
	// round-18 review, finding 2): every name a rollback could later
	// hand to the engine's cleanup queue joins the capacity ledger
	// here — direct-S3 records and compaction rewrites are admitted at
	// creation with a fail-closed bound check, while the workspace
	// dump's per-object reservations convert into retained ownership.
	// A refusal cleans the prepared copy — the batch never entered the
	// workspace, so nothing else owns it — and fails this write with
	// the workspace untouched.
	if err := txn.registerUnpublishedObjectRecordLocked(typ, inputBat); err != nil {
		copied.Clean(txn.proc.Mp())
		return nil, err
	}

	return &stagedWorkspaceRecord{
		typ:                typ,
		accountId:          accountId,
		databaseId:         databaseId,
		tableId:            tableId,
		databaseName:       databaseName,
		tableName:          tableName,
		fileName:           fileName,
		bat:                copied,
		tnStore:            tnStore,
		skipTransfer:       skipTransfer,
		autoIncrEpoch:      autoIncrEpoch,
		autoIncrEpochKnown: autoIncrEpochKnown,
		admittedNames:      txn.recordedUnpublishedObjectNamesLocked(typ, inputBat),
	}, nil
}

// publishStagedWorkspaceRecordLocked appends one fully staged record to
// the workspace. Every fallible step already succeeded while staging,
// so this suffix touches only maps, engine state and the writes slice —
// a pass that publishes several staged records has no failure point
// between them, which is what makes compaction's replacement records
// atomic across the whole pass (issue #25590 round-20 review, finding
// 1). Callers hold the transaction lock.
func (txn *Transaction) publishStagedWorkspaceRecordLocked(s *stagedWorkspaceRecord) {
	if s == nil {
		return
	}
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()

	if s.typ == INSERT {
		server := colexec.MustGetServer(txn.engine.service)
		col, area := vector.MustVarlenaRawData(s.bat.Vecs[1])
		for i := range col {
			stats := objectio.ObjectStats(col[i].GetByteSlice(area))
			oid := stats.ObjectName().ObjectId()
			sid := oid.Segment()

			server.PutCnSegment(&txn.workspaceSegments, txn.op.Txn().ID,
				s.tableId, sid, colexec.TxnWorkspaceUnCommitType)
			txn.registerCNObjects(*oid, s.accountId, s.databaseId, s.tableId,
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

	txn.appendWorkspaceEntryLocked(Entry{
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
	})
	s.bat = nil // ownership passed to the workspace entry
}

// retractUnpublishedRecordAdmissionLocked removes the names a STAGED
// record's choke registered from the transaction's recorded-name set
// (issue #25590 round-20 review, finding 1). Called only from a
// back-out whose staged records were never published, it restores the
// pre-record state the unwinding retainer expects: every UNRECORDED
// name becomes visible to the cleanup ownership again, because no
// workspace entry exists to own its lifecycle either way. Every name
// this staged record admitted belongs to a freshly persisted
// replacement this same pass created (compaction rewrites brand-new
// names), and the transaction lock serializes the choke and the
// back-out, so no other record can have claimed the same names in
// between. The capacity ledger keeps its owned membership — that
// membership is exactly the cleanup obligation the queue's drain will
// exercise below, so counting never falls off the bound the refusal was
// admitted under.
func (txn *Transaction) retractUnpublishedRecordAdmissionLocked(st *stagedWorkspaceRecord) {
	if st == nil || len(st.admittedNames) == 0 {
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
	stagedRec, err := txn.stageWorkspaceRecordLocked(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, inputBat, tnStore,
		autoIncrEpoch, autoIncrEpochKnown, false)
	if err != nil {
		return err
	}
	txn.publishStagedWorkspaceRecordLocked(stagedRec)
	return nil
}

// dumpSinkerCapacityOption installs the engine's per-object admission hook
// on the dump's spilling writers: each object the sinker is about to
// persist reserves one in-flight capacity unit before anything goes
// durable, in OBJECT units rather than writer units (issue #25590
// round-18 review, finding 1), and the reservation is keyed by name on
// this transaction so the record choke — or the failed-publication
// epilogue — converts exactly the names the dump persisted.
// SinkerCapacityOption is this workspace's public sinker option — the
// binding an S3 writer constructed on this transaction's behalf needs to
// join the per-object admission before it persists (issue #25590
// round-19 review, finding 2): the pipeline's direct-S3 insert operator
// resolves thisTransaction through the transaction operator's workspace
// (the ioutil.SinkerOwner structural interface) and appends the option
// to its writer, so the record's choke is no longer the FIRST gate the
// objects pass: every spilled object reserves one in-flight unit under
// the bound before it goes durable, and a saturated ledger refuses the
// writer's spill with nothing half-persisted behind it.
func (txn *Transaction) SinkerCapacityOption() ioutil.SinkerOption {
	return txn.dumpSinkerCapacityOption()
}

func (txn *Transaction) dumpSinkerCapacityOption() ioutil.SinkerOption {
	hook := func(name string) error {
		if err := txn.engine.reserveUnpublishedPendingObject(name); err != nil {
			return err
		}
		txn.noteUnpublishedDumpReservation(name)
		return nil
	}
	return ioutil.WithUncommittedCapacityHook(
		ioutil.UncommittedCapacityHook(hook))
}

// hasUnpublishedDumpReservation reports whether a per-object reservation
// is outstanding for the name, under the map's own guard.
func (txn *Transaction) hasUnpublishedDumpReservation(name string) bool {
	txn.unpublishedDumpReservationsMu.Lock()
	defer txn.unpublishedDumpReservationsMu.Unlock()
	_, ok := txn.unpublishedDumpReservations[name]
	return ok
}

// AdoptRemoteTransferredObjectNames adopts the storage-object names a
// remote fragment relayed to this recording workspace (issue
// #25590 round-22 review, finding H1): the coordinator's transport
// client calls this at its intake — BEFORE forwarding the batch into
// the local pipeline and BEFORE the ack returns to the fragment — so an
// enumerable cleanup owner exists on the recording side from the moment
// the bytes leave the wire, earlier than the fragment's own terminal
// settlement. Every name is admitted through the SAME per-object
// reservation the writers use (finding H2): the engine's bound either
// counts the obligation now and the record choke converts it as a
// reserved name later, or the refusal propagates the error — the
// transport fails the run, and the fragment's own failure-side
// settlement — retention plus the non-transferred terminal rollback —
// keeps every still-local name reachable and deletes it (the recording
// side books nothing). A name whose record already landed keeps its
// record's own lifecycle; a repeated adoption is a no-op.
func (txn *Transaction) AdoptRemoteTransferredObjectNames(names []string) error {
	return txn.adoptRemoteTransferredObjectNames(names)
}

func (txn *Transaction) adoptRemoteTransferredObjectNames(
	names []string,
) error {
	for _, name := range names {
		if name == "" {
			continue
		}
		txn.Lock()
		recorded := txn.hasRecordedUnpublishedObjectName(name)
		txn.Unlock()
		if recorded || txn.hasUnpublishedDumpReservation(name) {
			continue
		}
		if txn.engine == nil {
			continue
		}
		if err := txn.engine.reserveUnpublishedPendingObject(name); err != nil {
			return err
		}
		txn.noteUnpublishedDumpReservation(name)
	}
	return nil
}

// noteUnpublishedDumpReservation records the name's outstanding
// per-object reservation. The hook fires on sink-pool and compaction
// worker goroutines that hold NO transaction lock, so this uses the map's
// own guard; a dump's hook under the transaction lock is single-threaded
// but takes the same guard, keeping one lock order everywhere (issue
// #25590 round-19 review).
func (txn *Transaction) noteUnpublishedDumpReservation(name string) {
	txn.unpublishedDumpReservationsMu.Lock()
	defer txn.unpublishedDumpReservationsMu.Unlock()
	if txn.unpublishedDumpReservations == nil {
		txn.unpublishedDumpReservations = make(map[string]struct{})
	}
	txn.unpublishedDumpReservations[name] = struct{}{}
}

// recordedUnpublishedObjectNamesLocked extracts the DISTINCT storage-object
// names a record entry carries — the record layout holds one object-stats
// row per block, so the same object can appear several times and dedup
// keeps ONE ledger member per object — or an empty set when the record
// creates no objects. Callers hold the transaction lock.
func (txn *Transaction) recordedUnpublishedObjectNamesLocked(
	typ int, bat *batch.Batch,
) []string {
	if bat == nil || bat.Vecs == nil {
		return nil
	}
	var vec *vector.Vector
	switch typ {
	case INSERT:
		vec = bat.Vecs[1] // [%!%mo__meta_loc, object_stats]
	case DELETE:
		vec = bat.Vecs[0] // [object_stats, pk]
	default:
		return nil
	}
	if vec == nil || vec.Length() == 0 {
		return nil
	}
	col, area := vector.MustVarlenaRawData(vec)
	var (
		names []string
		seen  map[string]struct{}
	)
	for i := range col {
		stats := objectio.ObjectStats(col[i].GetByteSlice(area))
		name := stats.ObjectName().String()
		if seen == nil {
			seen = make(map[string]struct{}, vec.Length())
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

// registerUnpublishedObjectRecordLocked is the single admission choke of
// every recorded workspace object (issue #25590 round-18 review): when the
// transaction's spilling dump holds per-object reservations for these
// names — the writer's capacity hook reserved each object before it
// persisted — the names CONVERT those reservations into retained
// ownership in one atomic move; every other record creator (the
// direct-S3 metadata branches, compaction rewrites, transferred records)
// is admitted at CREATION with the fail-closed bound check, so names a
// ROLLBACK could hand to the queue can never pass behind the advertised
// limit. Callers hold the transaction lock.
func (txn *Transaction) registerUnpublishedObjectRecordLocked(
	typ int, bat *batch.Batch,
) error {
	names := txn.recordedUnpublishedObjectNamesLocked(typ, bat)
	if len(names) == 0 {
		return nil
	}
	e := txn.engine
	if e == nil {
		return nil
	}
	// Split the record's names by outstanding reservations: a
	// reservation is converted (one in-flight unit retires per name), a
	// name without one is admitted fail-closed at creation. Admitting
	// keeps refusal failure-atomic — the reserved names stay
	// outstanding until the admission has succeeded and are deleted
	// only in the same move that converts them, so the failed dump's
	// epilogue (or the terminal drain) still settles exactly the names
	// this dump persisted. The map carries its own guard since the
	// round-19 hook fires on writer goroutines without the transaction
	// lock; the engine's queue itself stays the capacity
	// linearization point.
	var reserved, fresh []string
	txn.unpublishedDumpReservationsMu.Lock()
	for _, n := range names {
		if _, ok := txn.unpublishedDumpReservations[n]; ok {
			reserved = append(reserved, n)
		} else {
			fresh = append(fresh, n)
		}
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	if len(fresh) > 0 {
		if err := e.admitUnpublishedPendingRecordedNames(txn, fresh); err != nil {
			// The refusal consumed no reservations; the split above was
			// read-only, so nothing needs to be restored and the reserved
			// names stay exactly as the failed dump's epilogue expects
			// them.
			// r5 (issue #25590 round-21 review, finding G2): the refused
			// input names that carry NO LOCAL reservation are the
			// remote-fragment class — no local writer exists to unwind and
			// re-assert them, so without this fail-open retain a clean
			// remote fragment whose metadata the recording side refused
			// would leave physical files no owner can reach (the fragment
			// already released them through its transferred settlement).
			// They are definite orphans here: the refusal failed this very
			// record, and a retry publishes fresh objects. CCPR records
			// keep their shared-file exemption — the txn cache owns those
			// shares — and local hooked producers are untouched: their
			// names carry reservations and stay with the epilogue/unwind
			// semantics exactly as r18 pinned.
			if !txn.isCCPRTxn {
				txn.retainDefiniteOrphansLocked(fresh,
					"record refused at the admission bound: the recording side fail-open-retains the refused reservation-less names so a remote fragment's persisted files never lose their cleanup owner (issue #25590 round-21 review)")
			}
			return err
		}
	}
	if len(reserved) > 0 {
		txn.unpublishedDumpReservationsMu.Lock()
		for _, n := range reserved {
			delete(txn.unpublishedDumpReservations, n)
		}
		txn.unpublishedDumpReservationsMu.Unlock()
		e.convertUnpublishedPendingReservations(txn, reserved)
	}
	txn.mergeRecordedUnpublishedObjectNames(names)
	return nil
}

// mergeRecordedUnpublishedObjectNames keeps the transaction's own copy of
// the recorded names so its terminal paths can retire exactly its
// obligations: commit releases what is still owned, rollback's queue
// handoff already moves the members, and stale members that intermediate
// transfers retired earlier simply no-op on release.
func (txn *Transaction) mergeRecordedUnpublishedObjectNames(names []string) {
	if len(names) == 0 {
		return
	}
	if txn.recordedUnpublishedObjects == nil {
		// Only workspaces built outside NewTxnWorkSpace reach here —
		// production installs the holder at construction. The lazy
		// install runs on the transaction's own goroutine (the record
		// choke holds the transaction lock), before any producer can
		// hand a member to the gcFiles worker or the queue drain, so
		// the holder pointer reads on every later path stay ordered
		// behind this write (issue #25590 round-41 review).
		txn.recordedUnpublishedObjects = &txnRecordedUnpublished{}
	}
	txn.recordedUnpublishedObjects.merge(names)
}

// hasRecordedUnpublishedObjectName reports whether the name's record landed
// on this transaction — the record's lifecycle owns the object's fate, so
// the retain funnels, the pending-cleanup handoff and the remote adoption
// all skip it. Callers hold the transaction lock; the read takes the
// recorded-name guard so its answer stays consistent with the gcFiles
// settle that can retire the copy without the transaction lock
// (issue #25590 round-37 review).
func (txn *Transaction) hasRecordedUnpublishedObjectName(name string) bool {
	return txn.recordedUnpublishedObjects.has(name)
}

// releaseRecordedUnpublishedObjectsLocked retires the still-owned ledger
// members of this transaction's recorded names and drops the
// transaction's bookkeeping copy. Commit (its obligations were published
// as data) and the CCPR rollback (whose shared files deliberately stay
// on storage under the txn cache's ownership) are the two terminal paths
// that retire without the queue; the ordinary rollback's transfers
// already moved every member. The release also detaches the queue's
// recorded-owner registrations that point at this terminal transaction —
// with the copy gone, the entries can never retire anything again, and a
// queued obligation must not retain the completed transaction's graph
// (issue #25590 round-40 review, finding 1). Dropping the copy keeps
// the holder — a GC worker still parked on this transaction's accepted
// batch may retire against it — but empties it, so the worker's late
// prune retires nothing (issue #25590 round-41 review, finding 1).
// Callers hold the transaction lock.
func (txn *Transaction) releaseRecordedUnpublishedObjectsLocked() {
	names, took := txn.recordedUnpublishedObjects.takeAll()
	if !took {
		return
	}
	if txn.engine != nil {
		// Round 57 (issue #25590 review): the release retires exactly
		// THIS workspace's claims; a name another live workspace claims
		// keeps its membership and capacity until that final claim
		// retires.
		txn.engine.releaseUnpublishedPendingPublished(txn, names)
		// round-40 (issue #25590 review of 2026-10-24, finding 1):
		// the release just ended the recorded-name copy of every
		// registered owner entry this transaction still had, so the
		// terminal also detaches those queue-side entries - a queued
		// cleanup obligation may not retain the completed transaction's
		// graph until the drain happens to delete its file (the
		// identity-conditioned pop keeps the LAST handoff's owner for a
		// name another transaction recorded).
		txn.engine.detachTerminalRecordedUnpublishedObjectOwners(txn, names)
	}
}

// convertDumpedReservationsLocked settles the persisted names of a failed
// publication — a dump whose record was refused, or a direct-S3 writer
// unwinding after its record was refused (issue #25590 round-18/19
// reviews): each name with an outstanding reservation — the capacity hook
// reserved it before it persisted — retires exactly its in-flight unit
// and retains owned membership; a persisted name without a reservation
// only gains the membership (it was never countable twice).
func (txn *Transaction) convertDumpedReservationsLocked(files []string) {
	e := txn.engine
	if e == nil || len(files) == 0 {
		return
	}
	var reserved, extra []string
	txn.unpublishedDumpReservationsMu.Lock()
	for _, n := range files {
		if _, ok := txn.unpublishedDumpReservations[n]; ok {
			delete(txn.unpublishedDumpReservations, n)
			reserved = append(reserved, n)
		} else {
			extra = append(extra, n)
		}
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	if len(reserved) > 0 {
		e.convertUnpublishedPendingReservations(txn, reserved)
	}
	if len(extra) > 0 {
		e.retainUnpublishedPendingNames(txn, extra)
	}
}

// drainUnpublishedDumpReservationsTerminalReason is the retain reason the
// transaction/fragment terminals pass the reservation drain: at a
// terminal the drain is each leftover's last reachable owner (issue
// #25590 round-22 review, finding H1). The statement-rollback boundary
// passes its own reason — an earlier, still-open-transaction recycle.
const drainUnpublishedDumpReservationsTerminalReason = "terminal reservation drain: the record never landed on a persisted or adopted name, so the terminal adopts it as its last reachable cleanup owner (issue #25590 round-22 review)"

// drainUnpublishedDumpReservationsLocked settles the outstanding
// reservations at a storage-quiet boundary whose statements can no
// longer record — the transaction's and remote fragments' TERMINALS,
// where the drain is each leftover's LAST reachable owner, and the
// quiesced STATEMENT-ROLLBACK boundary (round-33 review, finding 2),
// where the aborted statement's records can never land. Every
// reservation closes exactly once (a converted record settled by the
// epilogue washes out first, the epilogue swept the failures, and the
// terminal drain covers a dump interrupted between spill and record) —
// so no in-flight unit can outlive its statement's rollback, and none
// can outlive its transaction. The retain's recorded-membership recheck
// keeps every landed record's file on its own lifecycle and the
// committed-borrowed exemption keeps clone-cache shares intact at BOTH
// boundary classes, and the transfer itself performs no storage I/O:
// the boundary's queue handoff and the engine's drain collect the
// files afterwards exactly the r11 design's decision 3 records. Callers
// hold the transaction lock.
func (txn *Transaction) drainUnpublishedDumpReservationsLocked(reason string) {
	txn.unpublishedDumpReservationsMu.Lock()
	if len(txn.unpublishedDumpReservations) == 0 {
		txn.unpublishedDumpReservationsMu.Unlock()
		return
	}
	names := make([]string, 0, len(txn.unpublishedDumpReservations))
	for n := range txn.unpublishedDumpReservations {
		names = append(names, n)
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	if e := txn.engine; e != nil {
		// r6 (issue #25590 round-22 review, finding H1): a name whose
		// reservation is still outstanding at the boundary did NOT get
		// recorded — the choke converts every reserved name it registers,
		// and any epilogue that retained a failed publication already
		// consumed its reservation out of the map. The leftovers are the
		// adopted remote-transferred names whose record the coordinator
		// never reached — cancellation, discarded metadata, a preparation
		// failure — and the local writer classes whose record never
		// landed; both name classes can carry a physically persisted
		// object, and a terminal is their last reachable owner. The
		// retain converts each leftover's in-flight unit into owned
		// membership (the recorded filter keeps any live record's file
		// untouched), and the caller's pending handoff moves them to the
		// engine's lossless queue — the drain must therefore run BEFORE
		// the terminal's handoff. CCPR keeps the shared-file exemption
		// (the clone cache owns the shares) and abandons exactly.
		if txn.isCCPRTxn {
			txn.unpublishedDumpReservationsMu.Lock()
			txn.unpublishedDumpReservations = nil
			txn.unpublishedDumpReservationsMu.Unlock()
			e.abandonUnpublishedPendingReservations(names)
			return
		}
		txn.retainDefiniteOrphansLocked(names, reason)
	}
	txn.unpublishedDumpReservationsMu.Lock()
	txn.unpublishedDumpReservations = nil
	txn.unpublishedDumpReservationsMu.Unlock()
}

// RetainUnrecordedS3Objects implements the structural retainer interface
// the direct-S3 insert writer asserts on (ioutil.UnrecordedSinkerObjectsRetainer)
// — the pipeline's teardown hands the writer's persisted / possibly
// persisted names here when its metadata record was refused, or the
// stream broke between persistence and recording (issue #25590
// round-19 review, finding 2). Every name whose record DID land stays
// behind: its deletion is owned by the record's own lifecycle — rollback's
// queue handoff, commit's release — and Re-taking a recorded name would
// both double-count it and hand a live workspace object to the deleter.
// The UNRECORDED remainder joins the transaction's pending-object cleanup
// list — handed to the engine's lossless cleanup queue at the next
// statement admission or at the terminal — and the capacity ledger at
// the same time: each persisted name's per-object reservation converts
// into retained ownership, a name the hook never covered (a writer built
// without the capacity option) gains membership through the
// fail-open retain path, because the only alternative to counting an
// already-physical object is dropping its last cleanup owner.
func (txn *Transaction) RetainUnrecordedS3Objects(persisted []string, reason string) {
	txn.Lock()
	defer txn.Unlock()
	txn.retainUnrecordedS3ObjectsLocked(persisted, reason)
}

// retainUnrecordedS3ObjectsLocked is the lock-holding core of
// RetainUnrecordedS3Objects; the compaction pass reuses it for its
// rewritten-but-unrecorded replacements (issue #25590 round-19 review,
// finding 1). Callers hold the transaction lock.
func (txn *Transaction) retainUnrecordedS3ObjectsLocked(persisted []string, reason string) {
	// r5 (round-21 review, finding G1): the exported retainer surface serves
	// operator teardowns (insert, multi_update, the deletion flush, the
	// external producer seam) whose downstream recording consumer can still
	// run inside the SAME statement — a publication in flight until the
	// epoch moves.
	txn.retainUnrecordedS3ObjectsAtLocked(persisted, true, reason)
}

// retainDefiniteOrphansLocked is the r5 definite-orphan twin of
// retainUnrecordedS3ObjectsLocked: the names' recording consumer cannot run
// anymore — a failed compaction pass's replacements (the r19 back-out), a
// refused record's fail-open retain — so the round-14 immediate-transfer
// semantics apply (retainedIfRecordingLater false). Callers hold the
// transaction lock.
func (txn *Transaction) retainDefiniteOrphansLocked(persisted []string, reason string) {
	txn.retainUnrecordedS3ObjectsAtLocked(persisted, false, reason)
}

func (txn *Transaction) retainUnrecordedS3ObjectsAtLocked(
	persisted []string, retainedIfRecordingLater bool, reason string,
) {
	if len(persisted) == 0 {
		return
	}
	var unrecorded []string
	for _, n := range persisted {
		if txn.hasRecordedUnpublishedObjectName(n) {
			continue // owned by its record's lifecycle
		}
		// INV1 (issue #25590 round-24 finding F1, design r11 D1'): a
		// CLONE-COMMITTED-BORROWED name — an object this clone
		// transaction borrowed from committed state — never enters
		// the orphan machinery through either retain funnel. Its
		// prior owners still reference it: the clone completion and
		// the gcFiles scope rules own its lifecycle. Txn-local shared
		// names are a different class and keep flowing through here.
		if borrowsCommittedCloneShare(txn.engine, txn, n) {
			continue
		}
		unrecorded = append(unrecorded, n)
	}
	if len(unrecorded) == 0 {
		return
	}
	// r5: the lifecycle ordinal the retaining statement owns — advanced at
	// StartStatement, loaded lock-free (the retain runs under the
	// transaction lock; the lifecycle moves only in the serial statement
	// ceremony).
	epoch := int(txn.statementLifecycle.Load())
	for _, n := range unrecorded {
		txn.pendingObjectCleanup = append(txn.pendingObjectCleanup,
			pendingObjectCleanupItem{
				name:                     n,
				retainedIfRecordingLater: retainedIfRecordingLater,
				retainedAt:               epoch,
			})
	}
	if txn.engine != nil {
		txn.convertDumpedReservationsLocked(unrecorded)
	}
	logutil.Warn(
		"WORKSPACE-PENDING-CLEANUP-RETAKE",
		zap.Strings("files", unrecorded),
		zap.String("reason", reason),
		zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
	)
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
	// Same staged fallible prefix and infallible suffix as the
	// transferring record path (issue #25590 round-18/20 reviews): the
	// CCPR skip-transfer record also creates rollback obligations the
	// ledger must count at creation, and its copy completes before that
	// counting commits so a failed copy leaves the object reachable for
	// the unwinding retainer.
	stagedRec, err := txn.stageWorkspaceRecordLocked(typ, accountId, databaseId, tableId,
		databaseName, tableName, fileName, inputBat, tnStore,
		autoIncrEpoch, autoIncrEpochKnown, true)
	if err != nil {
		return err
	}
	txn.publishStagedWorkspaceRecordLocked(stagedRec)
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
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
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
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	txn.Lock()
	defer txn.Unlock()

	// Raw INSERT row IDs are allocated in TxnWorkspaceSegment. Persisted
	// block IDs cannot match those writes; metadata entries are excluded below.
	// Only inspect a single block, avoiding an extra scan for multi-block deletes
	// or a workspace whose original scan is already constant-sized.
	if len(deleteBlkId) == 1 && len(txn.writes) > 1 {
		for blockID := range deleteBlkId {
			if !blockID.Segment().EQ(&colexec.TxnWorkspaceSegment) {
				return
			}
		}
	}

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

	// Apply pending delete selections; fully-superseded entries are
	// retired as nil slots. The nil slots are NOT compacted here:
	// compaction changes the length recorded statement boundaries point
	// into, and IncrStatementID still has a fallible dump ahead, so the
	// statement's callers (IncrStatementID, Commit) run
	// compactNilWorkspaceSlotsLocked immediately after the dump succeeds,
	// right before the new boundary is appended — admission bookkeeping
	// and compaction are failure-atomic that way (issue #25590).
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

	foo := func(idxes []int64) (err error) {
		for i := 0; i < len(idxes); i++ {
			a := &txn.writes[idxes[i]]
			if a.bat == nil || a.bat.RowCount() == objectio.BlockMaxRows {
				continue
			}

			// Round-6 review (issue #25590): dry-run the merge first.
			// Every fallible step of the publication below — copying the
			// anchor, appending all columns of each source, regenerating
			// the combined batch's rowids — runs before the first
			// workspace mutation, so a failure preserves the original
			// sources. The old flow merged each pair in place and only
			// allocated the combined batch's new rowids afterwards: an
			// allocation failure at that point left the retained batch
			// mixing the rowids of several blocks with the merged
			// sources already gone, breaking the one-block-per-batch,
			// zero-based-offset invariants that workspace reads and
			// deleteTableWrites rely on — and the merge that shrank the
			// entry count would not run again to repair it.
			rowCount := a.bat.RowCount()
			sources := make([]int64, 0, 4)
			for j := i + 1; j < len(idxes); j++ {
				b := &txn.writes[idxes[j]]
				if b.bat != nil && a.tableId == b.tableId && a.databaseId == b.databaseId &&
					rowCount+b.bat.RowCount() <= objectio.BlockMaxRows {
					sources = append(sources, idxes[j])
					rowCount += b.bat.RowCount()
				}

				if rowCount == objectio.BlockMaxRows {
					break
				}
			}

			if len(sources) == 0 {
				continue
			}

			mp := txn.proc.GetMPool()

			var rowIdVector *vector.Vector
			if a.typ == INSERT {
				if objectio.CNWorkspaceMergeRowidAllocFailedInjected() {
					return moerr.NewInternalErrorNoCtx(
						"fault injection: workspace merge rowid alloc failed")
				}
				// regenerate the rowids for the published batch: all the
				// rowIds in one raw insert batch share one blkId, with
				// offsets running consecutively from zero.
				if rowIdVector, err = txn.batchAllocNewRowIds(rowCount); err != nil {
					return err
				}
			}

			// Stage the merged batch: the anchor's full copy carries the
			// rows of every source and the regenerated rowids. None of
			// the workspace entries is touched before this completes.
			staged, dupErr := a.bat.Dup(mp)
			if dupErr != nil {
				if rowIdVector != nil {
					rowIdVector.Free(mp)
				}
				return dupErr
			}
			for _, j := range sources {
				if _, aerr := staged.Append(ctx, mp, txn.writes[j].bat); aerr != nil {
					staged.Clean(mp)
					if rowIdVector != nil {
						rowIdVector.Free(mp)
					}
					return aerr
				}
			}
			if rowIdVector != nil {
				rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](rowIdVector)
				for x := range staged.RowCount() {
					if err = vector.SetFixedAtWithTypeCheck[objectio.Rowid](
						staged.Vecs[0], x, rowIds[x],
					); err != nil {
						staged.Clean(mp)
						rowIdVector.Free(mp)
						return err
					}
				}
				rowIdVector.Free(mp)
			}

			// Every fallible step succeeded: publish the merged batch and
			// retire its sources. Only accounting swaps and releases
			// remain here, so a failure between the mutation steps
			// cannot leave a half-merged combination behind.
			txn.beginCatalogMutation()
			// The staged batch replaces the anchor's layout: drop the
			// anchor's hydrated key index (and refund its budget bytes)
			// through the batching-mutation choke point before the swap,
			// so the stale layout is never answered, then fold every
			// source's key envelope into the survivor — the merged rows
			// span all contributing envelopes (issue #26796 ownership,
			// same invariants the dropped in-place merge carried).
			txn.unaccountInsertKeyOffsetsLocked(a)
			txn.unaccountWorkspaceEntryLocked(a)
			oldBat := a.bat
			a.bat = staged
			delete(txn.batchSelectList, oldBat)
			txn.accountWorkspaceEntryLocked(a)
			for _, j := range sources {
				src := &txn.writes[j]
				unionInsertKeyRange(a, src)
				txn.unaccountInsertKeyOffsetsLocked(src)
				txn.unaccountWorkspaceEntryLocked(src)
				delete(txn.batchSelectList, src.bat)
				src.bat.Clean(mp)
				src.bat = nil
			}
			txn.endCatalogMutation()
			oldBat.Clean(mp)
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

		var sortBuf []int64
		var shuffleBuf []byte
		for i := range txn.writes {
			// merge sources above (and the shrink pass) may have left nil
			// retired/combined slots; compaction runs after the admission
			// dump (issue #25590), so guard each entry here.
			if txn.writes[i].bat == nil {
				continue
			}
			if txn.writes[i].typ == DELETE && txn.writes[i].bat.RowCount() > 1 {
				if err := mergeutil.SortColumnsByIndexWithBuf(
					txn.writes[i].bat.Vecs, 0, txn.proc.Mp(), &sortBuf, &shuffleBuf); err != nil {
					return err
				}
			}
		}
	}

	txn.assertWorkspaceAccountingLocked()
	return nil
}

// compactNilWorkspaceSlotsLocked drops retired (nil-batch) slots from the
// workspace in place, shifting surviving entries left. Admission bookkeeping
// must stay failure-atomic with this (issue #25590): the callers
// (IncrStatementID, Commit) run it only after the fallible dump, in the
// same critical section that appends the new statement boundary, so a dump
// error can never leave a pre-compaction recorded boundary for statement
// rollback to reuse against the shortened workspace. Recorded boundaries
// are consumed newest-first (statement rollback truncation, forced-dump
// start, workspace reads), so the boundary appended right after this call
// stays exact.
func (txn *Transaction) compactNilWorkspaceSlotsLocked() {
	n := 0
	for i := range txn.writes {
		if txn.writes[i].bat == nil {
			continue
		}
		txn.writes[n] = txn.writes[i]
		n++
	}
	// Drop the vacated slots' stale Entry references through the
	// original full-length slice so the truncated workspace cannot pin
	// retired batches (the same reason the writeS3 epilogue clears its
	// vacated slots before truncating).
	clearVacatedWriteSlots(txn.writes, n, len(txn.writes))
	txn.writes = txn.writes[:n]
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
	type lookup struct {
		key          tableKey
		relationName string
	}
	tables := make(map[tableKey]engine.Relation)
	for {
		var missing []lookup
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
					missing = append(missing, lookup{k, physicalCatalogTableName(summary.databaseId, summary.tableId, summary.tbName)})
				}
				return true
			})
		if len(missing) == 0 {
			return tables, nil
		}

		txn.Unlock()
		for _, entry := range missing {
			k := entry.key
			tbl, err := txn.getTable(ctx, k.accountId, k.dbName, entry.relationName)
			if err != nil {
				txn.Lock()
				return nil, err
			}
			tables[k] = tbl
		}
		txn.Lock()
	}
}

// compactRepublication holds one fully staged re-publication of a clean
// object that shares a metadata batch with a dirty one: the single-object
// metadata batch the workspace will own (issue #25590 round-8 review).
// Every fallible step of a re-publication — the epoch-fence check, the
// batch allocation, the union of the block info, the stats bytes and the
// workspace's own batch copy — runs while a replacement is being staged,
// BEFORE the first workspace mutation of the compaction pass; only the
// publishStagedRepublicationLocked block below, which cannot fail, moves
// the staged batches into the workspace registries and txn.writes.
type compactRepublication struct {
	bat                 *batch.Batch
	name                string
	accountId           uint32
	databaseId, tableId uint64
	databaseName        string
	tableName           string
	tnStore             DNStore
	autoIncrEpoch       uint32
	autoIncrEpochKnown  bool
}

// publishStagedRepublicationLocked is the second, failure-free half of the
// staged compaction re-publication: segment registration, the CN-object
// summary overwrite, the transaction flags and the workspace append. With
// no error left anywhere in this chain, the pass either publishes every
// staged replacement back-to-back, or — when staging failed first — none.
// A partial publication (the first affected object re-published, the next
// failing while its original waits un-retired in the same statement region
// the interrupted-admission rollback then truncates) cannot survive: the
// stranded duplicate object references re-read and re-committed the same
// object twice (issue #25590 round-8 P1).
func (txn *Transaction) publishStagedRepublicationLocked(
	r *compactRepublication,
) {
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()

	server := colexec.MustGetServer(txn.engine.service)
	col, area := vector.MustVarlenaRawData(r.bat.Vecs[1])
	for i := range col {
		stats := objectio.ObjectStats(col[i].GetByteSlice(area))
		oid := stats.ObjectName().ObjectId()
		sid := oid.Segment()
		server.PutCnSegment(&txn.workspaceSegments, txn.op.Txn().ID, r.tableId, sid,
			colexec.TxnWorkspaceUnCommitType)
		txn.registerCNObjects(*oid, r.accountId, r.databaseId, r.tableId,
			r.bat, r.databaseName, r.tableName, r.autoIncrEpoch,
			r.autoIncrEpochKnown)
	}

	txn.hasS3Op.Store(true)
	txn.readOnly.Store(false)

	txn.appendWorkspaceEntryLocked(Entry{
		typ:                INSERT,
		accountId:          r.accountId,
		tableId:            r.tableId,
		databaseId:         r.databaseId,
		tableName:          r.tableName,
		databaseName:       r.databaseName,
		fileName:           r.name,
		bat:                r.bat,
		tnStore:            r.tnStore,
		pkCheckPos:         -1,
		pkCheckReady:       true,
		autoIncrEpoch:      r.autoIncrEpoch,
		autoIncrEpochKnown: r.autoIncrEpochKnown,
	})
}

// CN blocks compaction for txn
func (txn *Transaction) compactDeletionOnObjsLocked(ctx context.Context) error {

	if txn.deletedBlocks.size() == 0 {
		return nil
	}

	// resolve every affected table BEFORE building the compaction state and
	// spawning workers; workers must not call getTable (see
	// resolveCompactTablesLocked)
	tables, err := txn.resolveCompactTablesLocked(ctx)
	if err != nil {
		return err
	}
	if txn.deletedBlocks.size() == 0 {
		return nil
	}

	// object --> blk id --> deletion
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

			blkDel[*blkId] = offsets

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

	// round-19 restructure (issue #25590 round-19 review, finding 1): the
	// compaction keeps its fallible-prefix / infallible-tail discipline
	// across EVERY phase. Phase A workers run only the PHYSICAL rewrite
	// of a dirty object — the reader, the block copies, the spilling
	// writer with the per-object capacity hook — and return the
	// replacement plus its persisted names to the parent instead of
	// recording them under a lock they do not hold. Phase B — the parent,
	// holding the transaction lock admission already grants — records
	// every finished replacement, the single remaining fallible step.
	// Only after the last fallible step succeeds does the infallible
	// tail run: staged republications publish, the originals' batches
	// retire, the original objects unprotect and GC, the pending marks
	// clear. A refusal at any fallible point — the hook on a spilling
	// replacement object, the record choke — now fails the WHOLE
	// admission with the workspace exactly as found: the pre-fix worker
	// converted the record's admission error into a panic, the ants pool
	// recovered it, waiter.Done still ran, and the tail garbage-collected
	// the original objects, wiped the marks and returned nil — the
	// rejected replacement's surviving rows vanished from the workspace
	// (and the physically persisted replacement leaked with no cleanup
	// owner).
	type compactRewrite struct {
		tbl       *txnTable
		tbKey     workspaceTableKey
		writeName string
		bat       *batch.Batch
		fileName  string
		persisted []string
		err       error
	}

	waiter := sync.WaitGroup{}
	locker := sync.Mutex{}
	rewrites := make([]compactRewrite, 0, 8)

	dirtyObject := make([]objectio.ObjectStats, 0, 1)

	// Metadata entries whose objects this pass fully classified are retired
	// only after the tail below published every staged re-publication AND
	// recorded every replacement — never mid-loop and never before the
	// pass's last fallible step succeeded: the re-publications and
	// replacement records live in this statement's workspace region, and
	// the statement rollback that follows a failed admission truncates
	// exactly that region. Retiring an original before the pass outcome is
	// known would strand it — the rollback cannot restore a slot outside
	// the failing statement — and leave objects recorded by neither their
	// old nor their new metadata (issue #25590).
	//
	// The re-publication itself is two-phase (issue #25590 round-8 review):
	// the staging loop below runs EVERY fallible step of EVERY affected
	// object — the epoch-fence check, the fault window, the batch
	// allocation, the union of the object's block info, the stats bytes and
	// the workspace's own batch copy — before the first workspace mutation
	// of this pass. Only a complete staging set reaches the publication
	// loop, whose remaining steps touch nothing but maps and the writes
	// slice; with no failure point left between them, a partial
	// publication — one affected object's replacement appended, the next
	// failing, the originals all un-retired — can no longer be stranded
	// inside the rolled-back statement as duplicate object references that
	// re-read and re-commit the same object twice.
	retired := make([]int, 0, 4)
	staged := make([]compactRepublication, 0, 8)
	// cleanStagedRepubs forgives every staged batch on any staging error, so
	// an aborting pass leaves the workspace exactly as it found it.
	cleanStagedRepubs := func() {
		for k := range staged {
			staged[k].bat.Clean(txn.proc.Mp())
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

		// The epoch fence is part of the publication's fallible prefix: it
		// must hold for every staged object before the pass mutates any
		// registry, so a fenced entry cannot strand partially published
		// replacements behind it.
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
				// clean object, no deletion on it: stage its metadata as
				// its own single-object batch. Every step from here on is
				// fallible (the fault window, mpool allocations, the
				// workspace's own copy) and the pass is in the middle of
				// staging replacements for a batch that also holds
				// un-rewritten dirty objects — an error must keep the
				// workspace re-runnable as-found, so the pending
				// deletedBlocks of this and every other object stay
				// untouched, the retried admission reapplies them
				// (issue #25590 round 7), and nothing staged — not even
				// the objects whose staging already succeeded — reaches
				// any registry while staging continues (round 8).
				if objectio.CNWorkspaceCompactPublishFailedInjected() {
					cleanStagedRepubs()
					return moerr.NewInternalErrorNoCtx(
						"fault injection: object compaction publish failed")
				}
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

				// The workspace's own copy of the replacement batch — the
				// last allocation of the fallible prefix. The staged copy
				// transfers to the workspace entry in the publication loop
				// below, whose steps cannot fail anymore.
				entryBat, err := bat.Dup(txn.proc.Mp())
				bat.Clean(txn.proc.Mp())
				if err != nil {
					cleanStagedRepubs()
					return err
				}

				staged = append(staged, compactRepublication{
					bat:                entryBat,
					name:               stats.ObjectName().String(),
					accountId:          entry.accountId,
					databaseId:         entry.databaseId,
					tableId:            entry.tableId,
					databaseName:       entry.databaseName,
					tableName:          entry.tableName,
					tnStore:            entry.tnStore,
					autoIncrEpoch:      entry.autoIncrEpoch,
					autoIncrEpochKnown: entry.autoIncrEpochKnown,
				})
			} else {
				dirtyObject = append(dirtyObject, stats)
			}

			offset += int(stats.BlkCnt())
		}

		retired = append(retired, i)
	}

	// Phase A: rewrite every dirty object physically — the ONLY remaining
	// fallible work after staging, and it touches no workspace state at
	// all, so a failed rewrite (or the replacement's capacity refusal at
	// its bound sinker's hook) leaves nothing that a rollback would have
	// to restore. The parent pre-resolves each table and pre-computes the
	// record's write shapes, so a worker owns nothing but its own
	// result slot and its filesystem reader/writer.
	for i := range dirtyObject {
		stats := dirtyObject[i]
		objId := stats.ObjectName().ObjectId()
		tbKey := objTables[*objId]

		// resolveCompactTablesLocked guarantees every table referenced by
		// txn.deletedBlocks is in the map
		tbl, _ := tables[tbKey.tableKey].(*txnTable)
		if tbl == nil {
			delegate, _ := tables[tbKey.tableKey].(*txnTableDelegate)
			tbl = delegate.origin
		}
		writeName := tbl.tableName
		if tbl.db.databaseId == catalog.MO_CATALOG_ID && tbl.tableId == catalog.MO_COLUMNS_ID &&
			tbKey.name == catalog.MO_COLUMNS_UPDATE {
			writeName = tbKey.name
		}
		rewrites = append(rewrites, compactRewrite{
			tbl:       tbl,
			tbKey:     tbKey,
			writeName: writeName,
		})
	}
	if len(rewrites) > 0 {
		if txn.compactWorker == nil {
			txn.compactWorker, _ = ants.NewPool(min(runtime.NumCPU(), 4))
			defer func() {
				txn.compactWorker.Release()
				txn.compactWorker = nil
			}()
		}
		for i := range rewrites {
			waiter.Add(1)
			stats := dirtyObject[i]
			blkDeletion := objBlkDeletion[*dirtyObject[i].ObjectName().ObjectId()]
			txn.compactWorker.Submit(func() {
				defer waiter.Done()
				tbl := rewrites[i].tbl
				locker.Lock()
				tbl.ensureSeqnumsAndTypesExpectRowid()
				locker.Unlock()
				bat, fileName, persisted, err := tbl.rewriteObjectByDeletion(
					ctx, stats, blkDeletion)
				rewrites[i].bat = bat
				rewrites[i].fileName = fileName
				rewrites[i].persisted = persisted
				rewrites[i].err = err
			})
		}
		waiter.Wait()
	}

	// Phase B: prepare every finished replacement's record on the parent,
	// under the transaction lock admission already holds — STAGED, never
	// appended (issue #25590 round-20 review, finding 1). Every fallible
	// step of every record (the epoch fence, the workspace's own batch
	// copy, the record's admission choke) runs here on data the parent
	// owns; a record that staged committed only still-retractable
	// admission state, because the publication loop below — the pass's
	// INFALLIBLE tail — appends the staged entries to the writes slice
	// back-to-back. The FIRST error still stops the staging loop, and the
	// back-out retracts every staged admission, releases every staged
	// re-publication batch and hands every unrecorded persisted
	// replacement to the cleanup queue. This is what makes the pass
	// atomic across ALL its replacements: the pre-fix loop recorded each
	// replacement the moment its own record call returned, so with two
	// dirty objects an earlier successful record survived a later
	// refusal as a live workspace entry next to an un-retired original —
	// its statement boundary sits above that appended record, so the
	// statement rollback could not remove it, and the retried compaction
	// wrote a SECOND replacement for the same original, committing the
	// surviving rows twice.
	var compactErr error
	recordStages := make([]*stagedWorkspaceRecord, 0, len(rewrites))
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
			// round-32 review, finding 1: a fully deleted rewrite is NO
			// replacement record (rewriteObjectByDeletion returns a nil
			// bat for an object whose deletions removed every row), so
			// nothing stages here and its original entry retires through
			// the pass's own retirement loop below exactly like every
			// other classified object - the pass stays atomic across
			// all its replacements, and the fully deleted object leaves
			// no empty workspace entry behind. A degenerate name the
			// rewrite nonetheless persisted takes the same
			// definite-orphan funnel an unrecorded replacement takes,
			// so no unrecorded file can outlive the entries.
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
	}

	if compactErr != nil {
		// Failure-atomic back-out: no staged re-publication published, no
		// original batch retired, no original object GC'd, no pending
		// mark cleared — the workspace stays re-runnable exactly as
		// found and the retried admission re-runs the whole pass.
		// Staged records never reached the writes slice, so their
		// admissions UNDO here first: retracting the staged names from
		// the recorded set puts them back where every leftover-object
		// enumeration can see them, and the prepared copies clean with
		// the staged re-publication batches (issue #25590 round-20
		// reviews, findings 1 and 4 — those batches never entered
		// txn.writes, so no transaction cleanup can release them:
		// without this release every refused admission would strand one
		// allocated batch per clean object behind the bound).
		for _, st := range recordStages {
			txn.retractUnpublishedRecordAdmissionLocked(st)
			if st.bat != nil {
				st.bat.Clean(txn.proc.Mp())
				st.bat = nil
			}
		}
		cleanStagedRepubs()
		// Every physical replacement this pass created — rewritten but
		// never recorded, or commit-ambiguously half-persisted by a
		// failing rewrite — transfers to the transaction's asynchronous
		// cleanup ownership, the SAME transfer a failed dump
		// publication performs: the names join pendingObjectCleanup (the
		// next admission boundary — or the terminal — moves them to the
		// engine's lossless queue whose drain deletes them) and the
		// capacity ledger, and the deletedBlocks marks stay put so the
		// retried pass reapplies them (issue #25590 round-19 review,
		// finding 1).
		for i := range rewrites {
			r := &rewrites[i]
			if r.bat != nil {
				r.bat.Clean(txn.proc.Mp())
				r.bat = nil
			}
			if len(r.persisted) == 0 {
				continue
			}
			// r5 (round-21 review): the failed pass's replacements are
			// definite orphans — the retry rewrites the originals freshly,
			// so no recording consumer can ever land for these names.
			txn.retainDefiniteOrphansLocked(
				r.persisted,
				"object compaction admitted a replacement's persistence but returned before recording any replacement: no append could survive the statement rollback, so the names hand to the cleanup queue and the marks stay for the retried admission (issue #25590 round-19/20 reviews)")
		}
		return compactErr
	}

	// The pass's last fallible step is done: publish the staged
	// replacement records back-to-back, then all staged re-publications,
	// then retire the originals' batches — none of the three published
	// steps can fail, they touch only engine maps, txn state and the
	// writes slice, so this tail appends every record and re-publication
	// together or, had any earlier step failed, none at all (issue
	// #25590 round-8/20 reviews).
	for _, st := range recordStages {
		txn.publishStagedWorkspaceRecordLocked(st)
	}
	for i := range rewrites {
		r := &rewrites[i]
		if r.bat != nil {
			r.bat.Clean(txn.proc.Mp())
			r.bat = nil
		}
	}
	for k := range staged {
		txn.publishStagedRepublicationLocked(&staged[k])
		staged[k].bat = nil
	}

	// The classification survived every entry, and every replacement and
	// re-publication of the pass has recorded, so the originals' batches
	// and objects can be replaced by them.
	for _, i := range retired {
		txn.releaseWorkspaceEntryBatchLocked(i)
	}
	txn.assertWorkspaceAccountingLocked()

	txn.unprotectUnreferencedTxnLocalSharedFilesLocked(dirtyObject, nil)
	_ = txn.GCObjsByStats(dirtyObject...)

	// The rewrites above physically embody the pending deletions, so the
	// marks may go now. Every early return above intends the opposite:
	// the deletions are still pending state of earlier statements and the
	// next admission reapplies them (issue #25590).
	txn.deletedBlocks.clean()
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

type tombstoneTransferKey struct {
	accountId           uint32
	databaseId, tableId uint64
}

type tombstoneTransferTable struct {
	databaseName string
	writeNames   [2]string
	hasDeletes   [2]bool
	table        *txnTable
}

// collectTombstoneTransferTablesLocked retains only scalar lookup metadata for
// this invocation. Relation lookup may unlock and reenter the workspace.
func (txn *Transaction) collectTombstoneTransferTablesLocked() map[tombstoneTransferKey]tombstoneTransferTable {
	var tables map[tombstoneTransferKey]tombstoneTransferTable
	for i := range txn.writes {
		e := &txn.writes[i]
		if e.typ != DELETE || e.bat == nil || e.bat.RowCount() == 0 || e.skipTransfer {
			continue
		}
		key := tombstoneTransferKey{e.accountId, e.databaseId, e.tableId}
		if tables == nil {
			tables = make(map[tombstoneTransferKey]tombstoneTransferTable)
		}
		t := tables[key]
		if !t.hasDeletes[0] && !t.hasDeletes[1] {
			t.databaseName = e.databaseName
		}
		kind := 0
		if e.fileName != "" {
			kind = 1
		}
		if !t.hasDeletes[kind] {
			t.hasDeletes[kind] = true
			t.writeNames[kind] = e.tableName
			tables[key] = t
		}
	}
	return tables
}

func (txn *Transaction) forEachTableHasDeletesLocked(
	tables map[tombstoneTransferKey]tombstoneTransferTable,
	kind int,
	f func(tbl *txnTable, writeName string) error) error {
	for key, t := range tables {
		if !t.hasDeletes[kind] || t.table != nil {
			continue
		}
		name := physicalCatalogTableName(key.databaseId, key.tableId, t.writeNames[kind])
		dbName := t.databaseName
		// Database lookup may run internal SQL on this transaction.
		rel, err := func() (engine.Relation, error) {
			txn.Unlock()
			defer txn.Lock()
			return txn.getTable(txn.proc.Ctx, key.accountId, dbName, name)
		}()
		if err != nil {
			return err
		}
		if v, ok := rel.(*txnTableDelegate); ok {
			t.table = v.origin
		} else {
			t.table = rel.(*txnTable)
		}
		tables[key] = t
	}
	for _, t := range tables {
		if t.hasDeletes[kind] {
			if err := f(t.table, t.writeNames[kind]); err != nil {
				return err
			}
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
		return nil, err
	}

	// mergeTxnWorkspaceLocked (and the compactDeletionOnObjsLocked call
	// inside it) mutates txn.writes and may release/re-acquire the lock
	// around metadata resolution, so it must run with the lock held — same
	// as the IncrStatementID path.
	txn.Lock()
	if err := txn.mergeTxnWorkspaceLocked(ctx); err != nil {
		txn.Unlock()
		return nil, err
	}
	if err := txn.dumpBatchLocked(ctx, -1); err != nil {
		txn.Unlock()
		return nil, err
	}
	// Same failure-atomic discipline as IncrStatementID: retired and
	// merge-combined nil slots are compacted only after the fallible
	// dump, before the commit payload is assembled from the workspace.
	txn.compactNilWorkspaceSlotsLocked()
	txn.Unlock()

	if msg, injected := objectio.CNCommitAfterWorkspaceDumpFailedInjected(); injected {
		if msg == "" {
			msg = "injected commit failure after workspace dump"
		}
		return nil, moerr.NewInternalError(ctx, msg)
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

	// r5 (issue #25590 round-21 review, finding G3): the ledger release that
	// r18/r20 performed HERE — right after drafting the commit payload —
	// moved out. This function only PREPARES the commit: the caller still
	// runs the commit RPC and classifies the outcome after it, and a
	// prepared-but-unclassified transaction must keep holding its recorded
	// names' capacity (a competing transaction can never consume the unit
	// a parked commit still needs; a definite failure transfers owned→
	// queued without increasing the total). The terminal releases live in
	// FinalizeCommit (published), Rollback (definite failure, transfer)
	// and FinalizeCommitWithUnknownResult (release without deletion).

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

func (txn *Transaction) FinalizeCommit(ctx context.Context) {
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnCommit(txn.op.Txn().ID)
	}
	// r5 terminal settlement (issue #25590 round-21 review, finding G3):
	// the caller classified a SUCCESSFUL commit, so the transaction's
	// recorded workspace objects are published — retain their capacity no
	// longer. The pending handoff runs first (under the transaction's
	// recorded set intact, per the r5 boundary classification): the
	// unrecorded remainder moves to the engine's lossless quiesced handoff
	// with no storage I/O on the dying transaction's path, then the
	// recorded names retire as published (no deletion), and every
	// outstanding dump reservation settles so no in-flight unit can
	// outlive its transaction.
	txn.Lock()
	// r6 (round-22): the reservation drain runs FIRST — unconverted
	// reservations adopt the terminal as their last reachable owner and
	// join the pending list below — then the quiesced handoff carries
	// everything (the retained adoptions included) to the engine's
	// lossless queue with the recorded set intact, and the recorded
	// names retire as published.
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	txn.handoffPendingObjectCleanupLocked(true)
	txn.releaseRecordedUnpublishedObjectsLocked()
	txn.Unlock()
	txn.delTransaction()
}

func (txn *Transaction) FinalizeCommitWithUnknownResult(ctx context.Context) {
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnUnknownResult(txn.op.Txn().ID)
	}
	// r5 terminal settlement (issue #25590 round-21 review, finding G3):
	// the unknown-outcome classification also settles exactly once, and it
	// RETIRES without deleting: the commit may have reached TN, so no
	// recorded object may be enqueued for deletion here (the pre-existing
	// ambiguity doctrine), while the capacity obligation ends with this
	// transaction no matter which way TN resolves it — a parked unit
	// could otherwise never return to the bound, because nothing ever
	// classifies this workspace a second time. The unrecorded remaining
	// pending names hand off to the engine's queue through the same
	// lossless, storage-free transfer (recorded-membership rechecked
	// first): no commit payload can reference them.
	txn.Lock()
	// r6 (round-22): the drain adopts unconverted reservations (the
	// coordinator never recorded them) into the pending list first, the
	// quiesced handoff carries everything to the queue with the recorded
	// set intact, and the recorded names retire without deletion.
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	txn.handoffPendingObjectCleanupLocked(true)
	txn.releaseRecordedUnpublishedObjectsLocked()
	txn.Unlock()
	// The Commit may have reached TN. delTransaction releases only CN-local
	// state and intentionally does not run gcObjsByIdxRange or delete shared
	// object-storage data for recorded objects.
	txn.delTransaction()
}

// FinalizeRemoteFragmentWorkspace performs the r5 one-of terminal
// settlement of a workspace a remote statement run constructed — the
// per-message mirror-txn workspace remoterunServer binds with
// disttae.NewTxnWorkSpace before the fragment's writers run (issue
// #25590 round-21 review, finding G2). On this tree no other terminal
// ever reaches that workspace: the run's cleanup releases scopes and
// process resources only, so the fragment's per-object reservations and
// teardown-retained names would stay counted on the remote engine's
// ledger forever — every successful distributed write permanently
// consuming remote capacity.
//
// transferred=true — the fragment ran clean, so every metadata batch it
// emitted flowed to the coordinator, whose record choke IS the
// acknowledgment (an admission releases with the record; a refusal
// fail-open-retains the reservation-less names on the recording side).
// The settlement releases exactly: outstanding reservations settle,
// the retained pending names retire WITHOUT deletion (their records own
// the fate: publish on commit, the coordinator's rollback transfer on
// failure), and any remotely-recorded names release as published. No
// local storage I/O runs at all. The transfer then tears down the
// mirror workspace's LOCAL state through the same delTransaction the
// other terminals end with (issue #25590 round-55 review): the copied
// mpool batches a flush consumer's source.Write staged in this
// workspace, the workspace index's accounting, the transaction's own
// segment-map claims — released per claim, so an entry a recording
// workspace re-registered stays serving that workspace — and the
// process/compact/quota references all retire with the run, because no
// other terminal ever reaches a settled mirror workspace again: an
// idle-but-live completed mirror is itself the leak.
//
// transferred=false — the run failed or was cancelled, so the statement
// fails everywhere and the in-flight names share one deletion fate: the
// ordinary workspace Rollback terminal both settles and cleans (retains
// and enqueues the unrecorded remainder, transfers recorded names
// owned→queued, settles reservations, destroys the workspace state) on
// the local engine that owns them.
func (txn *Transaction) FinalizeRemoteFragmentWorkspace(
	ctx context.Context, transferred bool,
) error {
	if !transferred {
		return txn.Rollback(ctx)
	}
	txn.Lock()
	// r6 (issue #25590 round-22 review): the reservation drain runs FIRST —
	// leftover reservations (a fragment writer the coordinator never
	// recorded) adopt this terminal as their last reachable owner and join
	// the pending list — so the names collected below cover them, and the
	// published release settles every obligation exactly once (the
	// coordinator's adopted reservations own the files' fate).
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	names := txn.pendingObjectCleanupNames()
	txn.pendingObjectCleanup = nil
	txn.releaseRecordedUnpublishedObjectsLocked()
	txn.Unlock()
	if txn.engine != nil && len(names) > 0 {
		// Round 57 (issue #25590 review): the fragment's released names
		// retire exactly the FRAGMENT's claims; a name the recording
		// workspace re-claimed keeps its membership and capacity until
		// that workspace's own terminal retires the final claim.
		txn.engine.releaseUnpublishedPendingPublished(txn, names)
	}
	logutil.Info(
		"WORKSPACE-REMOTE-FRAGMENT-SETTLED",
		zap.Int("retained-names-released", len(names)),
		zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
	)
	// Local workspace teardown, identical in shape to FinalizeCommit's
	// lock-settle-unlock-teardown ordering (issue #25590 round-55
	// review): delTransaction releases only CN-local state — the staged
	// entry batches, the workspace index accounting, the transaction's
	// own segment-map claims — and intentionally performs no
	// storage I/O, so the objects the coordinator adopted remain
	// untouched on exactly the transferred-name fate the settlement
	// above recorded. delTransaction requires the caller to hold no
	// transaction lock.
	txn.delTransaction()
	return nil
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

	tables := txn.collectTombstoneTransferTablesLocked()
	if err = transferInmemTombstones(ctx, txn, tables, start, end); err != nil {
		return err
	}

	return transferTombstoneObjects(ctx, txn, tables, start, end)
}

func forceTransfer(ctx context.Context) bool {
	return ctx.Value(UT_ForceTransCheck{}) != nil
}

func skipTransfer(ctx context.Context, txn *Transaction) bool {
	return time.Since(txn.start) < txn.engine.config.cnTransferTxnLifespanThreshold
}

func (txn *Transaction) Rollback(ctx context.Context) error {
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	if !txn.ReadOnly() && len(txn.writes) > 0 {
		logutil.Info(
			"Transaction.Rollback",
			zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
		)
	}
	_, loadCleanupErr := txn.deleteLoadFiles(ctx, nil)

	// Storage objects orphaned by failed dump publications were recorded by
	// no workspace entry, so the gc sweep below cannot know them — this
	// pass hands every still-owned name to the engine's queue. The shared
	// terminal helper is the SAME leg the two commit finalizers run and
	// performs no storage I/O itself (issue #25590 round-12 review), so a
	// dirty — even context-blind-stalled — file service cannot park the
	// rollback's final moment or the transaction locks behind it: the queue
	// owns the names the moment the transaction ends, and the drain
	// collects them storage-fate notwithstanding.
	txn.Lock()
	txn.finalizePendingObjectCleanup()
	txn.Unlock()

	// For CCPR transactions, call OnTxnRollback to clean up the cache and GC objects
	// Skip normal GCObjsByIdxRange for CCPR transactions because:
	// 1. CCPR objects may be shared across transactions
	// 2. CCPRTxnCache.OnTxnRollback handles GC properly (only deletes when no other txn references the object)
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnRollback(txn.op.Txn().ID)
	} else {
		//to gc the s3 objs
		// The recorded files transfer to the engine's lossless queue
		// instead of a pool submission: this rollback releases the
		// transaction locks only after it returns, and a saturated GC
		// pool would park the rollback behind stalled storage exactly
		// where the transaction's final moment must stay free of every
		// storage fate (issue #25590 round-14 review).
		if err := txn.gcObjsByIdxRangeToOwner(0, len(txn.writes)-1, cloneGCTxnRollback); err != nil {
			panic("Rollback txn failed: to gc objects generated by CN failed")
		}
	}

	// Terminal retirement of the recorded-name ledger (issue #25590
	// round-18 review, finding 2): the ordinary rollback's queue
	// transfers above moved every handed member already — clone-scope
	// filters and the CCPR branch's deliberately-shared files are the
	// remaining still-owned members, and their obligations also END at
	// this terminal (the clone cache owns the shares), so the release
	// is idempotent for moved names and exact for the rest.
	//
	// r6 (issue #25590 round-22 review): the reservation drain now RESETTLES
	// the leftover reservations — the adopted remote-transferred names whose
	// record the coordinator never reached among them — by adopting them
	// into the pending list as definite orphans (the retain reads the
	// recorded set and mutates the pending list, so this section holds the
	// transaction lock), and the quiesced handoff below carries them to the
	// engine's lossless queue; the head finalize above handed the
	// pre-existing list off already, so this second pass moves exactly the
	// drain's adoptions and is idempotent for the rest.
	txn.Lock()
	txn.drainUnpublishedDumpReservationsLocked(drainUnpublishedDumpReservationsTerminalReason)
	txn.handoffPendingObjectCleanup()
	txn.releaseRecordedUnpublishedObjectsLocked()
	txn.Unlock()

	txn.delTransaction()
	return loadCleanupErr
}

func (txn *Transaction) delTransaction() {
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
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
		txn.unaccountInsertKeyOffsetsLocked(&txn.writes[i])
		txn.unaccountWorkspaceEntryLocked(&txn.writes[i])
		txn.writes[i].bat.Clean(txn.proc.Mp())
	}
	txn.assertWorkspaceAccountingLocked()

	txn.tableCache = nil
	txn.tableOps = nil
	txn.databaseOps = nil

	txn.cn_flushed_s3_tombstone_object_stats_list = nil
	txn.deletedBlocks = nil
	txn.haveDDL.Store(false)
	// Retirement removes exactly the segment-map entries THIS workspace
	// registered (its ledger), never sweeping foreign transactions'
	// registrations: a read-only fragment's empty ledger never opens the
	// segment mutex, and a fragment cannot unregister the registrations
	// another fragment of the same distributed transaction made under
	// the shared txn ID on this CN (issue #25590 review finding P2).
	colexec.MustGetServer(txn.engine.service).
		RetireTxnWorkspaceSegments(&txn.workspaceSegments)
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

var _ client.TableCreationView = (*Transaction)(nil)

func (txn *Transaction) IsTableCreatedInTxn(tableID uint64) bool {
	return txn.tableOps != nil && txn.tableOps.existCreatedInTxn(tableID)
}

var _ client.TerminalTableDeletionView = (*Transaction)(nil)

// IsTableDeletedAtTxnClose reads the surviving physical deletion set after
// statement rollback and commit preparation. The terminal callback runs while
// the transaction is quiescent, so taking the workspace lock here would add an
// operator-to-workspace lock edge without protecting a concurrent writer.
func (txn *Transaction) IsTableDeletedAtTxnClose(physicalTableID uint64) bool {
	_, deleted := txn.tablesInVain[physicalTableID]
	return deleted
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
// end of txn.writes. Only owning statement-boundary callers may use it:
// frontend admission or a new user Compile/Reset. Borrowed SQL never advances the
// boundary, or a mid-statement dump can compact entries covered by it.
func (txn *Transaction) UpdateSnapshotWriteOffset() {
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
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

	// Mirror NewTxnWorkSpace: the rowid generator of a cloned workspace
	// must issue TxnWorkspaceSegment rowids. Without the segment, rows
	// inserted through a cloned workspace leave the workspace-resident
	// delete path: deleteTableWrites/deleteBatch classify by segment, so
	// deletes of those rows would not be marked via batchSelectList and
	// would fall back to emitting persistent tombstones for rows that
	// never left the transaction.
	ws.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

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
