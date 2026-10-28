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
	if entry.typ == INSERT &&
		isPkMoveLinkTableInsert(entry.databaseName, entry.tableName) {
		// Link batches dodge every pre-commit dump, so the workspace
		// totals alone never bound them: track their retained bytes
		// exactly for the admission check in writeBatch.
		txn.pkMoveLinkStagedBytes += int64(entry.accountedSize)
	}
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
		panic(fmt.Sprintf(
			"BUG: workspace entry changed outside accounting helpers: %s.%s typ=%d rows=%d accounted=%d actual=%d",
			entry.databaseName, entry.tableName, entry.typ, entry.bat.RowCount(),
			entry.accountedSize, entry.bat.Size(),
		))
	}
	if txn.workspaceSize < entry.accountedSize {
		panic("BUG: workspace size accounting underflow")
	}

	txn.workspaceSize -= entry.accountedSize
	if entry.typ == INSERT &&
		isPkMoveLinkTableInsert(entry.databaseName, entry.tableName) {
		txn.pkMoveLinkStagedBytes -= int64(entry.accountedSize)
		if txn.pkMoveLinkStagedBytes < 0 {
			panic("BUG: pk-move link staged-bytes accounting underflow")
		}
	}
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
	if entry.bat.RowCount() == 0 {
		// A zero-row entry's index is provably empty: fail closed to the
		// FilterVector path instead of charging a map that can never
		// surface a row.
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

func (txn *Transaction) shrinkWorkspaceEntryBatchLocked(idx int, sels []int64) error {
	entry := &txn.writes[idx]
	if entry.bat == nil {
		return nil
	}
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
	bat := entry.bat
	if len(sels) == 0 {
		delete(txn.batchSelectList, bat)
		return nil
	}

	slices.Sort(sels)
	sels = slices.Compact(sels)
	// shrinkBatchWithRowids renumbers the SURVIVORS' row offsets inside the
	// same block, so every pk-move provenance cell naming a survivor must
	// travel to the new identity: link old/new row ids, the origin ledger,
	// and the pending-heal bindings. Without the carry the next prune reads
	// the stale identity as dead and retires a perfectly live link, and the
	// apply side replays the move as DELETE+INSERT — declared ON DELETE
	// actions would destroy dependents of a row that moved. Capture the
	// survivor identities BEFORE the shrink rewrites them; the renumbering
	// makes the pre-shrink values unreadable afterwards.
	var oldRowIds []types.Rowid
	var deadRowIds map[types.Rowid]struct{}
	if entry.typ == INSERT && len(bat.Vecs) > 0 &&
		bat.Vecs[0].GetType().Oid == types.T_Rowid {
		if n := min(bat.RowCount(), bat.Vecs[0].Length()); n > 0 {
			ids := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])[:n]
			gone := make(map[int32]struct{}, len(sels))
			for _, s := range sels {
				gone[int32(s)] = struct{}{}
			}
			oldRowIds = make([]types.Rowid, 0, n-len(gone))
			deadRowIds = make(map[types.Rowid]struct{}, len(gone))
			for i := int32(0); i < int32(n); i++ {
				if _, dead := gone[i]; !dead {
					oldRowIds = append(oldRowIds, ids[i])
					continue
				}
				// The victims' rowids are re-minted for the survivors by
				// the renumbering, so provenance still keyed by them must
				// retire instead of grafting onto an unrelated row.
				deadRowIds[ids[i]] = struct{}{}
			}
		}
	}
	txn.unaccountWorkspaceEntryLocked(entry)
	txn.unaccountInsertKeyOffsetsLocked(entry)
	shrinkBatchWithRowids(bat, sels)
	txn.accountWorkspaceEntryLocked(entry)
	if len(oldRowIds) > 0 {
		vec := bat.Vecs[0]
		n := min(bat.RowCount(), vec.Length())
		newRowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)[:n]
		// A remap failure is returned, never swallowed: the surviving user
		// rows are already renumbered, so silently continuing would let the
		// prune retire now-dangling links and commit the update without its
		// provenance. Failing the boundary discards the whole workspace
		// region through statement rollback instead.
		if err := txn.remapPkMoveLinkRowIDsLocked(oldRowIds, newRowIds, deadRowIds); err != nil {
			return err
		}
		// Row identities the folded harvest caches built on moved: force a
		// rebuild exactly like the compaction re-key does.
		txn.pkMoveHarvestGen++
	}
	delete(txn.batchSelectList, bat)
	return nil
}

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
	// r26 review F2 (D109): provenance harvest needs the pk vector even for
	// composite keys, which duplicate checks deliberately reject. Resolve
	// both positions in ONE table lookup so the hot write path pays for a
	// single getTable.
	pkMovePkPos := -1
	if typ == INSERT || typ == DELETE {
		// resolvePKPositionsForWrite may reach Engine.Database, which can craft an
		// internal SQL on the current txn and reenter txn.Lock via
		// UpdateSnapshotWriteOffset. Resolve the PK positions before taking txn.Lock.
		pkCheckPos, pkCheckReady, pkMovePkPos, err = txn.resolvePKPositionsForWrite(
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
		if typ == INSERT {
			// WriteBatch prepends rowid at attr 0 for inserts after this metadata lookup.
			if pkCheckPos >= 0 {
				pkCheckPos++
			}
			if pkMovePkPos >= 0 {
				pkMovePkPos++
			}
		}
	}

	// Retention-ledger genesis: the producer's marked identity batch pays the
	// keyed committed-state probe here, before the workspace lock — per
	// address and registration epoch exactly once (D159/D161/D165). The
	// locked admission re-consults the epoch map as the authority.
	var pkMoveGenesisVerdicts map[uint64]pkMoveGenesisProbeVerdict
	pkMoveGenesisMarked := typ == INSERT && bat != nil && bat.RowCount() > 0 &&
		isPkMoveRetentionTableInsert(databaseName, tableName) &&
		PkMoveGenesisWriteMarked(ctx)
	if pkMoveGenesisMarked {
		// D206(B.1) SETTER SITE (1): the genesis registration insert is the
		// first fabric-owned protocol artifact this transaction stages, so
		// its expected internal conflicts are protocol-expected; idempotent,
		// so repeat touches fold to a no-op.
		client.MarkTxnExpectedConflictIfSupported(txn.op)
		if pkMoveGenesisVerdicts, err = txn.probePkMoveGenesis(
			ctx, accountId, bat); err != nil {
			return nil, err
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

		if pkMoveGenesisMarked {
			// Retention-ledger genesis: dedup through the epoch map, memo the
			// probe-hit registrations EXTERNAL, and stage only fresh misses as
			// OWNED. An all-deduped batch stages nothing — mirroring the
			// zero-row link path below, the caller discards the returned
			// (empty) row-id vector.
			if err := txn.admitPkMoveGenesisBatchLocked(accountId, bat, pkMoveGenesisVerdicts); err != nil {
				return nil, err
			}
			if bat.RowCount() == 0 {
				bat.Clean(txn.proc.Mp())
				genRowidVec = vector.NewVec(types.T_Rowid.ToType())
				return genRowidVec, nil
			}
		}
		if isPkMoveLinkTableInsert(databaseName, tableName) {
			// Back-fill the link's new_row_id from this transaction's staged
			// inserts: the replacement row's identity only exists here, and
			// the diff consumer refuses to fold provenance without it. A
			// fill failure fails the statement so incomplete provenance
			// never commits; the statement rollback restores whatever the
			// failed pass already changed.
			if err := txn.fillPkMoveLinkNewRowIDsLocked(accountId, bat); err != nil {
				return nil, err
			}
			if bat.RowCount() == 0 {
				// Every row was a same-key continuity marker this fill
				// consumed: staging the shrunken batch would retain its
				// varlena payloads (Shrink does not release areas, see
				// vector.go) and charge the coordinator budget for zero
				// visible rows — an ordinary bulk payload UPDATE that
				// moves no primary key would fail on provenance bytes
				// that ceased to exist. Release the buffers and skip
				// admission and retention: dump, prune and request
				// generation all skip zero-row entries, and the marker's
				// healing evidence already lives in the per-statement
				// ledger entries the fill recorded. Callers of WriteBatch
				// discard the returned workspace row id vector, so
				// returning a fresh empty one keeps the contract.
				bat.Clean(txn.proc.Mp())
				genRowidVec = vector.NewVec(types.T_Rowid.ToType())
				return genRowidVec, nil
			}
			// The batch enters the workspace next and link batches ride no
			// pre-commit dump: fail the statement BEFORE it lands when the
			// retained provenance would cross the coordinator budget, so a
			// multi-wave ferry cannot accumulate without bound.
			if err := txn.admitPkMoveLinkBatchLocked(bat); err != nil {
				return nil, err
			}
		}

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
		pkMovePkPos:        pkMovePkPos,
	}
	txn.computeInsertKeyRangeLocked(&e)
	txn.appendWorkspaceEntryLocked(e)
	txn.pkCount += bat.RowCount()

	return
}

func (txn *Transaction) dumpBatch(ctx context.Context, offset int) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.dumpBatchLocked(ctx, offset, -1)
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

// stmtStart pins the statement start used by the workspace force-dump
// safety valve; a negative value derives today's value from
// txn.offsets[txn.statementID-1]. The statement boundary (D120)
// establishes the rollback point — and with it the statementID — before
// dumping, so it passes the finished statement's pre-establishment start
// explicitly to keep the valve's semantics unchanged.
func (txn *Transaction) dumpBatchLocked(ctx context.Context, offset int, stmtStart int) error {
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
				if stmtStart < 0 {
					stmtStart = 0
					if txn.statementID > 0 {
						stmtStart = txn.offsets[txn.statementID-1]
					}
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

	if err := txn.dumpInsertBatchLocked(ctx, fs, offset, &pkCount, dumpAll); err != nil {
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
	} else if txn.maintWindow != nil {
		// D129: the windowed boundary dump keeps its spilled slots live
		// until the adoption husks them; move the pkCount subtraction
		// there so the counter keeps matching the in-memory rows at
		// every instant of the window.
		txn.maintWindow.spilledRows += pkCount
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
	dumpAll bool,
) error {

	// Check if force flush is enabled
	forceFlush := objectio.CNWorkspaceForceFlushInjected()

	_, _, skipTable := txn.dumpInsertTableSelectionLocked(offset, forceFlush)

	// Rows any staged pk-move provenance names by workspace row id:
	// dumping one of them mid-transaction would convert that pinned
	// identity into a durable object row id the provenance cannot name,
	// severing every in-transaction chain through the row. The pin is
	// row-based, so batches without any pinned row still dump, and the
	// commit dump flushes everything after the final prune.
	var pinnedRowIDs map[types.Rowid]struct{}
	if !dumpAll {
		pinnedRowIDs = txn.pkMoveStagedLinkPinnedRowIDsLocked()
		// Charge what the pin rule is about to retain, here at the only
		// point where that retention is decided: the keep predicate below
		// is batch-wide, so one pinned wide row holds its whole batch in
		// memory, and repeated small updates of wide rows spilled nothing
		// while the coordinator budget saw only the bookkeeping measure.
		// The measure is derived from the live workspace at this gate and
		// at every later budget check instead of being cached, so the
		// charge tracks the batches the workspace actually still holds.
		// Failing the check fails the dumping statement before any
		// workspace mutation below; a zero budget disables it.
		if len(pinnedRowIDs) > 0 && txn.pkMoveHasLiveStagedLinksLocked() {
			// Participation, the same predicate as the fill and delete
			// boundaries: the pin set names only link/heal-bound
			// identities now (origin-keyed rows ride the dump shadow
			// instead, r24 review F3), so a live staged link is the
			// cheapest sufficient predicate. A transaction whose only
			// provenance is origin bookkeeping — an ordinary bulk
			// non-key UPDATE — keeps an empty pin set and nothing here
			// is refused (r23 review F3).
			if err := txn.checkPkMoveBookkeepBudgetForPinnedLocked(pinnedRowIDs); err != nil {
				return err
			}
		}
	}

	// Resolve every table that will be flushed BEFORE mutating txn.writes:
	// getTable must never run while this goroutine holds the lock. See
	// resolveDumpTablesLocked for the window contract.
	tables, ok, err := txn.resolveDumpTablesLocked(ctx, offset, skipTable, INSERT, !dumpAll)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	lastWriteIndex := offset
	writes := txn.writes
	mp := make(map[workspaceTableKey][]*batch.Batch)
	// Spilling origin-keyed rows re-mints their workspace identities as
	// object row ids: shadow them so the durable-origin ledger can be
	// translated after the object lands (r24 review F3). Skipped at the
	// commit dump — no later statement can still name those identities.
	var shadows map[workspaceTableKey][]pkMoveDumpShadowRow
	captureShadow := !dumpAll && len(txn.pkMoveRowidOrigins) > 0
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
		if !dumpAll && txn.writes[i].typ == INSERT &&
			isPkMoveLinkTableInsert(txn.writes[i].databaseName, txn.writes[i].tableName) {
			// Mid-transaction (statement-boundary) dumps must keep pk-move
			// link rows in memory: the fixpoint prune has to re-evaluate
			// every staged link whenever a later statement kills a
			// recorded replacement, and a spilled link can no longer be
			// shrunk. The commit dump (dumpAll) flushes them after the
			// final prune.
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
			continue
		}
		if !dumpAll && txn.writes[i].typ == INSERT &&
			isPkMoveRetentionTableInsert(txn.writes[i].databaseName, txn.writes[i].tableName) &&
			txn.pkMoveGenesisAnyOwnedRowLocked(&txn.writes[i]) {
			// Retention genesis rows (D164/D166): an address's staged
			// identity row rides the SAME boundary keep lane as its links —
			// the prune cancellation shrinks staged rows physically, and a
			// spilled row could no longer die. The lane is a conjunction:
			// name-match AND membership in this transaction's OWNED genesis
			// set, so funnel KEEP/refresh upserts staged through ordinary
			// internal SQL never join the lane — they spill and stay
			// cancelable by the ordinary statement cut. TOMBSTONE-only
			// semantics are unaffected: ordinary INSERT staging keeps
			// spilling freely.
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
			continue
		}
		if !dumpAll && txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" &&
			len(pinnedRowIDs) > 0 {
			if pkMoveBatchHasPinnedRowID(txn.writes[i].bat, pinnedRowIDs) {
				// Keep the workspace identities the staged provenance
				// names: the successor pin matches departure and arrival
				// by exact row id, so a link- or heal-named row must
				// still be a workspace row when a later statement of
				// this transaction moves the same lineage onward.
				// Origin-ledger rows deliberately fall through: the
				// shadow above records them and the ledger translates
				// onto object identities (r24 review F3). Batches of
				// the same table without a pinned row keep spilling
				// normally, keeping retention bounded by the
				// provenance-relevant rows only. The commit dump flushes
				// the batch after the final prune, once intra-transaction
				// identity matching is settled.
				writes[lastWriteIndex] = writes[i]
				lastWriteIndex++
				continue
			}
		}

		keepElement := true
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
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
			if _, ok := tables[tbKey.tableKey]; ok {
				bat := txn.writes[i].bat
				*pkCount += bat.RowCount()
				if captureShadow {
					var shadowErr error
					if shadows, shadowErr = txn.recordPkMoveDumpShadowLocked(shadows, tbKey, &txn.writes[i]); shadowErr != nil {
						return shadowErr
					}
				}
				if txn.maintWindow == nil {
					// The windowed dump keeps the spilled slot's batch live
					// and accounted (append-only, D129): its workspace
					// charge AND its hydrated key-offset charge stay.
					txn.unaccountInsertKeyOffsetsLocked(&txn.writes[i])
					txn.unaccountWorkspaceEntryLocked(&txn.writes[i])
				}
				// skip rowid
				newBatch := batch.NewWithSize(len(bat.Vecs) - 1)
				newBatch.SetAttributes(bat.Attrs[1:])
				newBatch.Vecs = bat.Vecs[1:]
				newBatch.SetRowCount(bat.Vecs[0].Length())
				mp[tbKey] = append(mp[tbKey], newBatch)
				if txn.maintWindow != nil {
					// r31 review F1 (D129): the windowed boundary dump is
					// APPEND-ONLY — the spilled slot keeps its live,
					// accounted batch in place (the appended spill-file
					// entry carries the boundary tail's copy), the batch
					// memory is NOT cleaned here, and the slot index is
					// journaled for the adoption's husk. A rollback of
					// this boundary truncates the tail — spill files and
					// entries — and leaves this slot byte-identical to
					// the pre-maintenance state, whatever preceded it.
					txn.maintWindow.dumpedIdxs = append(txn.maintWindow.dumpedIdxs, i)
				} else {
					defer bat.Clean(txn.proc.GetMPool())

					keepElement = false
				}
			}
		}

		if keepElement {
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
		}
	}

	clearVacatedWriteSlots(writes, lastWriteIndex, len(writes))
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
		tbl := tables[tbKey.tableKey]

		tableDef := tbl.GetTableDef(txn.proc.Ctx)
		// r35 review F2 (D140): bounded producer admission reaches the
		// dump at the persistence choke point. The sinker runs this
		// observer after each output object reserves its name and before
		// its first byte reaches storage — including the threshold-driven
		// flushes inside Write — so an ownership refusal aborts the dump
		// BEFORE any byte exists instead of growing retained inventory
		// past the hard bound across retries and closes. Every booked
		// slot reconciles on exactly one terminal below: released on the
		// healthy registration, released or carried into
		// failedWaveOrphans by the failure paths.
		booked := 0
		s3Writer = colexec.NewCNS3DataWriterForService(
			txn.proc.GetService(), txn.proc.GetMPool(), fs, tableDef, -1, false,
			ioutil.WithUnpublishedNameObserver(func(octx context.Context, name string) error {
				if err := reserveDurableOrphanOwnership(txn.engine.fs, 1); err != nil {
					return err
				}
				booked++
				return nil
			}),
		)

		for _, bat = range mp[tbKey] {
			if err = s3Writer.Write(txn.proc.Ctx, bat); err != nil {
				txn.reclaimDumpWriterOutputLocked(s3Writer, booked)
				return err
			}
		}

		if stats, err = s3Writer.Sync(txn.proc.Ctx); err != nil {
			txn.reclaimDumpWriterOutputLocked(s3Writer, booked)
			return err
		}

		if len(shadows[tbKey]) != 0 {
			// The writer sorted the wave by pk on flush, so join the
			// shadow against the written objects by pk value and
			// translate the durable-origin ledger onto the derived
			// identities; parity loss here fails the dump loudly.
			mapping, shadowErr := txn.resolvePkMoveDumpShadowLocked(
				ctx, fs, stats, tableDef, shadows[tbKey])
			if shadowErr != nil {
				// r34 review F2 (D138): the Sync above already
				// persisted stats; reclaim them before exiting.
				txn.reclaimUnregisteredDumpLocked(fs, stats)
				return shadowErr
			}
			if transErr := txn.translatePkMoveOriginDumpShadowLocked(mapping); transErr != nil {
				// r38 review F2 (D144): a carry-journal admission
				// refusal fails the dump before any provenance map
				// mutates; the persisted stats are reclaimed exactly
				// like a parity loss above.
				txn.reclaimUnregisteredDumpLocked(fs, stats)
				return transErr
			}
		}

		fileName = stats[0].ObjectLocation().String()
		if bat, err = s3Writer.FillBlockInfoBat(); err != nil {
			// r34 review F2 (D138): same unregistered interval.
			txn.reclaimUnregisteredDumpLocked(fs, stats)
			return err
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
			// r34 review F2 (D138): the registration failed after the
			// output persisted; it is still unowned. Its D140 admission
			// bookings reconcile inside the reclaim.
			txn.reclaimUnregisteredDumpLocked(fs, stats)
			return err
		}

		// r35 review F2 (D140): the registration adopted this wave's
		// output into the workspace lifecycle — it is orphanable no
		// longer, so the admission bookings return unused.
		releaseDurableOrphanReservation(txn.engine.fs, booked)

		s3Writer.Close()

		s3Writer = nil
	}

	return nil
}

func (txn *Transaction) dumpDeleteBatchLocked(
	ctx context.Context,
	fs fileservice.FileService,
	offset int,
) error {

	// See the comment in dumpInsertBatchLocked: tables must be resolved
	// while txn.writes is still consistent, with the lock released.
	tables, ok, err := txn.resolveDumpTablesLocked(ctx, offset, nil, DELETE, false)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	deleteCnt := 0
	lastWriteIndex := offset
	writes := txn.writes
	mp := make(map[workspaceTableKey][]*batch.Batch)
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
			if _, ok := tables[tbKey.tableKey]; ok {
				bat := txn.writes[i].bat
				deleteCnt += bat.RowCount()
				txn.unaccountInsertKeyOffsetsLocked(&txn.writes[i])
				txn.unaccountWorkspaceEntryLocked(&txn.writes[i])

				newBat := batch.NewWithSize(len(bat.Vecs))
				newBat.SetAttributes(bat.Attrs)
				newBat.Vecs = bat.Vecs
				newBat.SetRowCount(bat.Vecs[0].Length())

				mp[tbKey] = append(mp[tbKey], newBat)
				defer bat.Clean(txn.proc.GetMPool())

				keepElement = false
			}
		}

		if keepElement {
			writes[lastWriteIndex] = writes[i]
			lastWriteIndex++
		}
	}

	clearVacatedWriteSlots(writes, lastWriteIndex, len(writes))
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
		tbl := tables[tbKey.tableKey]

		pkCol = plan2.PkColByTableDef(tbl.GetTableDef(txn.proc.Ctx))
		s3Writer = colexec.NewCNS3TombstoneWriterForService(
			txn.proc.GetService(), txn.proc.GetMPool(), fs, plan2.ExprType2Type(&pkCol.Typ), -1,
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
			return err
		}

		if err = s3Writer.Close(); err != nil {
			return err
		}
	}

	return nil
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
	midTxn bool,
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
		if typ == INSERT && midTxn {
			// Statement-boundary dumps keep these entries in memory (the
			// callers' keep guards), so resolving them through the
			// catalog is wasted work — and for the link table itself an
			// unnecessary catalog round-trip on every moving statement.
			if isPkMoveLinkTableInsert(e.databaseName, e.tableName) {
				continue
			}
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

// resolvePKCheckPosForWrite resolves the pk position duplicate checks use.
// Semantics unchanged: composite primary keys are deliberately NOT resolved
// here (duplicate checks reject composite batches). The provenance position
// lives in resolvePKPositionsForWrite (r26 review F2).
func (txn *Transaction) resolvePKCheckPosForWrite(
	ctx context.Context,
	typ int,
	accountId uint32,
	databaseName, tableName string,
	tableId uint64,
	bat *batch.Batch,
) (int, bool, error) {
	pos, ready, _, err := txn.resolvePKPositionsForWrite(
		ctx, typ, accountId, databaseName, tableName, tableId, bat)
	return pos, ready, err
}

// resolvePKPositionsForWrite resolves, in one table lookup, both pk vector
// positions of a write batch: the duplicate-check position (composite keys
// stay -1, exactly as resolved before r26) and the pk-move provenance
// position pkMovePkPos, which ALSO resolves the materialized __mo_cpkey
// column of composite-keyed tables (r26 review F2 / D109). Non-keyed and
// fake-pk tables resolve -1 for both.
func (txn *Transaction) resolvePKPositionsForWrite(
	ctx context.Context,
	typ int,
	accountId uint32,
	databaseName, tableName string,
	tableId uint64,
	bat *batch.Batch,
) (int, bool, int, error) {
	if bat == nil || bat.RowCount() == 0 {
		return -1, true, -1, nil
	}

	if typ != INSERT && typ != DELETE {
		return -1, true, -1, nil
	}

	if tableId == catalog.MO_TABLES_ID ||
		tableId == catalog.MO_COLUMNS_ID ||
		tableId == catalog.MO_DATABASE_ID {
		return -1, true, -1, nil
	}
	if txn.engine == nil {
		return -1, false, -1, nil
	}

	tbl, err := txn.getTable(ctx, accountId, databaseName, tableName)
	if err != nil {
		return -1, false, -1, err
	}
	tableDef := tbl.GetTableDef(defines.AttachAccountId(ctx, accountId))
	if tableDef == nil || tableDef.Pkey == nil {
		return -1, true, -1, nil
	}

	pkName := tableDef.Pkey.PkeyColName
	if pkName == "" ||
		pkName == catalog.FakePrimaryKeyColName {
		return -1, true, -1, nil
	}

	// Provenance also harvests composite keys: their batches materialize
	// the composed pk column, which is exactly what the fold/shadow
	// readers encode (r26 review F2).
	isCPKey := pkName == catalog.CPrimaryKeyColName

	if typ == DELETE {
		if len(bat.Vecs) < 2 {
			logutil.Warnf("delete has no pk vector, database:%s, table:%s", databaseName, tableName)
			return -1, false, -1, nil
		}
		// the delete batch layout is [rowid, pk]
		if isCPKey {
			return -1, true, 1, nil
		}
		return 1, true, 1, nil
	}

	pkPos := -1
	for i, attr := range bat.Attrs {
		if attr == pkName {
			pkPos = i
			break
		}
	}
	if pkPos < 0 {
		for i, attr := range bat.Attrs {
			if strings.EqualFold(attr, pkName) {
				pkPos = i
				break
			}
		}
	}
	if pkPos >= 0 {
		if isCPKey {
			// duplicate checks keep rejecting composite batches; only the
			// provenance readers consume this position.
			return -1, true, pkPos, nil
		}
		return pkPos, true, pkPos, nil
	}

	logutil.Warnf("pk column %s not found in write attrs, database:%s, table:%s, attrs:%v",
		pkName, databaseName, tableName, bat.Attrs)
	return -1, false, -1, nil
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
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
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
			txn.registerCNObjects(*oid, accountId, databaseId, tableId, copied, databaseName, tableName, autoIncrEpoch, autoIncrEpochKnown)
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
		pkCheckPos:         -1,
		pkCheckReady:       true,
		pkMovePkPos:        -1,
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
	txn.beginCatalogMutation()
	defer txn.endCatalogMutation()
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
			txn.registerCNObjects(*oid, accountId, databaseId, tableId, copied, databaseName, tableName, autoIncrEpoch, autoIncrEpochKnown)
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
) (*batch.Batch, error) {
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

	// r29 review F2 (D124): every diverted deletion is tagged with the
	// statement that executed it, so a failed statement boundary keeps the
	// finished statements' pending deletions for its retry while a
	// statement rollback drops only the rolled statement's own. deleteBatch
	// runs without the workspace lock (parallel producers on one
	// transaction, see below), and statement boundaries never interleave
	// with a statement's own producers, so read the id under a short lock.
	txn.Lock()
	deleteStmtID := txn.statementID
	txn.Unlock()

	for i, rowid := range rowids {

		blkid := rowid.CloneBlockID()
		deleteBlkId[blkid] = true
		mp[rowid] = 0
		rowOffset := rowid.GetRowOffset()

		if server.GetCnSegmentType(rowid.BorrowSegmentID(), tableId, txn.op.Txn().ID) == colexec.TxnWorkspaceUnCommitType {
			txn.deletedBlocks.addDeletedBlocks(&blkid, []int64{int64(rowOffset)}, deleteStmtID)
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
	// cn rowId antiShrink: the diverted rows' pk deaths are recorded by
	// the divert/delete boundary behind a single lock hold. The pk-move
	// provenance maps are shared with the update pipeline's link staging,
	// which holds txn.Lock; deleteBatch itself runs without it (parallel
	// UpdateWriteS3 producers on one transaction can each reach
	// source.Delete), so the divert hook gets its own critical section
	// instead of racing the staging writers.
	if err := txn.notePkMoveDivertedDeletes(tableId, bat, cnRowIdOffsets); err != nil {
		return bat, err
	}
	bat.Shrink(cnRowIdOffsets, true)
	if bat.RowCount() == 0 {
		return bat, nil
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
	// The rows about to be Shrunk away were raw-workspace deaths: they
	// excluded rows from uncommitted INSERT batches and leave neither a
	// tombstone nor a diverted-death record. Note them through the same
	// provenance boundary as the diverted rows above so a by-key lineage
	// binding cannot outlive the generation it names.
	if err := txn.notePkMoveSwallowedDeletes(tableId, bat, sels); err != nil {
		vector.PutSels(sels)
		return bat, err
	}
	bat.Shrink(sels, true)
	vector.PutSels(sels)
	return bat, nil
}

func (txn *Transaction) addBatchSelectionsLocked(bat *batch.Batch, sels []int64) {
	selections := append(txn.batchSelectList[bat], sels...)
	slices.Sort(selections)
	txn.batchSelectList[bat] = slices.Compact(selections)
	// Newly excluded rows invalidate every incremental provenance fold
	// that already harvested them (the pk-move harvest cache versions on
	// this counter).
	txn.batchSelectListGen++
}

func (txn *Transaction) selectAllBatchRowsLocked(bat *batch.Batch) {
	selections := txn.batchSelectList[bat][:0]
	for i := range bat.RowCount() {
		selections = append(selections, int64(i))
	}
	txn.batchSelectList[bat] = selections
	// See addBatchSelectionsLocked.
	txn.batchSelectListGen++
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

	if len(txn.batchSelectList) > 0 {
		for i := range txn.writes {
			if sels, ok := txn.batchSelectList[txn.writes[i].bat]; ok {
				if err := txn.shrinkWorkspaceEntryBatchLocked(i, sels); err != nil {
					return err
				}
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

			merged := false
			for j := i + 1; j < len(idxes); j++ {
				b := &txn.writes[idxes[j]]
				if b.bat != nil && a.tableId == b.tableId && a.databaseId == b.databaseId &&
					a.bat.RowCount()+b.bat.RowCount() <= objectio.BlockMaxRows {
					merged = true
					if err = txn.mergeWorkspaceEntryBatchesLocked(ctx, idxes[i], idxes[j]); err != nil {
						return err
					}
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
				oldRowIds := append([]types.Rowid(nil),
					vector.MustFixedColNoTypeCheck[types.Rowid](a.bat.Vecs[0])...)
				for x := range a.bat.RowCount() {
					if err = vector.SetFixedAtWithTypeCheck[objectio.Rowid](a.bat.Vecs[0], x, rowIds[x]); err != nil {
						rowIdVector.Free(txn.proc.GetMPool())
						return err
					}
				}
				// Recorded pk-move replacement identities name the
				// pre-merge workspace row ids of these rows: carry the
				// links through the re-key so liveness keeps matching the
				// rows it was recorded against.
				// A remap failure must abort the merge: these user rows are
				// already re-keyed, and continuing would let the prune retire
				// the now-dangling links and commit the update without
				// provenance.
				if err = txn.remapPkMoveLinkRowIDsLocked(oldRowIds[:a.bat.RowCount()], rowIds[:a.bat.RowCount()], nil); err != nil {
					rowIdVector.Free(txn.proc.GetMPool())
					return err
				}

				rowIdVector.Free(txn.proc.GetMPool())
			}
		}

		return nil
	}

	// this threshold may have a bad effect on the performance
	if inserts.Count()+deletes.Count() >= 30 {
		// Compaction re-keys merged INSERT batches and drops their source
		// entries: any folded pk->rowid harvest is stale from here on.
		txn.pkMoveHarvestGen++
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

		if txn.maintWindow != nil {
			// r31 review F1 (D129): the merge fold's husks keep their
			// slots for the whole window — compacting here would move
			// acknowledged entries while the boundary's rollback cut
			// still separates regions by place. The adoption (or, after
			// the tail cut, the rollback) compacts instead.
			txn.maintWindow.mergeHusked = true
		} else {
			n := 0
			for i := range txn.writes {
				if txn.writes[i].bat == nil {
					continue
				}
				txn.writes[n] = txn.writes[i]
				n++
			}

			clearVacatedWriteSlots(txn.writes, n, len(txn.writes))
			txn.writes = txn.writes[:n]
		}

		var sortBuf []int64
		var shuffleBuf []byte
		for i := range txn.writes {
			if txn.writes[i].bat != nil && txn.writes[i].typ == DELETE && txn.writes[i].bat.RowCount() > 1 {
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

// r27 review F2/F5 (D115): a compaction slot is the boundary's
// ownership record for ONE dirty object's rewrite. Workers fill slots
// through the pure rewrite pipeline (persist replacement, provenance
// shadow, read-back resolve) but touch NOTHING transaction-visible:
// they never register into txn.writes, never release an original
// batch, and never reclaim files. The boundary publishes replacements
// and releases originals only after the COMPLETE wave succeeded, so a
// failed wave leaves the staged data of every earlier statement
// byte-stable and recoverable by the ordinary statement rollback; the
// slot's fileName makes the boundary the explicit cleanup owner of a
// persisted-but-unpublished replacement until publication lands.
type compactSlot struct {
	stats     objectio.ObjectStats
	tbl       *txnTable
	tbKey     workspaceTableKey
	writeName string
	bat       *batch.Batch
	newStats  []objectio.ObjectStats
	fileName  string
	mig       pkMoveRewriteMigration
	err       error
	// orphans names persisted replacement files whose in-writer
	// reclaim failed (r28 review F3): the failed-wave cleanup reclaims
	// them with a background context, so every unpublished file keeps
	// a cleanup owner from writer defer to boundary end.
	orphans []string
}

// runCompactWave submits one compaction worker per slot and returns every
// worker abort as a collected error. r26 review F1: a failed worker panics
// via panicWhenFailed, and the ants pool's default recovery swallows it —
// the boundary then carried a partial migration, reclaimed the original
// objects and reported success, silently discarding surviving rows. Every
// worker abort (panic or submit failure) becomes a collected error so the
// boundary fails BEFORE any provenance carry or object reclaim; the caller
// aborts the enclosing statement/commit atomically and the original
// objects stay registered and protected.
//
// finishSlot runs exactly once per submitted slot and appends the abort
// BEFORE Done releases the parent wait, on every completion path. The
// panic path reaches finishSlot through the pool's PanicHandler: ants runs
// it synchronously in the aborted worker's goroutine while it recovers the
// panic (ants v2.12.0 worker.go:57-66), so recovery lives in the pool
// library and no in-code recover() is needed (checkrecover policy).
func (txn *Transaction) runCompactWave(slots []compactSlot, compactFunc func(*compactSlot)) []error {
	var workerErrs []error
	var workerErrMu sync.Mutex
	var waiter sync.WaitGroup
	finishSlot := func(err error) {
		if err != nil {
			workerErrMu.Lock()
			workerErrs = append(workerErrs, err)
			workerErrMu.Unlock()
		}
		waiter.Done()
	}

	if txn.compactWorker == nil {
		txn.compactWorker, _ = ants.NewPool(min(runtime.NumCPU(), 4),
			ants.WithPanicHandler(func(r any) {
				finishSlot(moerr.NewInternalErrorNoCtx(
					fmt.Sprintf("compaction worker aborted: %v", r)))
			}))
		defer func() {
			txn.compactWorker.Release()
			txn.compactWorker = nil
		}()
	}

	for i := range slots {
		waiter.Add(1)
		if submitErr := txn.compactWorker.Submit(func() {
			compactFunc(&slots[i])
			finishSlot(nil)
		}); submitErr != nil {
			finishSlot(moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("compaction worker submit failed: %v", submitErr)))
		}
	}
	waiter.Wait()
	return workerErrs
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

	// Identities the pk-move bookkeeping names must ride the dirty-object
	// rewrites below: the rewrite re-mints every surviving row's id, and
	// a stale origin/link/heal key would sever the lineage of every later
	// move of that row. nil for ordinary transactions — zero reads then
	// (r25 review F2).
	migrateRelevant := txn.pkMoveRewriteMigrationRelevantLocked()
	var migrations pkMoveRewriteMigration

	// object --> blk id --> deletion
	objBlkDeletion := make(map[objectio.ObjectId]map[objectio.Blockid][]int64)
	objTables := make(map[objectio.ObjectId]workspaceTableKey)

	affectedBats := make(map[*batch.Batch]struct{})

	// r29 review F2 (D124): the ledger is NOT cleared on exit anymore. A
	// failed wave consumed nothing, so its deletions stay pending; on
	// success the drain moves into the staged boundary finalization below,
	// and only the boundary's adoption (or its rollback, which restores
	// the drained entries) decides their fate.

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

	locker := sync.Mutex{}

	// stagedCarry is the wave-staged form of a clean-object carry (r28
	// review F2, D121): the cloned result batch plus the index of the
	// source entry every registration field is read from at publication
	// time. Until publication the ORIGINAL entry remains the only
	// reference to the carried object file, so a failed wave cleans the
	// clone batch and never touches the file.
	type stagedCarry struct {
		bat      *batch.Batch
		stats    objectio.ObjectStats
		entryIdx int
	}

	compactFunc := func(slot *compactSlot) {
		// waiter accounting belongs to runCompactWave's submitter: on a
		// panic its error collection must complete BEFORE Done releases
		// the parent wait (r26 review F1 follow-up — the r26 race gate
		// caught the waiter observing the error slice mid-append); see
		// runCompactWave for the recovery-boundary contract.

		// need to rewrite the whole object
		stats := slot.stats
		objId := stats.ObjectName().ObjectId()

		tbKey := objTables[*objId]
		slot.tbKey = tbKey

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

		// resolveCompactTablesLocked guarantees every table referenced by
		// txn.deletedBlocks is in the map
		rel, ok := tables[tbKey.tableKey]
		if !ok {
			panicWhenFailed(moerr.NewInternalErrorNoCtx(
				"table not pre-resolved for object compaction"), "get table failed")
		}

		tbl, ok := rel.(*txnTable)
		if !ok {
			delegate := rel.(*txnTableDelegate)
			tbl = delegate.origin
		}
		slot.tbl = tbl

		locker.Lock()
		tbl.ensureSeqnumsAndTypesExpectRowid()
		locker.Unlock()

		bat, newStats, orphans, fileName, err := tbl.rewriteObjectByDeletion(ctx, stats, objBlkDeletion[*objId])
		if err != nil {
			// r28 review F3 (D122): the writer ran its reclaim on a
			// cancellation-decoupled context; any files it could not
			// delete arrive here as orphans so the failed-wave cleanup
			// reclaims them — every unpublished file keeps a cleanup
			// owner from writer defer to boundary end.
			slot.err = moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("rewrite object by deletion failed: %v", err))
			slot.orphans = orphans
			return
		}
		// From here on the replacement object can already exist in the
		// shared FS; the slot name makes the boundary its cleanup owner
		// until publication (r27 review F5).
		slot.bat = bat
		slot.newStats = newStats
		slot.fileName = fileName

		// Shadow the provenance-named survivors of THIS object before and
		// after the rewrite, joining by pk value: positional assumptions
		// are impossible because the writer re-sorts the wave, and a
		// parity loss must fail loudly instead of publishing severed
		// lineage (r25 review F2). Pure computation — no txn state.
		if migrateRelevant != nil {
			proc := tbl.proc.Load()
			shadow, shErr := recordPkMoveRewriteShadow(
				ctx, proc.GetMPool(), tbl.getTxn().engine.fs, stats,
				tbl.tableDef, objBlkDeletion[*objId], migrateRelevant)
			if shErr != nil {
				slot.err = moerr.NewInternalErrorNoCtx(
					fmt.Sprintf("pk-move rewrite shadow failed: %v", shErr))
				return
			}
			if len(shadow) != 0 {
				sharedFS, fsErr := colexec.GetSharedFSFromProc(proc)
				if fsErr != nil {
					slot.err = moerr.NewInternalErrorNoCtx(
						fmt.Sprintf("pk-move rewrite resolve lost its filesystem: %v", fsErr))
					return
				}
				mapping, rsErr := resolvePkMoveDumpShadow(
					ctx, proc.GetMPool(), sharedFS, slot.newStats, tbl.tableDef, shadow)
				if rsErr != nil {
					slot.err = moerr.NewInternalErrorNoCtx(
						fmt.Sprintf("pk-move rewrite resolve failed: %v", rsErr))
					return
				}
				slot.mig.add(mapping)
			}
		}

		writeName := tbl.tableName
		if tbl.db.databaseId == catalog.MO_CATALOG_ID && tbl.tableId == catalog.MO_COLUMNS_ID &&
			tbKey.name == catalog.MO_COLUMNS_UPDATE {
			writeName = tbKey.name
		}
		slot.writeName = writeName
	}

	dirtyObject := make([]objectio.ObjectStats, 0, 1)
	// Batches of every phase-1-scanned INSERT entry the boundary releases
	// after — and ONLY after — the complete wave succeeds (r27 review
	// F2). Releasing inside the scan (the pre-r27 shape) destructively
	// modified whichever earlier statement wrote the entry when a later
	// worker failure aborted the boundary: statement rollback truncates
	// only the current tail and can never restore a released batch. The
	// staging holds the batch IDENTITY, not the entry index (r30 review
	// F1, D126): the boundary dump and the workspace-merge compaction
	// compact txn.writes in place before adoption, so a stored index
	// silently aliases a neighbour.
	releaseBats := make([]*batch.Batch, 0, 1)

	// r28 review F2 (D121): a clean-object carry re-registers an object
	// that a scanned (soon released) batch references while its dirty
	// siblings get rewritten. Stage carries during the scan and publish
	// them inside the SAME atomic wave as the dirty replacements: the
	// pre-r28 immediate registration outlived a worker failure until
	// statement rollback, whose range GC then deleted files the retained
	// ORIGINAL entries still referenced, severing earlier acknowledged
	// statements. A staged carry holds only its clone batch; its file
	// stays owned by the original entry until publication.
	stagedCarries := make([]stagedCarry, 0, 1)
	cleanStagedCarryBats := func() {
		for k := range stagedCarries {
			if stagedCarries[k].bat != nil {
				stagedCarries[k].bat.Clean(txn.proc.Mp())
				stagedCarries[k].bat = nil
			}
		}
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

		offset := 0
		col, area := vector.MustVarlenaRawData(entry.bat.Vecs[1])
		for j := range len(col) {
			stats := objectio.ObjectStats(col[j].GetByteSlice(area))

			if objBlkDeletion[*stats.ObjectName().ObjectId()] == nil {
				// clean object, no deletion on it: stage the carry for
				// publication inside the same atomic wave as the dirty
				// replacements below (r28 review F2, D121). Publication
				// happens only after the complete wave succeeds; until
				// then the staged clone is the only allocation, so a
				// staging failure cleans every staged clone and leaves
				// the original entries — the file owners — untouched.
				bat := colexec.AllocCNS3ResultBat(false)
				if err := bat.Vecs[0].UnionBatch(entry.bat.Vecs[0],
					int64(offset), int(stats.BlkCnt()), nil, txn.proc.Mp()); err != nil {
					bat.Clean(txn.proc.Mp())
					cleanStagedCarryBats()
					return err
				}
				if err := vector.AppendBytes(
					bat.Vecs[1], stats.Marshal(), false, txn.proc.Mp()); err != nil {
					bat.Clean(txn.proc.Mp())
					cleanStagedCarryBats()
					return err
				}

				bat.SetRowCount(bat.Vecs[0].Length())
				stagedCarries = append(stagedCarries, stagedCarry{
					bat:      bat,
					stats:    stats,
					entryIdx: i,
				})

			} else {
				dirtyObject = append(dirtyObject, stats)
			}

			offset += int(stats.BlkCnt())
		}

		// r27 review F2 (D115): do NOT release the original batch here —
		// the wave has not produced its replacements yet, and releasing
		// now would destructively modify whatever (possibly earlier)
		// statement wrote this entry if a later worker failure aborts the
		// boundary. The release happens only after the complete wave
		// succeeded, by batch identity at the adoption site (r30 review
		// F1, D126).
		releaseBats = append(releaseBats, entry.bat)
	}

	// r32 review F2 (D133): book cleanup ownership for the wave's
	// orphanable replacements BEFORE any worker persists one. The wave
	// emits at most one replacement file per dirty object (publication
	// registers stats[0], matching rewriteObjectByDeletion's single
	// output object), so len(dirtyObject) is the exact production bound.
	// Exhaustion fails the boundary before any replacement file exists —
	// sustained storage failure backpressures the producing statement
	// instead of growing retained orphan state without bound. Every exit
	// below reconciles exactly: published/reclaimed/never-produced slots
	// release, registry-retained slots convert at the close drain.
	waveReserved := len(dirtyObject)
	if waveReserved > 0 {
		if err := reserveDurableOrphanOwnership(txn.engine.fs, waveReserved); err != nil {
			cleanStagedCarryBats()
			return err
		}
	}

	slots := make([]compactSlot, len(dirtyObject))
	for i := range slots {
		slots[i].stats = dirtyObject[i]
	}

	// r26 review F1: a panicking or unsubmittable compaction worker is
	// collected as an error (see runCompactWave) and fails this boundary
	// atomically below, BEFORE any provenance carry or object reclaim.
	workerErrs := txn.runCompactWave(slots, compactFunc)

	// cleanFailedWave disposes slots [from, len): cleans their replacement
	// batches and reclaims the replacement files that were persisted but
	// never published into txn.writes (r27 review F5). Slots before `from`
	// were already registered; statement rollback's tail-cut owns those.
	// The reclaim names the plain object file (ObjectName): entry fileName
	// strings are objectio locations (name_extent_rows_id) and would miss
	// the real file with a tolerated not-found.
	// The cleanup owns every file its wave persisted until proven
	// otherwise. The returned retained count is the number of orphan
	// names handed to the durable registry through failedWaveOrphans —
	// their D133 reservations stay booked (the close drain converts them
	// name-for-name); every other reserved slot reconciles by release at
	// the call sites below (r32 review F2).
	cleanFailedWave := func(from int) (retained int) {
		orphans := make([]string, 0, len(slots)-from)
		for i := from; i < len(slots); i++ {
			if slots[i].bat != nil {
				slots[i].bat.Clean(txn.proc.Mp())
				slots[i].bat = nil
			}
			for j := range slots[i].newStats {
				orphans = append(orphans, slots[i].newStats[j].ObjectName().String())
			}
			slots[i].newStats = nil
			slots[i].fileName = ""
			// Persisted replacements whose in-writer reclaim failed
			// arrive from the writer defer (r28 review F3, D122); this
			// background-context cleanup is their remaining owner.
			orphans = append(orphans, slots[i].orphans...)
			slots[i].orphans = nil
		}
		// Unpublished staged carries own only their clone batches; their
		// object files belong to the original entries, which this cleanup
		// must never reclaim (r28 review F2, D121). Published carries
		// already dropped their clones, so this is a no-op for them.
		cleanStagedCarryBats()
		if len(orphans) == 0 {
			return 0
		}
		sharedFS, fsErr := colexec.GetSharedFSFromProc(txn.proc)
		if fsErr != nil {
			// r29 review F3 (D125): the wave was the last local owner, and
			// its cleanup failed before it began — retain the inventory so
			// the transaction close's GC drain reclaims it instead of
			// leaking ownerless files.
			txn.failedWaveOrphans = append(txn.failedWaveOrphans, orphans...)
			logutil.Warn("compact wave cleanup lost its filesystem; unpublished replacements retained",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("objs", orphans),
				zap.Error(fsErr))
			return len(orphans)
		}
		// Cleanup must survive query cancellation, mirroring gcFiles.
		if _, dErr := ioutil.DeleteUnpublishedObjects(context.Background(), sharedFS, orphans...); dErr != nil {
			// r29 review F3 (D125): retain the inventory for the close-time
			// GC drain. The drain tolerates already-deleted names, so
			// retaining the whole batch is safe even if this attempt
			// stopped partway through it.
			txn.failedWaveOrphans = append(txn.failedWaveOrphans, orphans...)
			logutil.Warn("compact wave cleanup could not reclaim unpublished replacements",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("objs", orphans),
				zap.Error(dErr))
			return len(orphans)
		}
		return 0
	}

	var waveErr error
	if len(workerErrs) > 0 {
		waveErr = errors.Join(workerErrs...)
	}
	for i := range slots {
		if slots[i].err != nil {
			waveErr = errors.Join(waveErr, slots[i].err)
		}
	}
	if waveErr != nil {
		// Nothing was published and no original was released: return with
		// every staged entry untouched, so the enclosing statement rollback
		// restores the pre-boundary state exactly (r27 review F2).
		releaseDurableOrphanReservation(txn.engine.fs,
			waveReserved-cleanFailedWave(0))
		return waveErr
	}

	// Publish the staged clean-object carries first, mirroring the
	// pre-r28 scan registration order inside the same atomic wave (r28
	// review F2, D121). A failure here leaves the remaining carries
	// staged — cleanFailedWave cleans their clones — while carries
	// already published belong to the current statement's tail; the
	// statement-rollback range GC excludes files still referenced by
	// the retained original entries (D121), so their files survive the
	// tail cut.
	for k := range stagedCarries {
		c := &stagedCarries[k]
		entry := &txn.writes[c.entryIdx]
		if err := txn.writeFileLockedWithAutoIncrEpochKnown(
			INSERT,
			entry.accountId,
			entry.databaseId,
			entry.tableId,
			entry.databaseName,
			entry.tableName,
			c.stats.ObjectName().String(),
			c.bat,
			entry.tnStore,
			entry.autoIncrEpoch,
			entry.autoIncrEpochKnown,
		); err != nil {
			pubErr := moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("write txn file failed: %v", err))
			releaseDurableOrphanReservation(txn.engine.fs,
				waveReserved-cleanFailedWave(0))
			return pubErr
		}
		c.bat.Clean(txn.proc.Mp())
		c.bat = nil
	}

	// Publish the replacements in dirty-object order. Slots published
	// before a failure belong to the current statement's tail — the
	// statement rollback tail-cut reclaims them; the rest are reclaimed
	// here.
	for i := range slots {
		slot := &slots[i]
		if err := txn.writeFileLockedWithAutoIncrEpochKnown(
			INSERT,
			slot.tbl.accountId,
			slot.tbl.db.databaseId,
			slot.tbl.tableId,
			slot.tbl.db.databaseName,
			slot.writeName,
			slot.fileName,
			slot.bat,
			txn.tnStores[0],
			slot.tbKey.autoIncrEpoch,
			slot.tbKey.autoIncrEpochKnown,
		); err != nil {
			pubErr := moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("write txn file failed: %v", err))
			releaseDurableOrphanReservation(txn.engine.fs,
				waveReserved-cleanFailedWave(i))
			return pubErr
		}
		slot.bat.Clean(txn.proc.Mp())
		slot.bat = nil
		migrations.oldIds = append(migrations.oldIds, slot.mig.oldIds...)
		migrations.newIds = append(migrations.newIds, slot.mig.newIds...)
	}

	// D133: the complete wave succeeded — every replacement file is now
	// published and owned by the statement tail, so the orphan registry
	// never needs this wave's reserved slots (no orphan was retained).
	releaseDurableOrphanReservation(txn.engine.fs, waveReserved)

	// r36 review F1 (D141, folded by r38 review F1/F2 into D143/D144):
	// journal the wave's migration pairs — effective-only, coalesced, and
	// budget-admitted BEFORE the remap mutates any bookkeeping — in the
	// transaction-level carry journal. D123's staged pre-carry map
	// snapshot is gone: RollbackLastStatement's walker applies the exact
	// inverse against the CURRENT maps, so a boundary failure after the
	// carry composes with the dump's translations and the transfer's
	// carries instead of pitching the ledger back over them, and the
	// retry re-runs this wave on byte-identical inputs. An admission
	// refusal aborts the boundary statement-atomically — the just-
	// published replacements die in the statement rollback's tail cut,
	// and no map has mutated yet.
	effOld, effNew, admErr := txn.pkMoveCarryJournalAdmitLocked(
		migrations.oldIds, migrations.newIds)
	if admErr != nil {
		return admErr
	}
	txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld, effNew)

	// One bookkeeping remap for the whole compaction wave; a failure
	// aborts the boundary (and its statement/commit) rather than
	// committing rows whose provenance dangles (r25 review F2).
	if err := txn.carryPkMoveProvenanceThroughRekeyLocked(
		migrations.oldIds, migrations.newIds); err != nil {
		return err
	}

	// The complete wave succeeded. Its destructive tail — releasing the
	// original batches, reclaiming the superseded dirty objects, and
	// draining the consumed deletion ledger — stays STAGED rather than
	// applied (r29 review F1, D123): the enclosing boundary still has
	// fallible work below (the workspace dump and, on RC, the snapshot
	// transfer), and a failure there runs RollbackLastStatement, which
	// truncates the replacement registrations this wave just published.
	// Applying the releases now would destroy acknowledged originals with
	// no live replacement and no compensation. The boundary applies the
	// tail at adoption (IncrStatementID, or the final dump in Commit); a
	// rollback discards it and restores the drained ledger (r29 review
	// F2, D124), so the retry re-runs this wave on byte-identical inputs.
	pending := &boundaryFinalization{
		releaseBats: releaseBats,
		dirtyObject: dirtyObject,
	}
	pending.ledger, pending.ledgerStmts = txn.deletedBlocks.drain()
	txn.pendingBoundaryFinalization = pending

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
	// Retire pk-move link rows whose staged replacement died inside this
	// transaction BEFORE compaction re-keys staged inserts: the prune
	// matches recorded replacement identities against staged row ids, and
	// mergeTxnWorkspaceLocked rewrites exactly those identities. The
	// consumer's commit-timestamp claiming must never meet a link whose
	// landed generation the moving transaction itself removed.
	txn.pruneDeadPkMoveLinkRowsLocked()
	if err := txn.mergeTxnWorkspaceLocked(ctx); err != nil {
		txn.Unlock()
		return nil, err
	}
	if err := txn.dumpBatchLocked(ctx, -1, -1); err != nil {
		txn.Unlock()
		return nil, err
	}
	// r29 review F1 (D123): the commit path runs no later adoption for a
	// compaction tail staged by the merge above; this final dump is its
	// irrevocability point. A commit failure after this rolls back the
	// whole transaction, applied tail included.
	txn.applyPendingBoundaryFinalizationLocked()
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

	tables := txn.collectTombstoneTransferTablesLocked()
	if err = transferInmemTombstones(ctx, txn, tables, start, end); err != nil {
		return err
	}

	if msg, injected := objectio.CNIncrStmtRCTransferObjectsFailedInjected(); injected {
		return moerr.NewInternalErrorNoCtx("rc statement-boundary tombstone object transfer failed by injection: " + msg)
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

	// For CCPR transactions, call OnTxnRollback to clean up the cache and GC objects
	// Skip normal GCObjsByIdxRange for CCPR transactions because:
	// 1. CCPR objects may be shared across transactions
	// 2. CCPRTxnCache.OnTxnRollback handles GC properly (only deletes when no other txn references the object)
	if txn.isCCPRTxn && txn.engine.ccprTxnCache != nil {
		txn.engine.ccprTxnCache.OnTxnRollback(txn.op.Txn().ID)
	} else {
		//to gc the s3 objs
		if err := txn.gcObjsByIdxRange(0, len(txn.writes)-1, cloneGCTxnRollback); err != nil {
			panic("Rollback txn failed: to gc objects generated by CN failed")
		}
	}
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

	// r26 review F3 (D113): transfer-retired tombstone objects never reached
	// commit serialization in any outcome, so their files are unpublished
	// garbage to collect exactly once here, on every close path.
	txn.gcRetiredTransferFiles()
	// r29 review F3 (D125): unpublished compaction replacements whose
	// in-writer and failed-wave reclaims both failed are unpublished
	// garbage in every close outcome as well; this drain is their last
	// owner.
	txn.gcFailedWaveOrphans()
	// r29 review F1 (D123): a compaction tail staged but never adopted
	// dies with the transaction — the close-range GC (rollback) or the
	// commit itself already owns every file it names.
	txn.pendingBoundaryFinalization = nil
	// r31 review F1 (D129): the close also consumes any maintenance
	// window a failed boundary left open (the transaction-level rollback
	// path never runs RollbackLastStatement). The journal only names
	// slots this very loop below unaccounts and cleans, so dropping it
	// loses nothing.
	txn.maintWindow = nil

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
