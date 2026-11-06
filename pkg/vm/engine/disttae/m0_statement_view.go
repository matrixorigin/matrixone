// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"slices"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// m0StatementReadView is captured by the transaction owner before any pipeline
// runs. A write batch, selection, or raw-delete map must never be borrowed from
// the workspace: UPDATE may mutate it while another scan is still consuming it.
// The mpool reservation bounds the retained Go metadata; vector payloads are
// independently charged by CloneWithoutAllocationAccount to the same pool.
type m0StatementReadView struct {
	refs        atomic.Int64
	owner       *Transaction
	snapshot    timestamp.Timestamp
	offset      int
	writes      []Entry
	deleted     map[uint64]map[types.Blockid][]int64
	selections  map[*batch.Batch][]int64
	mp          *mpool.MPool
	reservation []byte
}

func (v *m0StatementReadView) Release() {
	if v == nil {
		return
	}
	if v.refs.Add(-1) != 0 {
		return
	}
	for _, e := range v.writes {
		if e.bat != nil {
			e.bat.Clean(v.mp)
		}
	}
	v.writes = nil
	v.deleted = nil
	v.selections = nil
	v.mp.Free(v.reservation)
	v.reservation = nil
}

func (v *m0StatementReadView) acquire(txn *Transaction, offset int) error {
	if v.owner != txn || v.offset != offset {
		return moerr.NewInternalErrorNoCtx("M0: statement workspace binding changed")
	}
	if !v.snapshot.Equal(txn.op.SnapshotTS()) {
		// A pre-pipeline RC lock may refresh the snapshot. The normal retry
		// path must capture both workspace/deletion transfer and snapshot anew.
		return moerr.NewTxnNeedRetryNoCtx()
	}
	for {
		refs := v.refs.Load()
		if refs <= 0 {
			return moerr.NewInternalErrorNoCtx("M0: statement workspace view closed")
		}
		if v.refs.CompareAndSwap(refs, refs+1) {
			return nil
		}
	}
}

// CaptureM0StatementReadView is an internal optional workspace capability. The
// ordinary compile never calls it. Capture follows the workspace->deletedBlocks
// lock order used by writers; it performs no I/O, logtail wait, or RPC.
func (txn *Transaction) CaptureM0StatementReadView(ctx context.Context, offset int) (m0read.StatementReadView, error) {
	txn.Lock()
	defer txn.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	revision, stable := txn.CatalogVisibility()
	if !stable {
		return nil, moerr.NewTxnNeedRetryNoCtx()
	}
	if txn.removed || offset < 0 || offset > len(txn.writes) {
		return nil, moerr.NewInternalErrorNoCtx("M0: invalid statement workspace boundary")
	}
	if txn.deletedBlocks != nil {
		txn.deletedBlocks.RLock()
		defer txn.deletedBlocks.RUnlock()
	}
	// Charge before allocating indexes. Per-map/bucket allowance is deliberately
	// conservative, not a claim about precise Go heap layout.
	budget := uint64(256) + uint64(offset)*uint64(unsafe.Sizeof(Entry{})+128)
	budget += uint64(len(txn.writes)) * 256 // temporary stats-owner index, charged before allocation
	for _, e := range txn.writes[:offset] {
		budget += uint64(len(e.pkRange.min) + len(e.pkRange.max))
		if e.bat != nil {
			budget += uint64(len(e.bat.Vecs))*512 + uint64(len(e.bat.Attrs))*16
		}
		budget += uint64(len(txn.batchSelectList[e.bat]))*8 + 128
	}
	for _, e := range txn.writes {
		if e.typ == DELETE && e.fileName != "" && e.bat != nil {
			// The index below consumes the stats vector, not the batch row
			// hint. Charge its actual length even for inconsistent metadata.
			idx := slices.Index(e.bat.Attrs, catalog.ObjectMeta_ObjectStats)
			if idx >= 0 && idx < len(e.bat.Vecs) && e.bat.Vecs[idx] != nil {
				budget += uint64(e.bat.Vecs[idx].Length()) * 512
			}
		}
	}
	if txn.deletedBlocks != nil {
		for _, blocks := range txn.deletedBlocks.byTable {
			budget += 256
			for _, offs := range blocks {
				budget += 256 + uint64(len(offs))*8
			}
		}
	}
	if budget > uint64(^uint(0)>>1) {
		return nil, moerr.NewInternalErrorNoCtx("M0: statement workspace metadata too large")
	}
	v := &m0StatementReadView{owner: txn, snapshot: txn.op.SnapshotTS(), offset: offset, mp: txn.proc.Mp()}
	v.refs.Store(1)
	reservation, err := v.mp.Alloc(int(budget), true)
	if err != nil {
		return nil, err
	}
	v.reservation = reservation
	ok := false
	defer func() {
		if !ok {
			v.Release()
		}
	}()
	v.writes = make([]Entry, 0, offset)
	v.selections = make(map[*batch.Batch][]int64)
	for _, e := range txn.writes[:offset] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry := e
		entry.keyOffsets = nil
		entry.keyIndexAccountedSize = 0
		entry.accountedSize = 0
		entry.pkRange.min = slices.Clone(e.pkRange.min)
		entry.pkRange.max = slices.Clone(e.pkRange.max)
		if e.bat != nil {
			entry.bat, err = e.bat.CloneWithoutAllocationAccount(v.mp, true)
			if err != nil {
				return nil, err
			}
			entry.bat.Attrs = slices.Clone(e.bat.Attrs)
			v.selections[entry.bat] = slices.Clone(txn.batchSelectList[e.bat])
		}
		v.writes = append(v.writes, entry)
	}
	v.deleted = make(map[uint64]map[types.Blockid][]int64)
	if txn.deletedBlocks != nil {
		for table, blocks := range txn.deletedBlocks.byTable {
			copyBlocks := make(map[types.Blockid][]int64, len(blocks))
			for block, offs := range blocks {
				copyBlocks[block] = slices.Clone(offs)
			}
			v.deleted[table] = copyBlocks
		}
	}
	// The legacy flushed index is not relation-scoped. Every stat must have a
	// real write-entry owner; otherwise omitting it would silently lose deletes.
	known := make(map[objectio.ObjectStats]struct{})
	for _, e := range txn.writes {
		if e.typ != DELETE || e.fileName == "" || e.bat == nil || e.bat.IsEmpty() {
			continue
		}
		idx := slices.Index(e.bat.Attrs, catalog.ObjectMeta_ObjectStats)
		if idx < 0 || idx >= len(e.bat.Vecs) || e.bat.Vecs[idx] == nil || !e.bat.Vecs[idx].GetType().IsVarlen() {
			return nil, moerr.NewInternalErrorNoCtx("M0: flushed tombstone entry lacks object stats")
		}
		for i := 0; i < e.bat.Vecs[idx].Length(); i++ {
			raw := e.bat.Vecs[idx].GetBytesAt(i)
			if len(raw) != objectio.ObjectStatsLen {
				return nil, moerr.NewInternalErrorNoCtx("M0: invalid flushed tombstone stats")
			}
			known[objectio.ObjectStats(raw)] = struct{}{}
		}
	}
	missing := false
	if txn.cn_flushed_s3_tombstone_object_stats_list != nil {
		txn.cn_flushed_s3_tombstone_object_stats_list.Range(func(k, _ any) bool {
			stats, valid := k.(objectio.ObjectStats)
			_, found := known[stats]
			missing = !valid || !found
			return !missing
		})
	}
	if missing {
		return nil, moerr.NewInternalErrorNoCtx("M0: flushed tombstone has no workspace owner")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if current, stable := txn.CatalogVisibility(); !stable || current != revision {
		return nil, moerr.NewTxnNeedRetryNoCtx()
	}
	ok = true
	return v, nil
}

func (v *m0StatementReadView) tombstones(table *txnTable) []objectio.ObjectStats {
	var result []objectio.ObjectStats
	for _, e := range v.writes {
		if e.typ == DELETE && e.fileName != "" && e.accountId == table.accountId && e.databaseId == table.db.databaseId && e.tableId == table.tableId {
			result = append(result, collectObjectStatsFromEntry(e)...)
		}
	}
	return result
}

// acquireM0StatementView borrows the execution's fixed view only for this call.
// Historical snapshot workspaces are independent and must stay empty.
func (tbl *txnTable) acquireM0StatementView(ctx context.Context, offset int) (*m0StatementReadView, error) {
	view := m0read.GetStatementReadView(ctx)
	if view == nil || tbl.db.op.IsSnapOp() {
		return nil, nil
	}
	v, ok := view.(*m0StatementReadView)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("M0: incompatible statement workspace view")
	}
	if err := v.acquire(tbl.getTxn(), offset); err != nil {
		return nil, err
	}
	return v, nil
}

func (v *m0StatementReadView) forTable(tbl *txnTable, fn func(Entry)) {
	for _, e := range v.writes {
		if e.accountId == tbl.accountId && e.databaseId == tbl.db.databaseId && e.tableId == tbl.tableId {
			fn(e)
		}
	}
}
