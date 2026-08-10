// Copyright 2026 Matrix Origin
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
	"slices"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// workspaceTableIDKey identifies one table inside a transaction workspace.
// Database id is part of the key because table ids from transactional DDL may
// not yet have the same global uniqueness guarantees as committed table ids.
type workspaceTableIDKey struct {
	databaseID uint64
	tableID    uint64
}

// txnWorkspace is the sole owner of the transaction's ordered write log and
// its access paths. entries preserves commit and statement order. The derived
// indexes never define visibility: callers always provide the statement's
// visible prefix and every indexed lookup is clamped to that prefix.
//
// Structural transformations (merge, dump, reorder, rollback) invalidate the
// indexes and rebuild them on the next indexed access. Appends extend a valid
// index incrementally, which keeps the ordinary UPDATE path independent of the
// total number of writes already present in the transaction.
type txnWorkspace struct {
	entries []Entry
	index   *txnWorkspaceIndex
}

type txnWorkspaceIndex struct {
	byTable          map[workspaceTableIDKey][]int
	rawInsertByBlock map[workspaceTableIDKey]map[types.Blockid][]int
	indexedCount     int
}

// txnWorkspaceView is an immutable visibility boundary over the workspace.
// It deliberately keeps the physical write-log offsets inside txnWorkspace:
// read paths select a view once, then ask that view for table access paths.
// The entries themselves remain owned by txnWorkspace. A view is valid only
// while the transaction workspace lock remains held.
type txnWorkspaceView struct {
	workspace *txnWorkspace
	start     int
	end       int
}

func (ws *txnWorkspace) len() int {
	return len(ws.entries)
}

func (ws *txnWorkspace) append(entry Entry) {
	ws.entries = append(ws.entries, entry)
	if ws.index != nil && ws.index.indexedCount == len(ws.entries)-1 {
		ws.indexEntry(len(ws.entries) - 1)
		ws.index.indexedCount++
		return
	}
	ws.invalidateIndex()
}

func (ws *txnWorkspace) replace(entries []Entry) {
	ws.entries = entries
	ws.invalidateIndex()
}

func (ws *txnWorkspace) truncate(end int) {
	if end < 0 || end > len(ws.entries) {
		panic("invalid transaction workspace truncation")
	}
	ws.entries = ws.entries[:end]
	ws.invalidateIndex()
}

func (ws *txnWorkspace) invalidateIndex() {
	ws.index = nil
}

func (ws *txnWorkspace) compact(keep func(*Entry) bool) {
	n := 0
	for idx := range ws.entries {
		if !keep(&ws.entries[idx]) {
			continue
		}
		if n != idx {
			ws.entries[n] = ws.entries[idx]
		}
		n++
	}
	ws.replace(ws.entries[:n])
}

func (ws *txnWorkspace) stableSortFrom(start int, cmp func(a, b Entry) int) {
	start = max(start, 0)
	start = min(start, len(ws.entries))
	slices.SortStableFunc(ws.entries[start:], cmp)
	ws.invalidateIndex()
}

func (ws *txnWorkspace) ensureIndex() {
	if ws.index != nil && ws.index.indexedCount == len(ws.entries) {
		return
	}
	ws.index = &txnWorkspaceIndex{
		byTable:          make(map[workspaceTableIDKey][]int),
		rawInsertByBlock: make(map[workspaceTableIDKey]map[types.Blockid][]int),
	}
	for idx := range ws.entries {
		ws.indexEntry(idx)
	}
	ws.index.indexedCount = len(ws.entries)
}

func (ws *txnWorkspace) indexEntry(idx int) {
	entry := &ws.entries[idx]
	key := workspaceTableIDKey{databaseID: entry.databaseId, tableID: entry.tableId}
	ws.index.byTable[key] = append(ws.index.byTable[key], idx)

	if !isIndexedWorkspaceInsert(entry) {
		return
	}
	rows := vector.MustFixedColNoTypeCheck[types.Rowid](entry.bat.Vecs[0])
	byBlock := ws.index.rawInsertByBlock[key]
	if byBlock == nil {
		byBlock = make(map[types.Blockid][]int)
		ws.index.rawInsertByBlock[key] = byBlock
	}
	// deleteTableWrites treats a raw CN insert batch as belonging to the block
	// of its first RowID. Preserve that existing selection contract exactly and
	// keep the access path proportional to workspace entries, not rows.
	blockID := rows[0].CloneBlockID()
	byBlock[blockID] = append(byBlock[blockID], idx)
}

func isIndexedWorkspaceInsert(entry *Entry) bool {
	if entry.typ != INSERT || entry.bat == nil || entry.bat.RowCount() == 0 || len(entry.bat.Vecs) == 0 {
		return false
	}
	if len(entry.bat.Attrs) == 0 || entry.bat.Attrs[0] != catalog.Row_ID {
		return false
	}
	return entry.bat.Vecs[0] != nil && entry.bat.Vecs[0].GetType().Oid == types.T_Rowid
}

func (ws *txnWorkspace) visibleEnd(end int) int {
	if end < 0 {
		return 0
	}
	if end > len(ws.entries) {
		return len(ws.entries)
	}
	return end
}

func (ws *txnWorkspace) view(start, end int) txnWorkspaceView {
	end = ws.visibleEnd(end)
	if start < 0 {
		start = 0
	}
	if start > end {
		start = end
	}
	return txnWorkspaceView{workspace: ws, start: start, end: end}
}

func (ws *txnWorkspace) visiblePrefix(end int) txnWorkspaceView {
	return ws.view(0, end)
}

func (ws *txnWorkspace) currentView() txnWorkspaceView {
	return ws.view(0, ws.len())
}

// tableEntryIndexes returns the ordered entry indexes for table in [start,end).
// The returned slice aliases the workspace index and must not be modified.
func (view txnWorkspaceView) tableEntryIndexes(databaseID, tableID uint64) []int {
	if view.workspace == nil || view.start >= view.end {
		return nil
	}
	view.workspace.ensureIndex()
	indexes := view.workspace.index.byTable[workspaceTableIDKey{databaseID: databaseID, tableID: tableID}]
	lo := sort.SearchInts(indexes, view.start)
	hi := sort.SearchInts(indexes, view.end)
	if lo == hi {
		return nil
	}
	return indexes[lo:hi]
}

// rawInsertEntryIndexes returns the ordered raw-insert entries for table that
// may contain rows from blocks in blockIDs and are visible in [start,end).
// A single-block result aliases the workspace index; a multi-block result is
// owned by the caller. Callers must not modify either form.
func (view txnWorkspaceView) rawInsertEntryIndexes(
	databaseID, tableID uint64,
	blockIDs map[types.Blockid]bool,
) []int {
	if view.workspace == nil || len(blockIDs) == 0 || view.start >= view.end {
		return nil
	}
	view.workspace.ensureIndex()
	byBlock := view.workspace.index.rawInsertByBlock[workspaceTableIDKey{databaseID: databaseID, tableID: tableID}]
	if len(byBlock) == 0 {
		return nil
	}

	var (
		first   []int
		indexes []int
		matches int
	)
	for blockID, selected := range blockIDs {
		if !selected {
			continue
		}
		entries := byBlock[blockID]
		lo := sort.SearchInts(entries, view.start)
		hi := sort.SearchInts(entries, view.end)
		if lo == hi {
			continue
		}
		matches++
		if matches == 1 {
			first = entries[lo:hi]
			continue
		}
		if indexes == nil {
			indexes = append(indexes, first...)
		}
		indexes = append(indexes, entries[lo:hi]...)
	}
	if matches == 0 {
		return nil
	}
	if matches == 1 {
		return first
	}
	sort.Ints(indexes)
	return indexes
}
