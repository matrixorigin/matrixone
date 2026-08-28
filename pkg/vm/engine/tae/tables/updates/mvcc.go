// Copyright 2021 Matrix Origin
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

package updates

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/tidwall/btree"
)

func init() {
	txnNodeSize := int(unsafe.Sizeof(txnbase.TxnMVCCNode{}))
	catalog.AppendNodeApproxSize = int(unsafe.Sizeof(AppendNode{})) + txnNodeSize
}

func mockTxn() *txnbase.Txn {
	txn := new(txnbase.Txn)
	txn.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), types.TS{})
	return txn
}

func MockTxnWithStartTS(ts types.TS) *txnbase.Txn {
	txn := mockTxn()
	txn.StartTS = ts
	return txn
}

type AppendMVCCHandle struct {
	*sync.RWMutex
	meta *catalog.ObjectEntry
	// appends is ordered by (prepare TS, physical start row). Active nodes use
	// UncommitTS and therefore stay at the end until their txn is prepared.
	appends *txnbase.MVCCSlice[*AppendNode]
	// rows contains the same nodes in physical row order. Append payload and
	// commit/abort vectors must always be interpreted through this view.
	rows []*AppendNode
	// prepareTree is an immutable, copy-on-write snapshot used by dedup to
	// select (PrepareTS, row range) entries without holding the object lock.
	// Tree items contain values rather than AppendNode pointers because an
	// AppendNode's transaction timestamps are updated in place.
	prepareTree    atomic.Pointer[AppendPrepareSnapshot]
	appendListener func(txnif.AppendNode) error
}

// AppendPrepareSnapshot is an immutable view of the append PrepareTS index.
// Its contents are intentionally private; callers may only use the pointer as
// a generation token when validating a selection after taking the object lock.
type AppendPrepareSnapshot struct {
	tree *btree.BTreeG[appendPrepareEntry]
}

type appendPrepareEntry struct {
	prepare          types.TS
	startRow, maxRow uint32
}

type appendRowRange struct {
	start, end uint32
}

func appendPrepareEntryLess(a, b appendPrepareEntry) bool {
	if !a.prepare.EQ(&b.prepare) {
		return a.prepare.LT(&b.prepare)
	}
	return a.startRow < b.startRow
}

func newAppendPrepareTree() *btree.BTreeG[appendPrepareEntry] {
	return btree.NewBTreeGOptions(appendPrepareEntryLess, btree.Options{
		Degree:  64,
		NoLocks: true,
	})
}

func appendPrepareEntryFromNode(node *AppendNode) appendPrepareEntry {
	return appendPrepareEntry{
		prepare:  node.Prepare,
		startRow: node.startRow,
		maxRow:   node.maxRow,
	}
}

func NewAppendMVCCHandle(meta *catalog.ObjectEntry) *AppendMVCCHandle {
	node := &AppendMVCCHandle{
		RWMutex: &sync.RWMutex{},
		meta:    meta,
		appends: txnbase.NewMVCCSlice(NewEmptyAppendNode, CompareAppendNode),
		rows:    make([]*AppendNode, 0),
	}
	node.prepareTree.Store(&AppendPrepareSnapshot{tree: newAppendPrepareTree()})
	return node
}

// ==========================================================
// *************** All appends related APIs *****************
// ==========================================================

// NOTE: after this call all appends related APIs should not be called
// ReleaseAppends release all append nodes.
// it is only called when the appendable block is persisted and the
// memory node is released
func (n *AppendMVCCHandle) ReleaseAppends() {
	n.Lock()
	defer n.Unlock()
	n.appends = nil
	n.rows = nil
	n.prepareTree.Store(nil)
}

func compareAppendPrepare(a, b *AppendNode) int {
	// Compare the stored value: it changes only while holding this handle's
	// lock. GetPrepare may expose the txn's new prepare TS before that node has
	// been repositioned, temporarily violating the ordering invariant.
	if cmp := a.Prepare.Compare(&b.Prepare); cmp != 0 {
		return cmp
	}
	if a.startRow < b.startRow {
		return -1
	}
	if a.startRow > b.startRow {
		return 1
	}
	return 0
}

func (n *AppendMVCCHandle) insertPrepareLocked(node *AppendNode) {
	off, _ := slices.BinarySearchFunc(n.appends.MVCC, node, compareAppendPrepare)
	n.appends.MVCC = slices.Insert(n.appends.MVCC, off, node)
	n.updatePrepareTreeLocked(nil, node)
}

func (n *AppendMVCCHandle) removePrepareLocked(node *AppendNode) {
	for i, candidate := range n.appends.MVCC {
		if candidate == node {
			n.appends.MVCC = slices.Delete(n.appends.MVCC, i, i+1)
			return
		}
	}
}

func (n *AppendMVCCHandle) insertRowLocked(node *AppendNode) {
	off, _ := slices.BinarySearchFunc(n.rows, node, func(a, b *AppendNode) int {
		if a.startRow < b.startRow {
			return -1
		}
		if a.startRow > b.startRow {
			return 1
		}
		return 0
	})
	n.rows = slices.Insert(n.rows, off, node)
}

func (n *AppendMVCCHandle) reorderPrepareLocked(node *AppendNode, oldPrepare types.TS) {
	n.removePrepareLocked(node)
	off, _ := slices.BinarySearchFunc(n.appends.MVCC, node, compareAppendPrepare)
	n.appends.MVCC = slices.Insert(n.appends.MVCC, off, node)
	old := appendPrepareEntry{prepare: oldPrepare, startRow: node.startRow}
	n.updatePrepareTreeLocked(&old, node)
}

func (n *AppendMVCCHandle) updatePrepareTreeLocked(oldEntry *appendPrepareEntry, node *AppendNode) {
	oldSnapshot := n.prepareTree.Load()
	if oldSnapshot == nil {
		panic("append prepare tree is released")
	}
	newTree := oldSnapshot.tree.Copy()
	if oldEntry != nil {
		if _, deleted := newTree.Delete(*oldEntry); !deleted {
			panic("append prepare entry not found")
		}
	}
	if node != nil {
		newTree.Set(appendPrepareEntryFromNode(node))
	}
	newSnapshot := &AppendPrepareSnapshot{tree: newTree}
	if !n.prepareTree.CompareAndSwap(oldSnapshot, newSnapshot) {
		panic("concurrent append prepare tree mutation")
	}
}

// only for internal usage
// given a row, it returns the append node which contains the row
func (n *AppendMVCCHandle) GetAppendNodeByRowLocked(row uint32) (an *AppendNode) {
	off, _ := slices.BinarySearchFunc(n.rows, row, func(node *AppendNode, row uint32) int {
		if node.maxRow <= row {
			return -1
		}
		if node.startRow > row {
			return 1
		}
		return 0
	})
	if off < len(n.rows) && n.rows[off].startRow <= row && row < n.rows[off].maxRow {
		return n.rows[off]
	}
	return nil
}

func (n *AppendMVCCHandle) GetRowSelectionByTSLocked(ts types.TS) index.RowSelection {
	selection := n.GetRowSelectionInRangeLocked(types.TS{}, ts)
	selection.MakePrefix()
	return selection
}

func (n *AppendMVCCHandle) GetRowSelectionInRangeLocked(start, end types.TS) (selection index.RowSelection) {
	for _, node := range n.rows {
		if in, _ := node.PreparedIn(start, end); in {
			selection.AddRange(node.startRow, node.maxRow)
		}
	}
	return
}

func (n *AppendMVCCHandle) GetRowSelectionAfter(start, end types.TS) (selection index.RowSelection) {
	selection, _ = n.GetRowSelectionAfterWithSnapshot(start, end)
	return
}

// GetRowSelectionAfterWithSnapshot returns a selection and the immutable
// snapshot from which it was built. Callers that build the selection before
// taking the object lock must validate the snapshot after acquiring the lock.
func (n *AppendMVCCHandle) GetRowSelectionAfterWithSnapshot(
	start, end types.TS,
) (selection index.RowSelection, snapshot *AppendPrepareSnapshot) {
	if end.LE(&start) {
		return
	}
	snapshot = n.prepareTree.Load()
	if snapshot == nil {
		return
	}
	selected := make([]appendRowRange, 0)
	snapshot.tree.Ascend(appendPrepareEntry{prepare: start}, func(entry appendPrepareEntry) bool {
		if !entry.prepare.GT(&start) {
			return true
		}
		if entry.prepare.GT(&end) {
			return false
		}
		selected = append(selected, appendRowRange{start: entry.startRow, end: entry.maxRow})
		return true
	})
	if len(selected) == 0 {
		return
	}
	selection.MinRow = selected[0].start
	selection.MaxRow = selected[0].end
	for _, rows := range selected[1:] {
		selection.MinRow = min(selection.MinRow, rows.start)
		selection.MaxRow = max(selection.MaxRow, rows.end)
	}

	// PrepareTS order and physical row order are independent. Building the
	// complement bitmap is linear in the selected nodes plus the bounded row
	// domain of one appendable object, and avoids sorting every dedup request.
	holes := &nulls.Bitmap{}
	holes.AddRange(uint64(selection.MinRow), uint64(selection.MaxRow))
	for _, rows := range selected {
		holes.GetBitmap().RemoveRange(uint64(rows.start), uint64(rows.end))
	}
	if holes.Any() {
		selection.Holes = holes
	}
	return
}

func (n *AppendMVCCHandle) IsPrepareSnapshotCurrent(snapshot *AppendPrepareSnapshot) bool {
	return snapshot == n.prepareTree.Load()
}

// it collects all append nodes in the range [start, end]
// minRow: is the min row
// maxRow: is the max row
// commitTSVec: is the commit ts vector
// abortVec: is the abort vector
// aborts: is the aborted bitmap
// If checkCommit, it ignore all uncommitted nodes
func (n *AppendMVCCHandle) CollectAppendLocked(
	start, end types.TS, mp *mpool.MPool,
) (selection index.RowSelection, commitTSVec, abortVec containers.Vector) {
	txns := make([]txnif.TxnReader, 0)
	for _, node := range n.appends.MVCC {
		txn := node.GetTxn()
		if txn == nil {
			continue
		}
		// A txn gets its prepare timestamp before AppendNode.PrepareCommit
		// copies it into the node. Use the txn timestamp here so a range scan
		// cannot miss that preparing window without waiting for it to close.
		prepare := txn.GetPrepareTS()
		if prepare.GE(&start) && prepare.LE(&end) {
			txns = append(txns, txn)
		}
	}
	if len(txns) != 0 {
		n.RUnlock()
		for _, txn := range txns {
			txn.GetTxnState(true)
		}
		n.RLock()
	}
	selection = n.GetRowSelectionInRangeLocked(start, end)
	if selection.IsEmpty() {
		return
	}
	commitTSVec = containers.MakeVector(types.T_TS.ToType(), mp)
	abortVec = containers.MakeVector(types.T_bool.ToType(), mp)
	for _, node := range n.rows {
		if in, _ := node.PreparedIn(start, end); in {
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS(), false)
				abortVec.Append(node.IsAborted(), false)
			}
		}
	}
	return
}

func (n *AppendMVCCHandle) FillInCommitTSVecLocked(commitTSVec containers.Vector, maxrow uint32, mp *mpool.MPool) {
	var cursor uint32
	for _, node := range n.rows {
		if node.startRow >= maxrow {
			break
		}
		// Freeze may have installed payload rows for a transaction that later
		// fails queue dedup before an AppendNode owns that physical range. Keep
		// the commit-TS vector aligned with physical row offsets; scan selection
		// holes delete these placeholder rows before they become visible.
		for cursor < node.startRow {
			commitTSVec.Append(txnif.UncommitTS, false)
			cursor++
		}
		end := min(node.maxRow, maxrow)
		for cursor < end {
			commitTSVec.Append(node.GetCommitTS(), false)
			cursor++
		}
	}
	for cursor < maxrow {
		commitTSVec.Append(txnif.UncommitTS, false)
		cursor++
	}
}

func (n *AppendMVCCHandle) GetCommitTSVecInRange(start, end types.TS, mp *mpool.MPool) containers.Vector {
	n.RLock()
	defer n.RUnlock()
	commitTSVec := containers.MakeVector(types.T_TS.ToType(), mp)
	for _, node := range n.rows {
		if in, _ := node.PreparedIn(start, end); in {
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS(), false)
			}
		}
	}
	return commitTSVec
}

// it is used to get the visible max row for a txn
// maxrow: is the max row that the txn can see
// visible: is true if the txn can see any row
// holes: is the bitmap of the holes that the txn cannot see
// holes exists only if any append node was rollbacked
func (n *AppendMVCCHandle) GetVisibleRowLocked(
	ctx context.Context,
	txn txnif.TxnReader,
) (selection index.RowSelection, visible bool, err error) {
	txnToWait := make([]txnif.TxnReader, 0)
	for _, an := range n.appends.MVCC {
		if !an.IsSameTxn(txn) {
			if needWait, waitTxn := an.NeedWaitCommitting(txn.GetStartTS()); needWait {
				txnToWait = append(txnToWait, waitTxn)
			}
		}
	}
	if len(txnToWait) != 0 {
		n.RUnlock()
		for _, waitTxn := range txnToWait {
			waitTxn.GetTxnState(true)
		}
		n.RLock()
	}
	for _, an := range n.rows {
		if an.IsVisible(txn) {
			visible = true
			selection.AddRange(an.startRow, an.maxRow)
		}
	}
	selection.MakePrefix()
	return
}

// it collects all append nodes that are prepared before the given ts
// foreachFn is called for each append node that is prepared before the given ts
func (n *AppendMVCCHandle) CollectUncommittedANodesPreparedBeforeLocked(
	ts types.TS,
	foreachFn func(*AppendNode),
) (anyWaitable bool) {
	if n.appends.IsEmpty() {
		return
	}
	n.appends.ForEach(func(an *AppendNode) bool {
		needWait, txn := an.NeedWaitCommitting(ts)
		if txn == nil {
			return true
		}
		if needWait {
			foreachFn(an)
			anyWaitable = true
		}
		return true
	}, false)
	return
}

func (n *AppendMVCCHandle) OnReplayAppendNode(an *AppendNode) {
	an.mvcc = n
	n.insertPrepareLocked(an)
	n.insertRowLocked(an)
}

// AddAppendNodeLocked add a new appendnode to the list.
func (n *AppendMVCCHandle) AddAppendNodeLocked(
	txn txnif.AsyncTxn,
	startRow uint32,
	maxRow uint32,
) (an *AppendNode, created bool) {
	var last *AppendNode
	if len(n.rows) != 0 {
		last = n.rows[len(n.rows)-1]
	}
	if last == nil || !last.IsSameTxn(txn) || last.maxRow != startRow {
		// if the appends is empty or the last appendnode is not of the same txn,
		// create a new appendnode and append it to the list.
		an = NewAppendNode(txn, startRow, maxRow, n.meta.IsTombstone, n)
		n.insertPrepareLocked(an)
		n.insertRowLocked(an)
		created = true
	} else {
		// if the last appendnode is of the same txn, update the maxrow of the last appendnode.
		an = last
		created = false
		an.SetMaxRow(maxRow)
		n.updatePrepareTreeLocked(nil, an)
	}
	return
}

// Reschedule until all appendnode is committed.
// Pending appendnode is not visible for compaction txn.
func (n *AppendMVCCHandle) PrepareCompactLocked() bool {
	return n.allAppendsCommittedLocked()
}
func (n *AppendMVCCHandle) PrepareCompact() bool {
	n.RLock()
	defer n.RUnlock()
	return n.allAppendsCommittedLocked()
}

func (n *AppendMVCCHandle) GetLatestAppendPrepareTSLocked() types.TS {
	if n.appends == nil || n.appends.IsEmpty() {
		return types.TS{}
	}
	return n.appends.GetUpdateNodeLocked().Prepare
}
func (n *AppendMVCCHandle) GetMeta() *catalog.ObjectEntry {
	return n.meta
}

// check if all appendnodes are committed.
func (n *AppendMVCCHandle) allAppendsCommittedLocked() bool {
	if n.appends == nil {
		meta := n.GetMeta()
		logutil.Warnf("[MetadataCheck] appends mvcc is nil, obj %v, has dropped %v, deleted at %v",
			meta.ID().String(),
			meta.HasDropCommitted(),
			meta.GetDeleteAt().ToString())
		return false
	}
	if n.appends.IsEmpty() {
		return true
	}
	for _, node := range n.appends.MVCC {
		if !node.IsCommitted() {
			return false
		}
	}
	return true
}

// DeleteAppendNodeLocked deletes the appendnode from the append list.
// it is called when txn of the appendnode is aborted.
func (n *AppendMVCCHandle) DeleteAppendNodeLocked(node *AppendNode) {
	n.removePrepareLocked(node)
	for i, candidate := range n.rows {
		if candidate == node {
			n.rows = slices.Delete(n.rows, i, i+1)
			return
		}
	}
}

func (n *AppendMVCCHandle) SetAppendListener(l func(txnif.AppendNode) error) {
	n.appendListener = l
}

func (n *AppendMVCCHandle) GetAppendListener() func(txnif.AppendNode) error {
	return n.appendListener
}

// AllAppendsCommittedBefore returns true if all appendnode is committed before ts.
func (n *AppendMVCCHandle) AllAppendsCommittedBeforeLocked(ts types.TS) bool {
	// get the latest appendnode
	for _, anode := range n.appends.MVCC {
		commitTS := anode.GetCommitTS()
		if !anode.IsCommitted() || !commitTS.LT(&ts) {
			return false
		}
	}
	return len(n.appends.MVCC) != 0
}

func (n *AppendMVCCHandle) StringLocked() string {
	return n.appends.StringLocked()
}

func (n *AppendMVCCHandle) EstimateMemSizeLocked() int {
	asize := 0
	if n.appends != nil {
		asize += len(n.appends.MVCC) * catalog.AppendNodeApproxSize
	}
	return asize
}

// GetTotalRow is only for replay
func (n *AppendMVCCHandle) GetTotalRow() uint32 {
	if len(n.rows) == 0 {
		return 0
	}
	return n.rows[len(n.rows)-1].maxRow
}

func (n *AppendMVCCHandle) GetID() *common.ID {
	return n.meta.AsCommonID()
}
