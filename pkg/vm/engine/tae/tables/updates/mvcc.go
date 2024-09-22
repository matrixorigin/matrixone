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
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
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
	meta           *catalog.ObjectEntry
	appends        *txnbase.MVCCSlice[*AppendNode]
	appendListener func(txnif.AppendNode) error
}

func NewAppendMVCCHandle(meta *catalog.ObjectEntry) *AppendMVCCHandle {
	node := &AppendMVCCHandle{
		RWMutex: &sync.RWMutex{},
		meta:    meta,
		appends: txnbase.NewMVCCSlice(NewEmptyAppendNode, CompareAppendNode),
	}
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
}

// only for internal usage
// given a row, it returns the append node which contains the row
func (n *AppendMVCCHandle) GetAppendNodeByRowLocked(row uint32) (an *AppendNode) {
	_, an = n.appends.SearchNodeByCompareFn(func(node *AppendNode) int {
		if node.maxRow <= row {
			return -1
		}
		if node.startRow > row {
			return 1
		}
		return 0
	})
	return
}

func (n *AppendMVCCHandle) GetMaxRowByTSLocked(ts types.TS) uint32 {
	_, node := n.appends.GetNodeToReadByPrepareTS(ts)
	if node == nil {
		return 0
	}
	return node.maxRow
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
) (
	minRow, maxRow uint32,
	commitTSVec, abortVec containers.Vector,
	aborts *nulls.Bitmap,
) {
	startOffset, node := n.appends.GetNodeToReadByPrepareTS(start)
	if node != nil {
		prepareTS := node.GetPrepare()
		if prepareTS.LT(&start) {
			startOffset++
		}
	}
	endOffset, node := n.appends.GetNodeToReadByPrepareTS(end)
	if node == nil || startOffset > endOffset {
		return
	}
	minRow = n.appends.GetNodeByOffset(startOffset).startRow
	maxRow = node.maxRow

	aborts = &nulls.Bitmap{}
	commitTSVec = containers.MakeVector(types.T_TS.ToType(), mp)
	abortVec = containers.MakeVector(types.T_bool.ToType(), mp)
	n.appends.LoopOffsetRange(
		startOffset,
		endOffset,
		func(node *AppendNode) bool {
			txn := node.GetTxn()
			if txn != nil {
				n.RUnlock()
				txn.GetTxnState(true)
				n.RLock()
			}
			if node.IsAborted() {
				aborts.AddRange(uint64(node.startRow), uint64(node.maxRow))
			}
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS(), false)
				abortVec.Append(node.IsAborted(), false)
			}
			return true
		})
	return
}

func (n *AppendMVCCHandle) FillInCommitTSVecLocked(commitTSVec containers.Vector, maxrow uint32, mp *mpool.MPool) {
	n.appends.ForEach(
		func(node *AppendNode) bool {
			if node.maxRow > maxrow {
				return false
			}
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS(), false)
			}
			return true
		},
		true)
}

func (n *AppendMVCCHandle) GetCommitTSVecInRange(start, end types.TS, mp *mpool.MPool) containers.Vector {
	n.RLock()
	defer n.RUnlock()
	commitTSVec := containers.MakeVector(types.T_TS.ToType(), mp)
	n.appends.ForEach(
		func(node *AppendNode) bool {
			in, before := node.PreparedIn(start, end)
			if in {
				for i := 0; i < int(node.maxRow-node.startRow); i++ {
					commitTSVec.Append(node.GetCommitTS(), false)
				}
			} else {
				return before
			}
			return true
		},
		true)
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
) (maxrow uint32, visible bool, holes *nulls.Bitmap, err error) {
	var holesMax uint32
	anToWait := make([]*AppendNode, 0)
	txnToWait := make([]txnif.TxnReader, 0)
	n.appends.ForEach(func(an *AppendNode) bool {
		if !an.IsSameTxn(txn) {
			needWait, waitTxn := an.NeedWaitCommitting(txn.GetStartTS())
			if needWait {
				anToWait = append(anToWait, an)
				txnToWait = append(txnToWait, waitTxn)
				return true
			}
		}
		if an.IsVisible(txn) {
			visible = true
			maxrow = an.maxRow
		} else {
			if holes == nil {
				holes = nulls.NewWithSize(int(an.maxRow) + 1)
			}
			holes.AddRange(uint64(an.startRow), uint64(an.maxRow))
			if holesMax < an.maxRow {
				holesMax = an.maxRow
			}
		}
		startTS := txn.GetStartTS()
		return !an.Prepare.Greater(&startTS)
	}, true)
	if len(anToWait) != 0 {
		n.RUnlock()
		for _, txn := range txnToWait {
			txn.GetTxnState(true)
		}
		n.RLock()
	}
	for _, an := range anToWait {
		if an.IsVisible(txn) {
			visible = true
			if maxrow < an.maxRow {
				maxrow = an.maxRow
			}
		} else {
			if holes == nil {
				holes = nulls.NewWithSize(int(an.maxRow) + 1)
			}
			holes.AddRange(uint64(an.startRow), uint64(an.maxRow))
			if holesMax < an.maxRow {
				holesMax = an.maxRow
			}
		}
	}
	if !holes.IsEmpty() {
		for i := uint64(maxrow); i < uint64(holesMax); i++ {
			holes.Del(i)
		}
	}
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
			return false
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
	n.appends.InsertNode(an)
}

// AddAppendNodeLocked add a new appendnode to the list.
func (n *AppendMVCCHandle) AddAppendNodeLocked(
	txn txnif.AsyncTxn,
	startRow uint32,
	maxRow uint32,
) (an *AppendNode, created bool) {
	if n.appends.IsEmpty() || !n.appends.GetUpdateNodeLocked().IsSameTxn(txn) {
		// if the appends is empty or the last appendnode is not of the same txn,
		// create a new appendnode and append it to the list.
		an = NewAppendNode(txn, startRow, maxRow, n.meta.IsTombstone, n)
		n.appends.InsertNode(an)
		created = true
	} else {
		// if the last appendnode is of the same txn, update the maxrow of the last appendnode.
		an = n.appends.GetUpdateNodeLocked()
		created = false
		an.SetMaxRow(maxRow)
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
	return n.appends.IsCommitted()
}

// DeleteAppendNodeLocked deletes the appendnode from the append list.
// it is called when txn of the appendnode is aborted.
func (n *AppendMVCCHandle) DeleteAppendNodeLocked(node *AppendNode) {
	n.appends.DeleteNode(node)
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
	anode := n.appends.GetUpdateNodeLocked()
	if anode == nil {
		return false
	}

	// if the latest appendnode is not committed, return false
	if !anode.IsCommitted() {
		return false
	}

	// check if the latest appendnode is committed before ts
	commitTS := anode.GetCommitTS()
	return commitTS.LT(&ts)
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
	an := n.appends.GetUpdateNodeLocked()
	if an == nil {
		return 0
	}
	return an.maxRow
}

func (n *AppendMVCCHandle) GetID() *common.ID {
	return n.meta.AsCommonID()
}
