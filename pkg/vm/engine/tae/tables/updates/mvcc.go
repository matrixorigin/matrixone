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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCHandle struct {
	*sync.RWMutex
	deletes         *DeleteChain
	meta            *catalog.BlockEntry
	appends         *txnbase.MVCCSlice
	changes         atomic.Uint32
	deletesListener func(uint64, common.RowGen, types.TS) error
	appendListener  func(txnif.AppendNode) error
}

func NewMVCCHandle(meta *catalog.BlockEntry) *MVCCHandle {
	node := &MVCCHandle{
		RWMutex: new(sync.RWMutex),
		meta:    meta,
		appends: txnbase.NewMVCCSlice(NewEmptyAppendNode, CompareAppendNode),
	}
	node.deletes = NewDeleteChain(nil, node)
	if meta == nil {
		return node
	}
	return node
}

func (n *MVCCHandle) SetAppendListener(l func(txnif.AppendNode) error) {
	n.appendListener = l
}

func (n *MVCCHandle) GetAppendListener() func(txnif.AppendNode) error {
	return n.appendListener
}

func (n *MVCCHandle) SetDeletesListener(l func(uint64, common.RowGen, types.TS) error) {
	n.deletesListener = l
}

func (n *MVCCHandle) GetDeletesListener() func(uint64, common.RowGen, types.TS) error {
	return n.deletesListener
}

func (n *MVCCHandle) HasActiveAppendNode() bool {
	n.RLock()
	defer n.RUnlock()
	return !n.appends.IsCommitted()
}

func (n *MVCCHandle) IncChangeNodeCnt() {
	n.changes.Add(1)
}

func (n *MVCCHandle) GetChangeNodeCnt() uint32 {
	return n.changes.Load()
}

func (n *MVCCHandle) GetDeleteCnt() uint32 {
	return n.deletes.GetDeleteCnt()
}

func (n *MVCCHandle) GetID() *common.ID             { return n.meta.AsCommonID() }
func (n *MVCCHandle) GetEntry() *catalog.BlockEntry { return n.meta }

func (n *MVCCHandle) StringLocked() string {
	s := ""
	if n.deletes.DepthLocked() > 0 {
		s = fmt.Sprintf("%s%s", s, n.deletes.StringLocked())
	}
	s = fmt.Sprintf("%s\n%s", s, n.appends.StringLocked())
	return s
}

func (n *MVCCHandle) CheckNotDeleted(start, end uint32, ts types.TS) error {
	return n.deletes.PrepareRangeDelete(start, end, ts)
}

func (n *MVCCHandle) CreateDeleteNode(txn txnif.AsyncTxn, deleteType handle.DeleteType) txnif.DeleteNode {
	return n.deletes.AddNodeLocked(txn, deleteType)
}

func (n *MVCCHandle) OnReplayDeleteNode(deleteNode txnif.DeleteNode) {
	n.deletes.OnReplayNode(deleteNode.(*DeleteNode))
}

func (n *MVCCHandle) GetDeleteChain() *DeleteChain {
	return n.deletes
}
func (n *MVCCHandle) OnReplayAppendNode(an *AppendNode) {
	an.mvcc = n
	n.appends.InsertNode(an)
}
func (n *MVCCHandle) AddAppendNodeLocked(
	txn txnif.AsyncTxn,
	startRow uint32,
	maxRow uint32) (an *AppendNode, created bool) {
	var ts types.TS
	if txn != nil {
		ts = txn.GetStartTS()
	}
	if n.appends.IsEmpty() || n.appends.SearchNode(NewCommittedAppendNode(ts, 0, 0, nil)) == nil {
		an = NewAppendNode(txn, startRow, maxRow, n)
		n.appends.InsertNode(an)
		created = true
	} else {
		an = n.appends.GetUpdateNodeLocked().(*AppendNode)
		created = false
		an.SetMaxRow(maxRow)
	}
	return
}
func (n *MVCCHandle) AppendCommitted() bool {
	return n.appends.IsCommitted()
}
func (n *MVCCHandle) DeleteAppendNodeLocked(node *AppendNode) {
	n.appends.DeleteNode(node)
}

func (n *MVCCHandle) IsVisibleLocked(row uint32, ts types.TS) (bool, error) {
	an := n.GetAppendNodeByRow(row)
	return an.IsVisible(ts), nil
}

func (n *MVCCHandle) IsDeletedLocked(row uint32, ts types.TS, rwlocker *sync.RWMutex) (bool, error) {
	return n.deletes.IsDeleted(row, ts, rwlocker)
}

//	  1         2        3       4      5       6
//	[----] [---------] [----][------][-----] [-----]
//
// -----------+------------------+---------------------->
//
//	start               end
func (n *MVCCHandle) CollectUncommittedANodesPreparedBefore(
	ts types.TS,
	fn func(*AppendNode)) (anyWaitable bool) {
	if n.appends.IsEmpty() {
		return
	}
	n.appends.ForEach(func(un txnif.MVCCNode) bool {
		an := un.(*AppendNode)
		needWait, txn := an.NeedWaitCommitting(ts)
		if txn == nil {
			return false
		}
		if needWait {
			fn(an)
			anyWaitable = true
		}
		return true
	}, false)
	return
}

func (n *MVCCHandle) CollectAppendLogIndexesLocked(startTs, endTs types.TS) (indexes []*wal.Index, err error) {
	if n.appends.IsEmpty() {
		return
	}
	indexes = make([]*wal.Index, 0)
	n.appends.ForEach(func(un txnif.MVCCNode) bool {
		an := un.(*AppendNode)
		needWait, txn := an.NeedWaitCommitting(endTs.Next())
		if needWait {
			n.RUnlock()
			txn.GetTxnState(true)
			n.RLock()
		}
		if an.Prepare.Less(startTs) {
			return true
		}
		if an.Prepare.Greater(endTs) {
			return false
		}
		indexes = append(indexes, an.GetLogIndex())
		return true
	}, true)
	return
}

func (n *MVCCHandle) GetVisibleRowLocked(ts types.TS) (maxrow uint32, visible bool, holes *roaring.Bitmap, err error) {
	anToWait := make([]*AppendNode, 0)
	txnToWait := make([]txnif.TxnReader, 0)
	n.appends.ForEach(func(un txnif.MVCCNode) bool {
		an := un.(*AppendNode)
		needWait, txn := an.NeedWaitCommitting(ts)
		if needWait {
			anToWait = append(anToWait, an)
			txnToWait = append(txnToWait, txn)
			return true
		}
		if an.IsVisible(ts) {
			visible = true
			maxrow = an.maxRow
		} else {
			if holes == nil {
				holes = roaring.NewBitmap()
			}
			holes.AddRange(uint64(an.startRow), uint64(an.maxRow))
		}
		return !an.Prepare.Greater(ts)
	}, true)
	if len(anToWait) != 0 {
		n.RUnlock()
		for _, txn := range txnToWait {
			txn.GetTxnState(true)
		}
		n.RLock()
	}
	for _, an := range anToWait {
		if an.IsVisible(ts) {
			visible = true
			if maxrow < an.maxRow {
				maxrow = an.maxRow
			}
		} else {
			if holes == nil {
				holes = roaring.NewBitmap()
			}
			holes.AddRange(uint64(an.startRow), uint64(an.maxRow))
		}
	}
	if holes != nil {
		holes.RemoveRange(uint64(maxrow), uint64(holes.Maximum())+1)
	}
	return
}

// GetTotalRow is only for replay
func (n *MVCCHandle) GetTotalRow() uint32 {
	van := n.appends.GetUpdateNodeLocked()
	if van == nil {
		return 0
	}
	an := van.(*AppendNode)
	return an.maxRow - n.deletes.cnt.Load()
}

func (n *MVCCHandle) CollectAppendLocked(
	start, end types.TS) (
	minRow, maxRow uint32,
	commitTSVec, abortVec containers.Vector,
	abortedBitmap *roaring.Bitmap) {
	startOffset, node := n.appends.GetNodeToReadByPrepareTS(start)
	if node != nil && node.GetPrepare().Less(start) {
		startOffset++
	}
	endOffset, node := n.appends.GetNodeToReadByPrepareTS(end)
	if node == nil || startOffset > endOffset {
		return
	}
	minRow = n.appends.GetNodeByOffset(startOffset).(*AppendNode).startRow
	maxRow = node.(*AppendNode).maxRow

	abortedBitmap = roaring.NewBitmap()
	commitTSVec = containers.MakeVector(types.T_TS.ToType(), false)
	abortVec = containers.MakeVector(types.T_bool.ToType(), false)
	n.appends.LoopOffsetRange(
		startOffset,
		endOffset,
		func(m txnif.MVCCNode) bool {
			node := m.(*AppendNode)
			txn := node.GetTxn()
			if txn != nil {
				n.RUnlock()
				txn.GetTxnState(true)
				n.RLock()
			}
			if node.IsAborted() {
				abortedBitmap.AddRange(uint64(node.startRow), uint64(node.maxRow))
			}
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS())
				abortVec.Append(node.IsAborted())
			}
			return true
		})
	return
}

func (n *MVCCHandle) CollectDelete(start, end types.TS) (rowIDVec, commitTSVec, abortVec containers.Vector, abortedBitmap *roaring.Bitmap) {
	n.RLock()
	defer n.RUnlock()
	if n.deletes.IsEmpty() {
		return
	}
	if !n.ExistDeleteInRange(start, end) {
		return
	}

	rowIDVec = containers.MakeVector(types.T_Rowid.ToType(), false)
	commitTSVec = containers.MakeVector(types.T_TS.ToType(), false)
	abortVec = containers.MakeVector(types.T_bool.ToType(), false)
	abortedBitmap = roaring.NewBitmap()
	prefix := n.meta.MakeKey()

	n.deletes.LoopChain(
		func(m txnif.MVCCNode) bool {
			node := m.(*DeleteNode)
			needWait, txn := node.NeedWaitCommitting(end.Next())
			if needWait {
				n.RUnlock()
				txn.GetTxnState(true)
				n.RLock()
			}
			in, before := node.PreparedIn(start, end)
			if in {
				it := node.mask.Iterator()
				if node.IsAborted() {
					abortedBitmap.AddMany(node.mask.ToArray())
				}
				for it.HasNext() {
					row := it.Next()
					rowIDVec.Append(model.EncodePhyAddrKeyWithPrefix(prefix, row))
					commitTSVec.Append(node.GetEnd())
					abortVec.Append(node.IsAborted())
				}
			}
			return !before
		})
	return
}

func (n *MVCCHandle) ExistDeleteInRange(start, end types.TS) (exist bool) {
	n.deletes.LoopChain(
		func(m txnif.MVCCNode) bool {
			node := m.(*DeleteNode)
			needWait, txn := node.NeedWaitCommitting(end.Next())
			if needWait {
				n.RUnlock()
				txn.GetTxnState(true)
				n.RLock()
			}
			in, before := node.PreparedIn(start, end)
			if in {
				exist = true
				return false
			}
			return !before
		})
	return
}

func (n *MVCCHandle) GetAppendNodeByRow(row uint32) (an *AppendNode) {
	_, van := n.appends.SearchNodeByCompareFn(func(a txnif.MVCCNode) int {
		node := a.(*AppendNode)
		if node.maxRow <= row {
			return -1
		}
		if node.startRow > row {
			return 1
		}
		return 0
	})
	if van == nil {
		return nil
	}
	return van.(*AppendNode)
}
func (n *MVCCHandle) GetDeleteNodeByRow(row uint32) (an *DeleteNode) {
	return n.deletes.GetDeleteNodeByRow(row)
}
