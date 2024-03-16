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
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

var (
	AppendNodeApproxSize int
	DeleteNodeApproxSize int

	DeleteChainApproxSize int
	MVCCHandleApproxSize  int
)

func init() {
	txnNodeSize := int(unsafe.Sizeof(txnbase.TxnMVCCNode{}))
	AppendNodeApproxSize = int(unsafe.Sizeof(AppendNode{})) + txnNodeSize
	DeleteNodeApproxSize = int(unsafe.Sizeof(DeleteNode{})) + txnNodeSize

	DeleteChainApproxSize = int(unsafe.Sizeof(DeleteChain{}))
	MVCCHandleApproxSize = int(unsafe.Sizeof(MVCCHandle{}))
}

type MVCCHandle struct {
	*sync.RWMutex
	deletes         atomic.Pointer[DeleteChain]
	meta            *catalog.BlockEntry
	appends         *txnbase.MVCCSlice[*AppendNode]
	changes         atomic.Uint32
	deletesListener func(uint64, types.TS) error
	appendListener  func(txnif.AppendNode) error
	persistedTS     types.TS
}

func NewMVCCHandle(meta *catalog.BlockEntry) *MVCCHandle {
	node := &MVCCHandle{
		RWMutex: new(sync.RWMutex),
		meta:    meta,
		appends: txnbase.NewMVCCSlice(NewEmptyAppendNode, CompareAppendNode),
	}
	node.deletes.Store(NewDeleteChain(nil, node))
	if meta == nil {
		return node
	}
	return node
}

// ==========================================================
// *************** All common related APIs *****************
// ==========================================================

func (n *MVCCHandle) GetID() *common.ID             { return n.meta.AsCommonID() }
func (n *MVCCHandle) GetEntry() *catalog.BlockEntry { return n.meta }

func (n *MVCCHandle) StringLocked() string {
	s := ""
	if n.deletes.Load().DepthLocked() > 0 {
		s = fmt.Sprintf("%s%s", s, n.deletes.Load().StringLocked())
	}
	s = fmt.Sprintf("%s\n%s", s, n.appends.StringLocked())
	return s
}

func (n *MVCCHandle) EstimateMemSizeLocked() (asize int, dsize int) {
	dsize += n.deletes.Load().EstimateMemSizeLocked()
	if n.appends != nil {
		asize += len(n.appends.MVCC) * AppendNodeApproxSize
	}
	return asize, dsize + MVCCHandleApproxSize
}

// ==========================================================
// *************** All deletes related APIs *****************
// ==========================================================

func (n *MVCCHandle) UpgradeDeleteChainByTS(flushed types.TS) {
	n.Lock()
	if n.persistedTS.Equal(&flushed) {
		n.Unlock()
		return
	}
	newDeletes := n.deletes.Load().shrinkDeleteChainByTS(flushed)

	n.deletes.Store(newDeletes)
	n.persistedTS = flushed
	n.Unlock()
}

func (n *MVCCHandle) SetDeletesListener(l func(uint64, types.TS) error) {
	n.deletesListener = l
}

func (n *MVCCHandle) GetDeletesListener() func(uint64, types.TS) error {
	return n.deletesListener
}

func (n *MVCCHandle) IncChangeIntentionCnt() {
	n.changes.Add(1)
}

func (n *MVCCHandle) DecChangeIntentionCnt() {
	n.changes.Add(^uint32(0))
}

// GetChangeIntentionCnt returns the number of operation of delete, which is updated before commiting.
// Note: Now it is ** only ** used in checkpointe runner to check whether this block has any chance to be flushed
func (n *MVCCHandle) GetChangeIntentionCnt() uint32 {
	return n.changes.Load()
}

// GetDeleteCnt returns committed deleted rows
func (n *MVCCHandle) GetDeleteCnt() uint32 {
	return n.deletes.Load().GetDeleteCnt()
}

// it checks whether there is any delete in the range [start, end)
// ts is not used for now
func (n *MVCCHandle) CheckNotDeleted(start, end uint32, ts types.TS) error {
	return n.deletes.Load().PrepareRangeDelete(start, end, ts)
}

func (n *MVCCHandle) CreateDeleteNode(txn txnif.AsyncTxn, deleteType handle.DeleteType) txnif.DeleteNode {
	return n.deletes.Load().AddNodeLocked(txn, deleteType)
}

func (n *MVCCHandle) CreatePersistedDeleteNode(txn txnif.AsyncTxn, deltaloc objectio.Location) txnif.DeleteNode {
	return n.deletes.Load().AddPersistedNodeLocked(txn, deltaloc)
}

func (n *MVCCHandle) OnReplayDeleteNode(deleteNode txnif.DeleteNode) {
	n.deletes.Load().OnReplayNode(deleteNode.(*DeleteNode))
}

func (n *MVCCHandle) GetDeleteChain() *DeleteChain {
	return n.deletes.Load()
}

func (n *MVCCHandle) IsDeletedLocked(
	row uint32, txn txnif.TxnReader, rwlocker *sync.RWMutex,
) (bool, error) {
	return n.deletes.Load().IsDeleted(row, txn, rwlocker)
}

// it collects all deletes in the range [start, end)
func (n *MVCCHandle) CollectDeleteLocked(
	start, end types.TS, pkType types.Type, mp *mpool.MPool,
) (rowIDVec, commitTSVec, pkVec, abortVec containers.Vector,
	aborts *nulls.Bitmap, deletes []uint32, minTS, persistedTS types.TS,
) {
	if n.deletes.Load().IsEmpty() {
		return
	}
	if !n.ExistDeleteInRange(start, end) {
		return
	}
	persistedTS = n.persistedTS

	for {
		needWaitFound := false
		if rowIDVec != nil {
			rowIDVec.Close()
		}
		rowIDVec = containers.MakeVector(types.T_Rowid.ToType(), mp)
		if commitTSVec != nil {
			commitTSVec.Close()
		}
		commitTSVec = containers.MakeVector(types.T_TS.ToType(), mp)
		if pkVec != nil {
			pkVec.Close()
		}
		pkVec = containers.MakeVector(pkType, mp)
		aborts = &nulls.Bitmap{}
		id := n.meta.ID

		n.deletes.Load().LoopChain(
			func(node *DeleteNode) bool {
				needWait, txn := node.NeedWaitCommitting(end.Next())
				if needWait {
					n.RUnlock()
					txn.GetTxnState(true)
					n.RLock()
					needWaitFound = true
					return false
				}
				if node.nt == NT_Persisted {
					return true
				}
				in, before := node.PreparedIn(start, end)
				if in {
					it := node.mask.Iterator()
					if node.IsAborted() {
						it := node.mask.Iterator()
						for it.HasNext() {
							row := it.Next()
							nulls.Add(aborts, uint64(row))
						}
					}
					for it.HasNext() {
						row := it.Next()
						rowIDVec.Append(*objectio.NewRowid(&id, row), false)
						commitTSVec.Append(node.GetEnd(), false)
						// for deleteNode V1，rowid2PK is nil after restart
						if node.version < IOET_WALTxnCommand_DeleteNode_V2 {
							if deletes == nil {
								deletes = make([]uint32, 0)
							}
							deletes = append(deletes, row)
						} else {
							pkVec.Append(node.rowid2PK[row].Get(0), false)
						}
						if minTS.IsEmpty() {
							minTS = node.GetEnd()
						} else {
							endTS := node.GetEnd()
							if minTS.Greater(&endTS) {
								minTS = node.GetEnd()
							}
						}
					}
				}
				return !before
			})
		if !needWaitFound {
			break
		}
	}
	abortVec = containers.NewConstFixed[bool](types.T_bool.ToType(), false, rowIDVec.Length(), containers.Options{Allocator: mp})
	return
}

// ExistDeleteInRange check if there is any delete in the range [start, end]
// it loops the delete chain and check if there is any delete node in the range
func (n *MVCCHandle) ExistDeleteInRange(start, end types.TS) (exist bool) {
	for {
		needWaitFound := false
		n.deletes.Load().LoopChain(
			func(node *DeleteNode) bool {
				needWait, txn := node.NeedWaitCommitting(end.Next())
				if needWait {
					n.RUnlock()
					txn.GetTxnState(true)
					n.RLock()
					needWaitFound = true
					return false
				}
				in, before := node.PreparedIn(start, end)
				if in {
					exist = true
					return false
				}
				return !before
			})
		if !needWaitFound {
			break
		}
	}

	return
}

func (n *MVCCHandle) GetDeleteNodeByRow(row uint32) (an *DeleteNode) {
	return n.deletes.Load().GetDeleteNodeByRow(row)
}

// ==========================================================
// *************** All appends related APIs *****************
// ==========================================================

// NOTE: after this call all appends related APIs should not be called
// ReleaseAppends release all append nodes.
// it is only called when the appendable block is persisted and the
// memory node is released
func (n *MVCCHandle) ReleaseAppends() {
	n.Lock()
	defer n.Unlock()
	n.appends = nil
}

// only for internal usage
// given a row, it returns the append node which contains the row
func (n *MVCCHandle) GetAppendNodeByRow(row uint32) (an *AppendNode) {
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

// it collects all append nodes in the range [start, end]
// minRow: is the min row
// maxRow: is the max row
// commitTSVec: is the commit ts vector
// abortVec: is the abort vector
// aborts: is the aborted bitmap
func (n *MVCCHandle) CollectAppendLocked(
	start, end types.TS, mp *mpool.MPool,
) (
	minRow, maxRow uint32,
	commitTSVec, abortVec containers.Vector,
	aborts *nulls.Bitmap,
) {
	startOffset, node := n.appends.GetNodeToReadByPrepareTS(start)
	if node != nil {
		prepareTS := node.GetPrepare()
		if prepareTS.Less(&start) {
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

// GetTotalRow is only for replay
func (n *MVCCHandle) GetTotalRow() uint32 {
	an := n.appends.GetUpdateNodeLocked()
	if an == nil {
		return 0
	}
	return an.maxRow - n.deletes.Load().cnt.Load()
}

// it is used to get the visible max row for a txn
// maxrow: is the max row that the txn can see
// visible: is true if the txn can see any row
// holes: is the bitmap of the holes that the txn cannot see
// holes exists only if any append node was rollbacked
func (n *MVCCHandle) GetVisibleRowLocked(
	ctx context.Context,
	txn txnif.TxnReader,
) (maxrow uint32, visible bool, holes *nulls.Bitmap, err error) {
	var holesMax uint32
	anToWait := make([]*AppendNode, 0)
	txnToWait := make([]txnif.TxnReader, 0)
	n.appends.ForEach(func(an *AppendNode) bool {
		needWait, waitTxn := an.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			anToWait = append(anToWait, an)
			txnToWait = append(txnToWait, waitTxn)
			return true
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
func (n *MVCCHandle) CollectUncommittedANodesPreparedBefore(
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

func (n *MVCCHandle) OnReplayAppendNode(an *AppendNode) {
	an.mvcc = n
	n.appends.InsertNode(an)
}

// AddAppendNodeLocked add a new appendnode to the list.
func (n *MVCCHandle) AddAppendNodeLocked(
	txn txnif.AsyncTxn,
	startRow uint32,
	maxRow uint32,
) (an *AppendNode, created bool) {
	if n.appends.IsEmpty() || !n.appends.GetUpdateNodeLocked().IsSameTxn(txn) {
		// if the appends is empty or the last appendnode is not of the same txn,
		// create a new appendnode and append it to the list.
		an = NewAppendNode(txn, startRow, maxRow, n)
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
func (n *MVCCHandle) PrepareCompact() bool {
	return n.allAppendsCommitted()
}

func (n *MVCCHandle) GetLatestAppendPrepareTSLocked() types.TS {
	return n.appends.GetUpdateNodeLocked().Prepare
}

// check if all appendnodes are committed.
func (n *MVCCHandle) allAppendsCommitted() bool {
	n.RLock()
	defer n.RUnlock()
	return n.appends.IsCommitted()
}

// DeleteAppendNodeLocked deletes the appendnode from the append list.
// it is called when txn of the appendnode is aborted.
func (n *MVCCHandle) DeleteAppendNodeLocked(node *AppendNode) {
	n.appends.DeleteNode(node)
}

func (n *MVCCHandle) SetAppendListener(l func(txnif.AppendNode) error) {
	n.appendListener = l
}

func (n *MVCCHandle) GetAppendListener() func(txnif.AppendNode) error {
	return n.appendListener
}

// AllAppendsCommittedBefore returns true if all appendnode is committed before ts.
func (n *MVCCHandle) AllAppendsCommittedBefore(ts types.TS) bool {
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
	return commitTS.Less(&ts)
}
