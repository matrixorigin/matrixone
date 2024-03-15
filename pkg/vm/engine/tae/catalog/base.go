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

package catalog

import (
	"fmt"
	// "io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type BaseEntry interface {
	//for global checkpoint
	RLock()
	RUnlock()
	DeleteBefore(ts types.TS) bool
}

func CompareUint64(left, right uint64) int {
	if left > right {
		return 1
	} else if left < right {
		return -1
	}
	return 0
}

type BaseEntryImpl[T BaseNode[T]] struct {
	//chain of MetadataMVCCNode
	*txnbase.MVCCChain[*MVCCNode[T]]
}

func NewReplayBaseEntry[T BaseNode[T]](factory func() T) *BaseEntryImpl[T] {
	be := &BaseEntryImpl[T]{
		MVCCChain: txnbase.NewMVCCChain(CompareBaseNode[T], NewEmptyMVCCNodeFactory(factory)),
	}
	return be
}

func NewBaseEntry[T BaseNode[T]](factory func() T) *BaseEntryImpl[T] {
	return &BaseEntryImpl[T]{
		MVCCChain: txnbase.NewMVCCChain(CompareBaseNode[T], NewEmptyMVCCNodeFactory(factory)),
	}
}

func (be *BaseEntryImpl[T]) StringLocked() string {
	return be.MVCCChain.StringLocked()
}

func (be *BaseEntryImpl[T]) String() string {
	be.RLock()
	defer be.RUnlock()
	return be.StringLocked()
}

func (be *BaseEntryImpl[T]) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, be.StringLocked())
	return s
}

func (be *BaseEntryImpl[T]) CreateWithTS(ts types.TS, baseNode T) {
	node := &MVCCNode[T]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
		BaseNode:    baseNode,
	}
	be.Insert(node)
}

func (be *BaseEntryImpl[T]) CreateWithTxn(txn txnif.AsyncTxn, baseNode T) {
	node := &MVCCNode[T]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		BaseNode:    baseNode,
	}
	be.Insert(node)
}

// used when replay
func (be *BaseEntryImpl[T]) CreateWithStartAndEnd(start, end types.TS, baseNode T) {
	node := &MVCCNode[T]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: end,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithStartEnd(start, end),
		BaseNode:    baseNode,
	}
	be.Insert(node)
}

func (be *BaseEntryImpl[T]) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	node := be.GetLatestCommittedNode()
	if node == nil {
		return
	}
	if node.HasDropCommitted() {
		return true, node.DeletedAt
	}
	return
}
func (be *BaseEntryImpl[T]) PrepareAdd(txn txnif.TxnReader) (err error) {
	if err = be.ConflictCheck(txn); err != nil {
		return
	}
	// check duplication then
	be.RLock()
	defer be.RUnlock()
	if txn == nil || be.GetTxn() != txn {
		if !be.HasDropCommittedLocked() {
			return moerr.GetOkExpectedDup()
		}
	} else {
		if be.ensureVisibleAndNotDropped(txn) {
			return moerr.GetOkExpectedDup()
		}
	}
	return
}

func (be *BaseEntryImpl[T]) ConflictCheck(txn txnif.TxnReader) (err error) {
	be.RLock()
	defer be.RUnlock()
	if txn != nil {
		needWait, waitTxn := be.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			be.RUnlock()
			waitTxn.GetTxnState(true)
			be.RLock()
		}
		err = be.CheckConflict(txn)
		if err != nil {
			return
		}
	}
	return
}
func (be *BaseEntryImpl[T]) getOrSetUpdateNode(txn txnif.TxnReader) (newNode bool, node *MVCCNode[T]) {
	entry := be.GetLatestNodeLocked()
	if entry.IsSameTxn(txn) {
		return false, entry
	} else {
		node := entry.CloneData()
		node.TxnMVCCNode = txnbase.NewTxnMVCCNodeWithTxn(txn)
		be.Insert(node)
		return true, node
	}
}

func (be *BaseEntryImpl[T]) DeleteLocked(txn txnif.TxnReader) (isNewNode bool, err error) {
	var entry *MVCCNode[T]
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.Delete()
	return
}

func (be *BaseEntryImpl[T]) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(&ts)
}

func (be *BaseEntryImpl[T]) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(startTS)
}

func (be *BaseEntryImpl[T]) HasDropCommitted() bool {
	be.RLock()
	defer be.RUnlock()
	return be.HasDropCommittedLocked()
}

func (be *BaseEntryImpl[T]) HasDropCommittedLocked() bool {
	un := be.GetLatestCommittedNode()
	if un == nil {
		return false
	}
	return un.HasDropCommitted()
}

func (be *BaseEntryImpl[T]) ensureVisibleAndNotDropped(txn txnif.TxnReader) bool {
	visible, dropped := be.GetVisibilityLocked(txn)
	if !visible {
		return false
	}
	return !dropped
}

func (be *BaseEntryImpl[T]) GetVisibilityLocked(txn txnif.TxnReader) (visible, dropped bool) {
	un := be.GetVisibleNode(txn)
	if un == nil {
		return
	}
	visible = true
	if un.IsSameTxn(txn) {
		dropped = un.HasDropIntent()
	} else {
		dropped = un.HasDropCommitted()
	}
	return
}

func (be *BaseEntryImpl[T]) IsVisible(txn txnif.TxnReader, mu *sync.RWMutex) (ok bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	ok = be.ensureVisibleAndNotDropped(txn)
	return
}

func (be *BaseEntryImpl[T]) DropEntryLocked(txn txnif.TxnReader) (isNewNode bool, err error) {
	err = be.CheckConflict(txn)
	if err != nil {
		return
	}
	if be.HasDropCommittedLocked() {
		return false, moerr.GetOkExpectedEOB()
	}
	isNewNode, err = be.DeleteLocked(txn)
	return
}

func (be *BaseEntryImpl[T]) DeleteAfter(ts types.TS) bool {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false
	}
	return un.DeletedAt.Greater(&ts)
}

func (be *BaseEntryImpl[T]) CloneCommittedInRange(start, end types.TS) BaseEntry {
	chain := be.MVCCChain.CloneCommittedInRange(start, end)
	if chain == nil {
		return nil
	}
	return &BaseEntryImpl[T]{
		MVCCChain: chain,
	}
}

func (be *BaseEntryImpl[T]) GetCreatedAtLocked() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.CreatedAt
}

func (be *BaseEntryImpl[T]) GetDeleteAt() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.DeletedAt
}

func (be *BaseEntryImpl[T]) GetVisibility(txn txnif.TxnReader) (visible, dropped bool) {
	be.RLock()
	defer be.RUnlock()
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		be.RUnlock()
		txnToWait.GetTxnState(true)
		be.RLock()
	}
	return be.GetVisibilityLocked(txn)
}
