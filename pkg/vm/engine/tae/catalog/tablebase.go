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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type TableBaseEntry struct {
	*sync.RWMutex
	MVCC   *common.GenericSortedDList[*TableUpdateNode]
	length uint64
	// meta *
	ID uint64
}

func NewReplayTableBaseEntry() *TableBaseEntry {
	be := &TableBaseEntry{
		RWMutex: &sync.RWMutex{},
		MVCC:    common.NewGenericSortedDList(compareTableUpdateNode),
	}
	return be
}

func NewTableBaseEntry(id uint64) *TableBaseEntry {
	return &TableBaseEntry{
		ID:      id,
		MVCC:    common.NewGenericSortedDList(compareTableUpdateNode),
		RWMutex: &sync.RWMutex{},
	}
}
func (be *TableBaseEntry) StringLocked() string {
	var w bytes.Buffer

	_, _ = w.WriteString(fmt.Sprintf("[%d %p]", be.ID, be.RWMutex))
	it := common.NewGenericSortedDListIt(nil, be.MVCC, false)
	for it.Valid() {
		version := it.Get().GetPayload()
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
		it.Next()
	}
	return w.String()
}

func (be *TableBaseEntry) String() string {
	be.RLock()
	defer be.RUnlock()
	return be.StringLocked()
}

func (be *TableBaseEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, be.StringLocked())
	return s
}

// for replay
func (be *TableBaseEntry) GetTs() types.TS {
	return be.GetUpdateNodeLocked().(*TableUpdateNode).End
}
func (be *TableBaseEntry) GetTxn() txnif.TxnReader { return be.getUpdateNodeLocked().Txn }

func (be *TableBaseEntry) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	vnode := be.GetCommittedNode()
	if vnode == nil {
		return
	}
	node := vnode.(*TableUpdateNode)
	if node.Deleted {
		return true, node.DeletedAt
	}
	return
}
func (be *TableBaseEntry) GetID() uint64 { return be.ID }

func (be *TableBaseEntry) GetIndexes() []*wal.Index {
	ret := make([]*wal.Index, 0)
	be.MVCC.Loop(func(n *common.GenericDLNode[*TableUpdateNode]) bool {
		un := n.GetPayload()
		ret = append(ret, un.LogIndex...)
		return true
	}, true)
	return ret
}
func (be *TableBaseEntry) InsertNode(vun UpdateNodeIf) {
	un := vun.(*TableUpdateNode)
	be.MVCC.Insert(un)
}
func (be *TableBaseEntry) CreateWithTS(ts types.TS) {
	node := &TableUpdateNode{
		EntryUpdateNode: &EntryUpdateNode{
			CreatedAt: ts,
		},
		VisibleUpdateNode: &VisibleUpdateNode{
			Start: ts,
			End:   ts,
		},
	}
	be.InsertNode(node)
}
func (be *TableBaseEntry) CreateWithTxn(txn txnif.AsyncTxn) {
	var startTS types.TS
	if txn != nil {
		startTS = txn.GetStartTS()
	}
	node := &TableUpdateNode{
		EntryUpdateNode: &EntryUpdateNode{},
		VisibleUpdateNode: &VisibleUpdateNode{
			Start: startTS,
			Txn:   txn,
		},
	}
	be.InsertNode(node)
}
func (be *TableBaseEntry) ExistUpdate(minTs, MaxTs types.TS) (exist bool) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*TableUpdateNode]) bool {
		un := n.GetPayload()
		if un.End.IsEmpty() {
			return true
		}
		if un.End.Less(minTs) {
			return false
		}
		if un.End.GreaterEq(minTs) && un.End.LessEq(MaxTs) {
			exist = true
			return false
		}
		return true
	}, false)
	return
}

// TODO update create
func (be *TableBaseEntry) DeleteLocked(txn txnif.TxnReader, impl INode) (node INode, err error) {
	entry := be.MVCC.GetHead().GetPayload()
	if entry.Txn == nil || entry.IsSameTxn(txn.GetStartTS()) {
		if be.HasDropped() {
			err = ErrNotFound
			return
		}
		nbe := entry.cloneData()
		nbe.Start = txn.GetStartTS()
		nbe.End = types.TS{}
		nbe.Txn = txn
		be.InsertNode(nbe)
		node = impl
		err = nbe.ApplyDeleteLocked()
		return
	} else {
		err = txnif.ErrTxnWWConflict
	}
	return
}

// GetUpdateNode gets the latest UpdateNode.
// It is useful in making command, apply state(e.g. ApplyCommit),
// check confilct.
func (be *TableBaseEntry) GetUpdateNodeLocked() UpdateNodeIf {
	head := be.MVCC.GetHead()
	if head == nil {
		return nil
	}
	payload := head.GetPayload()
	if payload == nil {
		return nil
	}
	entry := payload
	return entry
}

func (be *TableBaseEntry) getUpdateNodeLocked() *TableUpdateNode {
	head := be.MVCC.GetHead()
	if head == nil {
		return nil
	}
	payload := head.GetPayload()
	if payload == nil {
		return nil
	}
	entry := payload
	return entry
}

// GetCommittedNode gets the latest committed UpdateNode.
// It's useful when check whether the catalog/metadata entry is deleted.
func (be *TableBaseEntry) GetCommittedNode() (node UpdateNodeIf) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*TableUpdateNode]) bool {
		un := n.GetPayload()
		if !un.IsActive() {
			node = un
			return false
		}
		return true
	}, false)
	return
}

// GetNodeToRead gets UpdateNode according to the timestamp.
// It returns the UpdateNode in the same txn as the read txn
// or returns the latest UpdateNode with commitTS less than the timestamp.
func (be *TableBaseEntry) GetNodeToRead(startts types.TS) (node UpdateNodeIf) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*TableUpdateNode]) bool {
		un := n.GetPayload()
		if un.IsSameTxn(startts) {
			node = un
			return false
		}
		if un.IsActive() || un.IsCommitting() {
			return true
		}
		if un.End.LessEq(startts) {
			node = un
			return false
		}
		return true
	}, false)
	return
}

func (be *TableBaseEntry) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(ts)
}

// GetExactUpdateNode gets the exact UpdateNode with the startTs.
// It's only used in replay
func (be *TableBaseEntry) GetExactUpdateNode(startts types.TS) (node UpdateNodeIf) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*TableUpdateNode]) bool {
		un := n.GetPayload()
		if un.Start == startts {
			node = un
			return false
		}
		// return un.Start < startts
		return true
	}, false)
	return
}

func (be *TableBaseEntry) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetUpdateNodeLocked().(*TableUpdateNode)
	if un == nil {
		return false, nil
	}
	if !un.IsCommitting() {
		return false, nil
	}
	if un.Txn.GetCommitTS().GreaterEq(startTS) {
		return false, nil
	}
	return true, un.Txn
}

func (be *TableBaseEntry) InTxnOrRollbacked() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return true
	}
	return un.IsActive()
}

func (be *TableBaseEntry) IsDroppedCommitted() bool {
	un := be.GetCommittedNode()
	if un == nil {
		return false
	}
	return un.HasDropped()
}

func (be *TableBaseEntry) PrepareWrite(txn txnif.TxnReader) (err error) {
	node := be.GetUpdateNodeLocked().(*TableUpdateNode)
	if node.IsActive() {
		if node.IsSameTxn(txn.GetStartTS()) {
			return
		}
		return txnif.ErrTxnWWConflict
	}

	if node.End.Greater(txn.GetStartTS()) {
		return txnif.ErrTxnWWConflict
	}
	return
}

func (be *TableBaseEntry) WriteOneNodeTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	var n2 int64
	n2, err = be.GetUpdateNodeLocked().WriteTo(w)
	if err != nil {
		return
	}
	n += n2
	return
}

func (be *TableBaseEntry) WriteAllTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, be.length); err != nil {
		return
	}
	n += 8
	be.MVCC.Loop(func(node *common.GenericDLNode[*TableUpdateNode]) bool {
		var n2 int64
		n2, err = node.GetPayload().WriteTo(w)
		if err != nil {
			return false
		}
		n += n2
		return true
	}, true)
	return
}

func (be *TableBaseEntry) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	var n2 int64
	un := NewEmptyTableUpdateNode()
	n2, err = un.ReadFrom(r)
	if err != nil {
		return
	}
	be.InsertNode(un)
	n += n2
	return
}

func (be *TableBaseEntry) ReadAllFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &be.length); err != nil {
		return
	}
	n += 8
	for i := 0; i < int(be.length); i++ {
		var n2 int64
		un := NewEmptyTableUpdateNode()
		n2, err = un.ReadFrom(r)
		if err != nil {
			return
		}
		be.MVCC.Insert(un)
		n += n2
	}
	return
}

func (be *TableBaseEntry) DoCompre(voe BaseEntryIf) int {
	oe := voe.(*TableBaseEntry)
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	// return be.GetUpdateNodeLocked().Compare(oe.GetUpdateNodeLocked())
	return CompareUint64(be.ID, oe.ID)
}

func (be *TableBaseEntry) IsEmpty() bool {
	head := be.MVCC.GetHead()
	return head == nil
}
func (be *TableBaseEntry) ApplyRollback(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyRollback(index)

}

func (be *TableBaseEntry) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyCommit(index)
}

func (be *TableBaseEntry) HasDropped() bool {
	node := be.GetCommittedNode()
	if node == nil {
		return false
	}
	return node.HasDropped()
}
func (be *TableBaseEntry) ExistedForTs(ts types.TS) bool {
	un := be.GetNodeToRead(ts)
	if un == nil {
		return false
	}
	return !un.HasDropped()
}
func (be *TableBaseEntry) GetLogIndex() []*wal.Index {
	node := be.getUpdateNodeLocked()
	if node == nil {
		return nil
	}
	return node.LogIndex
}

func (be *TableBaseEntry) TxnCanRead(txn txnif.AsyncTxn, mu *sync.RWMutex) (canRead bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	canRead = be.ExistedForTs(txn.GetStartTS())
	return
}
func (be *TableBaseEntry) MetaTxnCanRead(txn txnif.AsyncTxn, mu *sync.RWMutex) (canRead bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	canRead = be.ExistedForTs(txn.GetStartTS())
	return
}
func (be *TableBaseEntry) CloneCreateEntry() BaseEntryIf {
	cloned := &TableBaseEntry{
		MVCC:    common.NewGenericSortedDList(compareTableUpdateNode),
		RWMutex: &sync.RWMutex{},
		ID:      be.ID,
	}
	un := be.getUpdateNodeLocked()
	uncloned := un.cloneData()
	uncloned.DeletedAt = types.TS{}
	cloned.InsertNode(un)
	return cloned
}

func (be *TableBaseEntry) DropEntryLocked(txnCtx txnif.TxnReader) error {
	err := be.PrepareWrite(txnCtx)
	if err != nil {
		return err
	}
	if be.HasDropped() {
		return ErrNotFound
	}
	_, err = be.DeleteLocked(txnCtx, nil)
	if err != nil {
		return err
	}
	return nil
}

// In /Catalog, there're three states: Active, Committing and Committed.
// A txn is Active before its CommitTs is allocated.
// It's Committed when its state will never change, i.e. TxnStateCommitted and  TxnStateRollbacked.
// It's Committing when it's in any other state, including TxnStateCommitting, TxnStateRollbacking, TxnStatePrepared and so on. When read or write an entry, if the last txn of the entry is Committing, we wait for it. When write on an Entry, if there's an Active txn, we report w-w conflict.
func (be *TableBaseEntry) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return false
	}
	return node.IsCommitting()
}

func (be *TableBaseEntry) Prepare2PCPrepare() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().Prepare2PCPrepare()
}

func (be *TableBaseEntry) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().PrepareCommit()
}

func (be *TableBaseEntry) PrepareRollback() (bool, error) {
	be.Lock()
	defer be.Unlock()
	node := be.MVCC.GetHead()
	be.MVCC.Delete(node)
	isEmpty := be.IsEmpty()
	return isEmpty, nil
}

func (be *TableBaseEntry) PrepareAdd(txn txnif.TxnReader) (err error) {
	be.RLock()
	defer be.RUnlock()
	if txn != nil {
		needWait, waitTxn := be.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			be.RUnlock()
			waitTxn.GetTxnState(true)
			be.RLock()
		}
		err = be.PrepareWrite(txn)
		if err != nil {
			return
		}
	}
	if txn == nil || be.GetTxn() != txn {
		if !be.HasDropped() {
			return ErrDuplicate
		}
	} else {
		if be.ExistedForTs(txn.GetStartTS()) {
			return ErrDuplicate
		}
	}
	return
}

func (be *TableBaseEntry) DeleteAfter(ts types.TS) bool {
	un := be.getUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.DeletedAt.Greater(ts)
}

func (be *TableBaseEntry) IsCommitted() bool {
	un := be.getUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.Txn == nil
}

func (be *TableBaseEntry) CloneCommittedInRange(start, end types.TS) (ret BaseEntryIf) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*TableUpdateNode]) bool {
		un := n.GetPayload()
		if un.IsActive() {
			return true
		}
		// 1. Committed
		if un.End.GreaterEq(start) && un.End.LessEq(end) {
			if ret == nil {
				ret = NewTableBaseEntry(be.ID)
			}
			ret.InsertNode(un.CloneAll())
			ret.(*TableBaseEntry).length++
		} else if un.End.Greater(end) {
			return false
		}
		return true
	}, true)
	return
}

func (be *TableBaseEntry) GetCurrOp() OpT {
	un := be.getUpdateNodeLocked()
	if un == nil {
		return OpCreate
	}
	if !un.Deleted {
		return OpCreate
	}
	return OpSoftDelete
}

func (be *TableBaseEntry) GetCreatedAt() types.TS {
	un := be.getUpdateNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.CreatedAt
}

func (be *TableBaseEntry) GetDeleteAt() types.TS {
	un := be.getUpdateNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.DeletedAt
}

func (be *TableBaseEntry) TxnCanGet(ts types.TS) (can, dropped bool) {
	be.RLock()
	defer be.RUnlock()
	needWait, txnToWait := be.NeedWaitCommitting(ts)
	if needWait {
		be.RUnlock()
		txnToWait.GetTxnState(true)
		be.RLock()
	}
	un := be.GetNodeToRead(ts)
	if un == nil {
		return
	}
	if un.HasDropped() {
		can, dropped = true, true
		return
	}
	can, dropped = true, false
	return
}
