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

func CompareUint64(left, right uint64) int {
	if left > right {
		return 1
	} else if left < right {
		return -1
	}
	return 0
}

type BaseEntry struct {
	*sync.RWMutex
	MVCC   *common.GenericSortedDList[*UpdateNode]
	length uint64
	// meta *
	ID uint64
}

func NewReplayBaseEntry() *BaseEntry {
	be := &BaseEntry{
		RWMutex: &sync.RWMutex{},
		MVCC:    common.NewGenericSortedDList[*UpdateNode](compareUpdateNode),
	}
	return be
}

func NewBaseEntry(id uint64) *BaseEntry {
	return &BaseEntry{
		ID:      id,
		MVCC:    common.NewGenericSortedDList[*UpdateNode](compareUpdateNode),
		RWMutex: &sync.RWMutex{},
	}
}
func (be *BaseEntry) StringLocked() string {
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

func (be *BaseEntry) String() string {
	be.RLock()
	defer be.RUnlock()
	return be.StringLocked()
}

func (be *BaseEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, be.StringLocked())
	return s
}

// for replay
func (be *BaseEntry) GetTs() types.TS {
	return be.GetUpdateNodeLocked().End
}
func (be *BaseEntry) GetTxn() txnif.TxnReader { return be.GetUpdateNodeLocked().Txn }

func (be *BaseEntry) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	node := be.GetCommittedNode()
	if node == nil {
		return
	}
	if node.Deleted {
		return true, node.DeletedAt
	}
	return
}
func (be *BaseEntry) GetID() uint64 { return be.ID }

func (be *BaseEntry) GetIndexes() []*wal.Index {
	ret := make([]*wal.Index, 0)
	be.MVCC.Loop(func(n *common.GenericDLNode[*UpdateNode]) bool {
		un := n.GetPayload()
		ret = append(ret, un.LogIndex...)
		return true
	}, true)
	return ret
}
func (be *BaseEntry) InsertNode(un *UpdateNode) {
	be.MVCC.Insert(un)
}
func (be *BaseEntry) CreateWithTS(ts types.TS) {
	node := &UpdateNode{
		CreatedAt: ts,
		Start:     ts,
		End:       ts,
	}
	be.InsertNode(node)
}
func (be *BaseEntry) CreateWithTxn(txn txnif.AsyncTxn) {
	var startTS types.TS
	if txn != nil {
		startTS = txn.GetStartTS()
	}
	node := &UpdateNode{
		Start: startTS,
		Txn:   txn,
	}
	be.InsertNode(node)
}
func (be *BaseEntry) ExistUpdate(minTs, MaxTs types.TS) (exist bool) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*UpdateNode]) bool {
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
func (be *BaseEntry) DeleteLocked(txn txnif.TxnReader, impl INode) (node INode, err error) {
	entry := be.MVCC.GetHead().GetPayload()
	if entry.Txn == nil || entry.IsSameTxn(txn.GetStartTS()) {
		if be.HasDropped() {
			err = ErrNotFound
			return
		}
		nbe := entry.CloneData()
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

func (be *BaseEntry) GetUpdateNodeLocked() *UpdateNode {
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

func (be *BaseEntry) GetCommittedNode() (node *UpdateNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*UpdateNode]) bool {
		un := n.GetPayload()
		if !un.IsActive() {
			node = un
			return false
		}
		return true
	}, false)
	return
}

func (be *BaseEntry) GetNodeToRead(startts types.TS) (node *UpdateNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*UpdateNode]) bool {
		un := n.GetPayload()
		if un.IsActive() {
			if un.IsSameTxn(startts) {
				node = un
				return false
			}
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
func (be *BaseEntry) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(ts)
}

// for replay
func (be *BaseEntry) GetExactUpdateNode(startts types.TS) (node *UpdateNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*UpdateNode]) bool {
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

func (be *BaseEntry) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false, nil
	}
	if !un.IsCommitting() {
		return false, nil
	}
	if un.Txn.GetStartTS().Equal(startTS) {
		return false, nil
	}
	return true, un.Txn
}

// active -> w-w/same
// rollbacked/ing -> next
// committing shouldRead

// get first
// get committed
// get same node
// get to read
func (be *BaseEntry) InTxnOrRollbacked() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return true
	}
	return un.IsActive()
}

func (be *BaseEntry) IsDroppedCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	if un.IsActive() {
		return false
	}
	return un.HasDropped()
}

func (be *BaseEntry) PrepareWrite(txn txnif.TxnReader, rwlocker *sync.RWMutex) (err error) {
	node := be.GetUpdateNodeLocked()
	if node.IsActive() {
		if node.IsSameTxn(txn.GetStartTS()) {
			return
		}
		return txnif.ErrTxnWWConflict
	}

	if node.Start.Greater(txn.GetStartTS()) {
		return txnif.ErrTxnWWConflict
	}
	return
}

func (be *BaseEntry) WriteOneNodeTo(w io.Writer) (n int64, err error) {
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

func (be *BaseEntry) WriteAllTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, be.length); err != nil {
		return
	}
	n += 8
	be.MVCC.Loop(func(node *common.GenericDLNode[*UpdateNode]) bool {
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

func (be *BaseEntry) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	var n2 int64
	un := NewEmptyUpdateNode()
	n2, err = un.ReadFrom(r)
	if err != nil {
		return
	}
	be.InsertNode(un)
	n += n2
	return
}

func (be *BaseEntry) ReadAllFrom(r io.Reader) (n int64, err error) {
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
		un := NewEmptyUpdateNode()
		n2, err = un.ReadFrom(r)
		if err != nil {
			return
		}
		be.MVCC.Insert(un)
		n += n2
	}
	return
}

func (be *BaseEntry) DoCompre(oe *BaseEntry) int {
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	// return be.GetUpdateNodeLocked().Compare(oe.GetUpdateNodeLocked())
	return CompareUint64(be.ID, oe.ID)
}

func (be *BaseEntry) IsEmpty() bool {
	head := be.MVCC.GetHead()
	return head == nil
}
func (be *BaseEntry) ApplyRollback(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyRollback(index)

}

func (be *BaseEntry) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyCommit(index)
}

func (be *BaseEntry) HasDropped() bool {
	node := be.GetCommittedNode()
	if node == nil {
		return false
	}
	return node.HasDropped()
}
func (be *BaseEntry) ExistedForTs(ts types.TS) bool {
	un := be.GetExactUpdateNode(ts)
	if un == nil {
		un = be.GetNodeToRead(ts)
	}
	if un == nil {
		return false
	}
	return !un.HasDropped()
}
func (be *BaseEntry) GetLogIndex() []*wal.Index {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return nil
	}
	return node.LogIndex
}

func (be *BaseEntry) OnReplay(node *UpdateNode) error {
	be.InsertNode(node)
	return nil
}
func (be *BaseEntry) TxnCanRead(txn txnif.AsyncTxn, mu *sync.RWMutex) (canRead bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	canRead = be.ExistedForTs(txn.GetStartTS())
	return
}
func (be *BaseEntry) CloneCreateEntry() *BaseEntry {
	cloned := &BaseEntry{
		MVCC:    common.NewGenericSortedDList[*UpdateNode](compareUpdateNode),
		RWMutex: &sync.RWMutex{},
		ID:      be.ID,
	}
	un := be.GetUpdateNodeLocked()
	uncloned := un.CloneData()
	uncloned.DeletedAt = types.TS{}
	cloned.InsertNode(un)
	return cloned
}
func (be *BaseEntry) DropEntryLocked(txnCtx txnif.TxnReader) error {
	node := be.GetUpdateNodeLocked()
	if node.IsActive() && !node.IsSameTxn(txnCtx.GetStartTS()) {
		return txnif.ErrTxnWWConflict
	}
	if be.HasDropped() {
		return ErrNotFound
	}
	_, err := be.DeleteLocked(txnCtx, nil)
	if err != nil {
		return err
	}
	return nil
}

func (be *BaseEntry) SameTxn(o *BaseEntry) bool {
	id := be.GetTxnID()
	if id == 0 {
		return false
	}
	return id == o.GetTxnID()
}

func (be *BaseEntry) GetTxnID() uint64 {
	node := be.GetUpdateNodeLocked()
	if node.Txn != nil {
		return node.Txn.GetID()
	}
	return 0
}

func (be *BaseEntry) IsSameTxn(ctx txnif.TxnReader) bool {
	return be.GetTxnID() == ctx.GetID()
}

func (be *BaseEntry) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	return node.State == txnif.TxnStateCommitting
}

func (be *BaseEntry) GetDeleted() bool {
	return be.GetUpdateNodeLocked().Deleted
}

func (be *BaseEntry) Prepare2PCPrepare() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().Prepare2PCPrepare()
}

func (be *BaseEntry) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().PrepareCommit()
}

func (be *BaseEntry) PrepareRollback() (bool, error) {
	be.Lock()
	defer be.Unlock()
	node := be.MVCC.GetHead()
	be.MVCC.Delete(node)
	isEmpty := be.IsEmpty()
	return isEmpty, nil
}

func (be *BaseEntry) PrepareAdd(txn txnif.TxnReader) (err error) {
	be.RLock()
	defer be.RUnlock()
	if txn != nil {
		needWait, waitTxn := be.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			be.RUnlock()
			waitTxn.GetTxnState(true)
			be.RLock()
		}
		err = be.PrepareWrite(txn, be.RWMutex)
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

func (be *BaseEntry) DeleteAfter(ts types.TS) bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.DeletedAt.Greater(ts)
}
func (be *BaseEntry) IsCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.Txn == nil
}

func (be *BaseEntry) CloneCommittedInRange(start, end types.TS) (ret *BaseEntry) {
	be.MVCC.Loop(func(n *common.GenericDLNode[*UpdateNode]) bool {
		un := n.GetPayload()
		if un.IsActive() {
			return true
		}
		// 1. Committed
		if un.End.GreaterEq(start) && un.End.LessEq(end) {
			if ret == nil {
				ret = NewBaseEntry(be.ID)
			}
			ret.InsertNode(un.CloneAll())
			ret.length++
		} else if un.End.Greater(end) {
			return false
		}
		return true
	}, true)
	return
}

func (be *BaseEntry) GetCurrOp() OpT {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return OpCreate
	}
	if !un.Deleted {
		return OpCreate
	}
	return OpSoftDelete
}

func (be *BaseEntry) GetCreatedAt() types.TS {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.CreatedAt
}

func (be *BaseEntry) GetDeleteAt() types.TS {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.DeletedAt
}

func (be *BaseEntry) TxnCanGet(ts types.TS) (can, dropped bool) {
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
