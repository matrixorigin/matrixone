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
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type TableBaseEntry struct {
	*txnbase.MVCCChain
	ID uint64
}

func NewReplayTableBaseEntry() *TableBaseEntry {
	be := &TableBaseEntry{
		MVCCChain: txnbase.NewMVCCChain(CompareTableBaseNode, NewEmptyTableMVCCNode),
	}
	return be
}

func NewTableBaseEntry(id uint64) *TableBaseEntry {
	return &TableBaseEntry{
		ID:        id,
		MVCCChain: txnbase.NewMVCCChain(CompareTableBaseNode, NewEmptyTableMVCCNode),
	}
}

func (be *TableBaseEntry) StringLocked() string {
	return fmt.Sprintf("[%d]%s", be.ID, be.MVCCChain.StringLocked())
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

func (be *TableBaseEntry) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	node := be.GetLatestCommittedNode()
	if node == nil {
		return
	}
	if node.(*TableMVCCNode).HasDropCommitted() {
		return true, node.(*TableMVCCNode).DeletedAt
	}
	return
}
func (be *TableBaseEntry) GetID() uint64 { return be.ID }

func (be *TableBaseEntry) CreateWithTS(ts types.TS) {
	node := &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
	}
	be.Insert(node)
}

func (be *TableBaseEntry) CreateWithTxn(txn txnif.AsyncTxn) {
	node := &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
	}
	be.Insert(node)
}

func (be *TableBaseEntry) getOrSetUpdateNode(txn txnif.TxnReader) (newNode bool, node *TableMVCCNode) {
	entry := be.GetLatestNodeLocked()
	if entry.IsSameTxn(txn.GetStartTS()) {
		return false, entry.(*TableMVCCNode)
	} else {
		node := entry.CloneData().(*TableMVCCNode)
		node.TxnMVCCNode = txnbase.NewTxnMVCCNodeWithTxn(txn)
		be.Insert(node)
		return true, node
	}
}

func (be *TableBaseEntry) DeleteLocked(txn txnif.TxnReader) (isNewNode bool, err error) {
	var entry *TableMVCCNode
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.Delete()
	return
}

func (be *TableBaseEntry) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(ts)
}

func (be *TableBaseEntry) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(startTS)
}

func (be *TableBaseEntry) HasDropCommitted() bool {
	be.RLock()
	defer be.RUnlock()
	return be.HasDropCommittedLocked()
}

func (be *TableBaseEntry) HasDropCommittedLocked() bool {
	un := be.GetLatestCommittedNode()
	if un == nil {
		return false
	}
	return un.(*TableMVCCNode).HasDropCommitted()
}

func (be *TableBaseEntry) DoCompre(voe BaseEntry) int {
	oe := voe.(*TableBaseEntry)
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	return CompareUint64(be.ID, oe.ID)
}

func (be *TableBaseEntry) ensureVisibleAndNotDropped(ts types.TS) bool {
	visible, dropped := be.GetVisibilityLocked(ts)
	if !visible {
		return false
	}
	return !dropped
}

func (be *TableBaseEntry) GetVisibilityLocked(ts types.TS) (visible, dropped bool) {
	un := be.GetVisibleNode(ts)
	if un == nil {
		return
	}
	visible = true
	if un.IsSameTxn(ts) {
		dropped = un.(*TableMVCCNode).HasDropIntent()
	} else {
		dropped = un.(*TableMVCCNode).HasDropCommitted()
	}
	return
}

func (be *TableBaseEntry) IsVisible(ts types.TS, mu *sync.RWMutex) (ok bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(ts)
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	ok = be.ensureVisibleAndNotDropped(ts)
	return
}

func (be *TableBaseEntry) DropEntryLocked(txnCtx txnif.TxnReader) (isNewNode bool, err error) {
	err = be.CheckConflict(txnCtx)
	if err != nil {
		return
	}
	if be.HasDropCommittedLocked() {
		return false, moerr.GetOkExpectedEOB()
	}
	isNewNode, err = be.DeleteLocked(txnCtx)
	return
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
		err = be.CheckConflict(txn)
		if err != nil {
			return
		}
	}
	if txn == nil || be.GetTxn() != txn {
		if !be.HasDropCommittedLocked() {
			return moerr.GetOkExpectedDup()
		}
	} else {
		if be.ensureVisibleAndNotDropped(txn.GetStartTS()) {
			return moerr.GetOkExpectedDup()
		}
	}
	return
}

func (be *TableBaseEntry) DeleteAfter(ts types.TS) bool {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false
	}
	return un.(*TableMVCCNode).DeletedAt.Greater(ts)
}

func (be *TableBaseEntry) CloneCommittedInRange(start, end types.TS) BaseEntry {
	chain := be.MVCCChain.CloneCommittedInRange(start, end)
	if chain == nil {
		return nil
	}
	return &TableBaseEntry{
		MVCCChain: chain,
		ID:        be.ID,
	}
}

func (be *TableBaseEntry) GetCreatedAt() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*TableMVCCNode).CreatedAt
}

func (be *TableBaseEntry) GetDeleteAt() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*TableMVCCNode).DeletedAt
}

func (be *TableBaseEntry) GetVisibility(ts types.TS) (visible, dropped bool) {
	be.RLock()
	defer be.RUnlock()
	needWait, txnToWait := be.NeedWaitCommitting(ts)
	if needWait {
		be.RUnlock()
		txnToWait.GetTxnState(true)
		be.RLock()
	}
	return be.GetVisibilityLocked(ts)
}
func (be *TableBaseEntry) WriteOneNodeTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.MVCCChain.WriteOneNodeTo(w)
	if err != nil {
		return
	}
	n += sn
	return
}
func (be *TableBaseEntry) WriteAllTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.MVCCChain.WriteAllTo(w)
	if err != nil {
		return
	}
	n += sn
	return
}
func (be *TableBaseEntry) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.MVCCChain.ReadOneNodeFrom(r)
	if err != nil {
		return
	}
	n += sn
	return
}
func (be *TableBaseEntry) ReadAllFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.MVCCChain.ReadAllFrom(r)
	if err != nil {
		return
	}
	n += sn
	return
}
