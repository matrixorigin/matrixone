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

type DBBaseEntry struct {
	*txnbase.MVCCChain
	ID uint64
}

func NewReplayDBBaseEntry() *DBBaseEntry {
	be := &DBBaseEntry{
		MVCCChain: txnbase.NewMVCCChain(CompareDBBaseNode, NewEmptyDBMVCCNode),
	}
	return be
}

func NewDBBaseEntry(id uint64) *DBBaseEntry {
	return &DBBaseEntry{
		ID:        id,
		MVCCChain: txnbase.NewMVCCChain(CompareDBBaseNode, NewEmptyDBMVCCNode),
	}
}

func (be *DBBaseEntry) StringLocked() string {
	return fmt.Sprintf("[%d]%s", be.ID, be.MVCCChain.StringLocked())
}

func (be *DBBaseEntry) String() string {
	be.RLock()
	defer be.RUnlock()
	return be.StringLocked()
}

func (be *DBBaseEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, be.StringLocked())
	return s
}

func (be *DBBaseEntry) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	node := be.GetLatestCommittedNode()
	if node == nil {
		return
	}
	if node.(*DBMVCCNode).HasDropCommitted() {
		return true, node.(*DBMVCCNode).DeletedAt
	}
	return
}
func (be *DBBaseEntry) GetID() uint64 { return be.ID }

func (be *DBBaseEntry) CreateWithTS(ts types.TS) {
	node := &DBMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
	}
	be.Insert(node)
}

func (be *DBBaseEntry) CreateWithTxn(txn txnif.AsyncTxn) {
	node := &DBMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
	}
	be.Insert(node)
}

func (be *DBBaseEntry) getOrSetUpdateNode(txn txnif.TxnReader) (newNode bool, node *DBMVCCNode) {
	entry := be.GetLatestNodeLocked()
	if entry.IsSameTxn(txn.GetStartTS()) {
		return false, entry.(*DBMVCCNode)
	} else {
		node := entry.CloneData().(*DBMVCCNode)
		node.TxnMVCCNode = txnbase.NewTxnMVCCNodeWithTxn(txn)
		be.Insert(node)
		return true, node
	}
}

func (be *DBBaseEntry) DeleteLocked(txn txnif.TxnReader) (isNewNode bool, err error) {
	var node *DBMVCCNode
	isNewNode, node = be.getOrSetUpdateNode(txn)
	node.Delete()
	return
}

func (be *DBBaseEntry) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(ts)
}

func (be *DBBaseEntry) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(startTS)
}

func (be *DBBaseEntry) HasDropCommitted() bool {
	be.RLock()
	defer be.RUnlock()
	return be.HasDropCommittedLocked()
}

func (be *DBBaseEntry) HasDropCommittedLocked() bool {
	un := be.GetLatestCommittedNode()
	if un == nil {
		return false
	}
	return un.(*DBMVCCNode).HasDropCommitted()
}

func (be *DBBaseEntry) DoCompre(voe BaseEntry) int {
	oe := voe.(*DBBaseEntry)
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	return CompareUint64(be.ID, oe.ID)
}

func (be *DBBaseEntry) ensureVisibleAndNotDropped(ts types.TS) bool {
	visible, dropped := be.GetVisibilityLocked(ts)
	if !visible {
		return false
	}
	return !dropped
}

func (be *DBBaseEntry) GetVisibilityLocked(ts types.TS) (visible, dropped bool) {
	un := be.GetVisibleNode(ts)
	if un == nil {
		return
	}
	visible = true
	if un.IsSameTxn(ts) {
		dropped = un.(*DBMVCCNode).HasDropIntent()
	} else {
		dropped = un.(*DBMVCCNode).HasDropCommitted()
	}
	return
}

func (be *DBBaseEntry) IsVisible(ts types.TS, mu *sync.RWMutex) (ok bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(ts)
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	ok = be.ensureVisibleAndNotDropped(ts)
	return
}

func (be *DBBaseEntry) DropEntryLocked(txn txnif.TxnReader) (isNewNode bool, err error) {
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

func (be *DBBaseEntry) PrepareAdd(txn txnif.TxnReader) (err error) {
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
			return moerr.NewDuplicate()
		}
	} else {
		if be.ensureVisibleAndNotDropped(txn.GetStartTS()) {
			return moerr.NewDuplicate()
		}
	}
	return
}

func (be *DBBaseEntry) DeleteAfter(ts types.TS) bool {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false
	}
	return un.(*DBMVCCNode).DeletedAt.Greater(ts)
}

func (be *DBBaseEntry) CloneCommittedInRange(start, end types.TS) BaseEntry {
	chain := be.MVCCChain.CloneCommittedInRange(start, end)
	if chain == nil {
		return nil
	}
	return &DBBaseEntry{
		MVCCChain: chain,
		ID:        be.ID,
	}
}

func (be *DBBaseEntry) GetCreatedAt() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*DBMVCCNode).CreatedAt
}

func (be *DBBaseEntry) GetDeleteAt() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*DBMVCCNode).DeletedAt
}

func (be *DBBaseEntry) GetVisibility(ts types.TS) (visible, dropped bool) {
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
func (be *DBBaseEntry) WriteOneNodeTo(w io.Writer) (n int64, err error) {
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
func (be *DBBaseEntry) WriteAllTo(w io.Writer) (n int64, err error) {
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
func (be *DBBaseEntry) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
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
func (be *DBBaseEntry) ReadAllFrom(r io.Reader) (n int64, err error) {
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
