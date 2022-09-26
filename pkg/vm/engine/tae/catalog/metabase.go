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

type MetaBaseEntry struct {
	*txnbase.MVCCChain
	ID uint64
}

func NewReplayMetaBaseEntry() *MetaBaseEntry {
	be := &MetaBaseEntry{
		MVCCChain: txnbase.NewMVCCChain(CompareMetaBaseNode, NewEmptyMetadataMVCCNode),
	}
	return be
}

func NewMetaBaseEntry(id uint64) *MetaBaseEntry {
	return &MetaBaseEntry{
		ID:        id,
		MVCCChain: txnbase.NewMVCCChain(CompareMetaBaseNode, NewEmptyMetadataMVCCNode),
	}
}

func (be *MetaBaseEntry) StringLocked() string {
	return fmt.Sprintf("[%d]%s", be.ID, be.MVCCChain.StringLocked())
}

func (be *MetaBaseEntry) String() string {
	be.RLock()
	defer be.RUnlock()
	return be.StringLocked()
}

func (be *MetaBaseEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, be.StringLocked())
	return s
}
func (be *MetaBaseEntry) GetMetaLoc() string {
	be.RLock()
	defer be.RUnlock()
	if be.GetNodeLocked() == nil {
		return ""
	}
	str := be.GetNodeLocked().(*MetadataMVCCNode).MetaLoc
	return str
}
func (be *MetaBaseEntry) GetDeltaLoc() string {
	be.RLock()
	defer be.RUnlock()
	if be.GetNodeLocked() == nil {
		return ""
	}
	str := be.GetNodeLocked().(*MetadataMVCCNode).DeltaLoc
	return str
}

func (be *MetaBaseEntry) GetVisibleMetaLoc(ts types.TS) string {
	be.RLock()
	defer be.RUnlock()
	str := be.GetVisibleNode(ts).(*MetadataMVCCNode).MetaLoc
	return str
}
func (be *MetaBaseEntry) GetVisibleDeltaLoc(ts types.TS) string {
	be.RLock()
	defer be.RUnlock()
	str := be.GetVisibleNode(ts).(*MetadataMVCCNode).DeltaLoc
	return str
}
func (be *MetaBaseEntry) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	node := be.GetCommittedNode()
	if node == nil {
		return
	}
	if node.(*MetadataMVCCNode).HasDropped() {
		return true, node.(*MetadataMVCCNode).DeletedAt
	}
	return
}
func (be *MetaBaseEntry) GetID() uint64 { return be.ID }

func (be *MetaBaseEntry) CreateWithTS(ts types.TS) {
	node := &MetadataMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
	}
	be.Insert(node)
}

func (be *MetaBaseEntry) CreateWithTxn(txn txnif.AsyncTxn) {
	node := &MetadataMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
	}
	be.Insert(node)
}

func (be *MetaBaseEntry) getOrSetUpdateNode(txn txnif.TxnReader) (newNode bool, node *MetadataMVCCNode) {
	entry := be.GetNodeLocked()
	if entry.IsSameTxn(txn.GetStartTS()) {
		return false, entry.(*MetadataMVCCNode)
	} else {
		node := entry.CloneData().(*MetadataMVCCNode)
		node.TxnMVCCNode = txnbase.NewTxnMVCCNodeWithTxn(txn)
		be.Insert(node)
		return true, node
	}
}

func (be *MetaBaseEntry) DeleteLocked(txn txnif.TxnReader) (isNewNode bool, err error) {
	var entry *MetadataMVCCNode
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.Delete()
	return
}

func (be *MetaBaseEntry) UpdateMetaLoc(txn txnif.TxnReader, metaloc string) (isNewNode bool, err error) {
	be.Lock()
	defer be.Unlock()
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		be.Unlock()
		txnToWait.GetTxnState(true)
		be.Lock()
	}
	err = be.CheckConflict(txn)
	if err != nil {
		return
	}
	var entry *MetadataMVCCNode
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.UpdateMetaLoc(metaloc)
	return
}

func (be *MetaBaseEntry) UpdateDeltaLoc(txn txnif.TxnReader, deltaloc string) (isNewNode bool, err error) {
	be.Lock()
	defer be.Unlock()
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		be.Unlock()
		txnToWait.GetTxnState(true)
		be.Lock()
	}
	err = be.CheckConflict(txn)
	if err != nil {
		return
	}
	var entry *MetadataMVCCNode
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.UpdateDeltaLoc(deltaloc)
	return
}

func (be *MetaBaseEntry) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(ts)
}

func (be *MetaBaseEntry) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(startTS)
}

func (be *MetaBaseEntry) IsCreating() bool {
	un := be.GetNodeLocked()
	if un == nil {
		return true
	}
	return un.IsActive()
}

func (be *MetaBaseEntry) IsDroppedCommitted() bool {
	un := be.GetCommittedNode()
	if un == nil {
		return false
	}
	return un.(*MetadataMVCCNode).HasDropped()
}

func (be *MetaBaseEntry) DoCompre(voe BaseEntry) int {
	oe := voe.(*MetaBaseEntry)
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	return CompareUint64(be.ID, oe.ID)
}

func (be *MetaBaseEntry) HasDropped() bool {
	node := be.GetCommittedNode()
	if node == nil {
		return false
	}
	return node.(*MetadataMVCCNode).HasDropped()
}

func (be *MetaBaseEntry) ensureVisibleAndNotDropped(ts types.TS) bool {
	visible, dropped := be.GetVisibilityLocked(ts)
	if !visible {
		return false
	}
	return !dropped
}

func (be *MetaBaseEntry) GetVisibilityLocked(ts types.TS) (visible, dropped bool) {
	un := be.GetVisibleNode(ts)
	if un == nil {
		return
	}
	visible, dropped = true, un.(*MetadataMVCCNode).HasDropped()
	return
}

func (be *MetaBaseEntry) IsVisible(ts types.TS, mu *sync.RWMutex) (ok bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(ts)
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	ok = be.ensureVisibleAndNotDropped(ts)
	return
}

func (be *MetaBaseEntry) CloneCreateEntry() BaseEntry {
	cloned, uncloned := be.CloneLatestNode()
	uncloned.(*MetadataMVCCNode).DeletedAt = types.TS{}
	return &MetaBaseEntry{
		MVCCChain: cloned,
		ID:        be.ID,
	}
}

func (be *MetaBaseEntry) DropEntryLocked(txnCtx txnif.TxnReader) (isNewNode bool, err error) {
	err = be.CheckConflict(txnCtx)
	if err != nil {
		return
	}
	if be.HasDropped() {
		return false, moerr.NewNotFound()
	}
	isNewNode, err = be.DeleteLocked(txnCtx)
	return
}

func (be *MetaBaseEntry) DeleteAfter(ts types.TS) bool {
	un := be.GetNodeLocked()
	if un == nil {
		return false
	}
	return un.(*MetadataMVCCNode).DeletedAt.Greater(ts)
}

func (be *MetaBaseEntry) CloneCommittedInRange(start, end types.TS) BaseEntry {
	chain := be.MVCCChain.CloneCommittedInRange(start, end)
	if chain == nil {
		return nil
	}
	return &MetaBaseEntry{
		MVCCChain: chain,
		ID:        be.ID,
	}
}

func (be *MetaBaseEntry) GetCurrOp() OpT {
	un := be.GetNodeLocked()
	if un == nil {
		return OpCreate
	}
	if !un.(*MetadataMVCCNode).HasDropped() {
		return OpCreate
	}
	return OpSoftDelete
}

func (be *MetaBaseEntry) GetCreatedAt() types.TS {
	un := be.GetNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*MetadataMVCCNode).CreatedAt
}

func (be *MetaBaseEntry) GetDeleteAt() types.TS {
	un := be.GetNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*MetadataMVCCNode).DeletedAt
}

func (be *MetaBaseEntry) GetVisibility(ts types.TS) (visible, dropped bool) {
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
func (be *MetaBaseEntry) WriteOneNodeTo(w io.Writer) (n int64, err error) {
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
func (be *MetaBaseEntry) WriteAllTo(w io.Writer) (n int64, err error) {
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
func (be *MetaBaseEntry) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
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
func (be *MetaBaseEntry) ReadAllFrom(r io.Reader) (n int64, err error) {
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
