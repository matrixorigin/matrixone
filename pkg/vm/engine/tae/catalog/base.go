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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

// JXM TODO:
// Generic BaseEntry can't work in go 1.19 because of compiler bug:
// https://github.com/golang/go/issues/54671
// Refactor catalog and use generic BaseEntry after go 1.19.1 release.
type BaseEntry interface {
	RLock()
	RUnlock()

	String() string
	StringLocked() string
	PPString(common.PPLevel, int, string) string

	GetTxn() txnif.TxnReader
	GetID() uint64
	GetIndexes() []*wal.Index
	GetLogIndex() *wal.Index

	GetLatestNodeLocked() txnif.MVCCNode
	IsVisible(ts types.TS, mu *sync.RWMutex) (ok bool, err error)

	HasCommittedNodeInRange(minTs, MaxTs types.TS) bool
	IsCreatingOrAborted() bool
	IsCommitting() bool
	DeleteBefore(ts types.TS) bool
	HasDropCommitted() bool
	HasDropCommittedLocked() bool

	WriteOneNodeTo(w io.Writer) (n int64, err error)
	ReadOneNodeFrom(r io.Reader) (n int64, err error)
	CloneCommittedInRange(start, end types.TS) (ret BaseEntry)

	PrepareCommit() error
	PrepareRollback() (bool, error)
	ApplyCommit(index *wal.Index) error
	ApplyRollback(index *wal.Index) error
}

func CompareUint64(left, right uint64) int {
	if left > right {
		return 1
	} else if left < right {
		return -1
	}
	return 0
}

type BaseEntryImpl[T BaseNode] struct {
	//chain of MetadataMVCCNode
	*txnbase.MVCCChain
	ID uint64
}

func NewReplayBaseEntry[T BaseNode](factory func() T) *BaseEntryImpl[T] {
	be := &BaseEntryImpl[T]{
		MVCCChain: txnbase.NewMVCCChain(CompareBaseNode[T], NewEmptyMVCCNodeFactory(factory)),
	}
	return be
}

func NewBaseEntry[T BaseNode](id uint64, factory func() T) *BaseEntryImpl[T] {
	return &BaseEntryImpl[T]{
		ID:        id,
		MVCCChain: txnbase.NewMVCCChain(CompareBaseNode[T], NewEmptyMVCCNodeFactory(factory)),
	}
}

func (be *BaseEntryImpl[T]) StringLocked() string {
	return fmt.Sprintf("[%d]%s", be.ID, be.MVCCChain.StringLocked())
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
func (be *BaseEntryImpl[T]) HasPersistedData() bool {
	return be.GetMetaLoc() != ""
}
func (be *BaseEntryImpl[T]) GetMetaLoc() string {
	be.RLock()
	defer be.RUnlock()
	if be.GetLatestNodeLocked() == nil {
		return ""
	}
	str := be.GetLatestNodeLocked().(*MVCCNode[*MetadataMVCCNode]).BaseNode.MetaLoc
	return str
}
func (be *BaseEntryImpl[T]) HasPersistedDeltaData() bool {
	return be.GetDeltaLoc() != ""
}
func (be *BaseEntryImpl[T]) GetDeltaLoc() string {
	be.RLock()
	defer be.RUnlock()
	if be.GetLatestNodeLocked() == nil {
		return ""
	}
	str := be.GetLatestNodeLocked().(*MVCCNode[*MetadataMVCCNode]).BaseNode.DeltaLoc
	return str
}

func (be *BaseEntryImpl[T]) GetVisibleMetaLoc(ts types.TS) string {
	be.RLock()
	defer be.RUnlock()
	str := be.GetVisibleNode(ts).(*MVCCNode[*MetadataMVCCNode]).BaseNode.MetaLoc
	return str
}
func (be *BaseEntryImpl[T]) GetVisibleDeltaLoc(ts types.TS) string {
	be.RLock()
	defer be.RUnlock()
	str := be.GetVisibleNode(ts).(*MVCCNode[*MetadataMVCCNode]).BaseNode.DeltaLoc
	return str
}

func (be *BaseEntryImpl[T]) GetID() uint64 { return be.ID }

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

func (be *BaseEntryImpl[T]) CreateWithLoc(ts types.TS, metaLoc string, deltaLoc string) {
	baseNode := &MetadataMVCCNode{
		MetaLoc:  metaLoc,
		DeltaLoc: deltaLoc,
	}
	node := &MVCCNode[T]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
		BaseNode:    baseNode.CloneAll().(T),
	}
	be.Insert(node)
}

func (be *BaseEntryImpl[T]) CreateWithTxnAndSchema(txn txnif.AsyncTxn, schema *Schema) {
	node := &MVCCNode[*TableMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		BaseNode: &TableMVCCNode{
			SchemaConstraints: string(schema.Constraint),
		},
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

func (be *BaseEntryImpl[T]) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	node := be.GetLatestCommittedNode()
	if node == nil {
		return
	}
	if node.(*MVCCNode[T]).HasDropCommitted() {
		return true, node.(*MVCCNode[T]).DeletedAt
	}
	return
}
func (be *BaseEntryImpl[T]) PrepareAdd(txn txnif.TxnReader) (err error) {
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
func (be *BaseEntryImpl[T]) UpdateConstraint(txn txnif.TxnReader, cstr []byte) (isNewNode bool, err error) {
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
	var entry *MVCCNode[T]
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.BaseNode.Update(
		&TableMVCCNode{
			SchemaConstraints: string(cstr),
		})
	return
}

func (be *BaseEntryImpl[T]) CreateWithTxnAndMeta(txn txnif.AsyncTxn, metaLoc string, deltaLoc string) {
	baseNode := &MetadataMVCCNode{
		MetaLoc:  metaLoc,
		DeltaLoc: deltaLoc,
	}
	node := &MVCCNode[T]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		BaseNode:    baseNode.CloneAll().(T),
	}
	be.Insert(node)
}

func (be *BaseEntryImpl[T]) getOrSetUpdateNode(txn txnif.TxnReader) (newNode bool, node *MVCCNode[T]) {
	entry := be.GetLatestNodeLocked()
	if entry.IsSameTxn(txn.GetStartTS()) {
		return false, entry.(*MVCCNode[T])
	} else {
		node := entry.CloneData().(*MVCCNode[T])
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

func (be *BaseEntryImpl[T]) UpdateMetaLoc(txn txnif.TxnReader, metaloc string) (isNewNode bool, err error) {
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
	baseNode := &MetadataMVCCNode{
		MetaLoc: metaloc,
	}
	var entry *MVCCNode[T]
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.BaseNode.Update(baseNode)
	return
}

func (be *BaseEntryImpl[T]) UpdateDeltaLoc(txn txnif.TxnReader, deltaloc string) (isNewNode bool, err error) {
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
	baseNode := &MetadataMVCCNode{
		DeltaLoc: deltaloc,
	}
	var entry *MVCCNode[T]
	isNewNode, entry = be.getOrSetUpdateNode(txn)
	entry.BaseNode.Update(baseNode)
	return
}

func (be *BaseEntryImpl[T]) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(ts)
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
	return un.(*MVCCNode[T]).HasDropCommitted()
}

func (be *BaseEntryImpl[T]) DoCompre(oe *BaseEntryImpl[T]) int {
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	return CompareUint64(be.ID, oe.ID)
}

func (be *BaseEntryImpl[T]) ensureVisibleAndNotDropped(ts types.TS) bool {
	visible, dropped := be.GetVisibilityLocked(ts)
	if !visible {
		return false
	}
	return !dropped
}

func (be *BaseEntryImpl[T]) GetVisibilityLocked(ts types.TS) (visible, dropped bool) {
	un := be.GetVisibleNode(ts)
	if un == nil {
		return
	}
	visible = true
	if un.IsSameTxn(ts) {
		dropped = un.(*MVCCNode[T]).HasDropIntent()
	} else {
		dropped = un.(*MVCCNode[T]).HasDropCommitted()
	}
	return
}

func (be *BaseEntryImpl[T]) IsVisible(ts types.TS, mu *sync.RWMutex) (ok bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(ts)
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	ok = be.ensureVisibleAndNotDropped(ts)
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
	return un.(*MVCCNode[T]).DeletedAt.Greater(ts)
}

func (be *BaseEntryImpl[T]) CloneCommittedInRange(start, end types.TS) BaseEntry {
	chain := be.MVCCChain.CloneCommittedInRange(start, end)
	if chain == nil {
		return nil
	}
	return &BaseEntryImpl[T]{
		MVCCChain: chain,
		ID:        be.ID,
	}
}

func (be *BaseEntryImpl[T]) GetCreatedAt() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*MVCCNode[T]).CreatedAt
}

func (be *BaseEntryImpl[T]) GetDeleteAt() types.TS {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.(*MVCCNode[T]).DeletedAt
}

func (be *BaseEntryImpl[T]) GetVisibility(ts types.TS) (visible, dropped bool) {
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
func (be *BaseEntryImpl[T]) WriteOneNodeTo(w io.Writer) (n int64, err error) {
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
func (be *BaseEntryImpl[T]) WriteAllTo(w io.Writer) (n int64, err error) {
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
func (be *BaseEntryImpl[T]) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
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
func (be *BaseEntryImpl[T]) ReadAllFrom(r io.Reader) (n int64, err error) {
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
