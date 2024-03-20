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
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	EntryMVCCNodeSize int = int(unsafe.Sizeof(EntryMVCCNode{}))
)

type EntryMVCCNode struct {
	CreatedAt, DeletedAt types.TS
}

func EncodeEntryMVCCNode(node *EntryMVCCNode) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(node)), EntryMVCCNodeSize)
}

func DecodeEntryMVCCNode(v []byte) *EntryMVCCNode {
	return (*EntryMVCCNode)(unsafe.Pointer(&v[0]))
}

// Dropped committed
func (un *EntryMVCCNode) HasDropCommitted() bool {
	return !un.DeletedAt.IsEmpty() && un.DeletedAt != txnif.UncommitTS
}

// Dropped committed or uncommitted
func (un *EntryMVCCNode) HasDropIntent() bool {
	return !un.DeletedAt.IsEmpty()
}

func (un *EntryMVCCNode) GetCreatedAt() types.TS {
	return un.CreatedAt
}

func (un *EntryMVCCNode) GetDeletedAt() types.TS {
	return un.DeletedAt
}

func (un *EntryMVCCNode) IsCreating() bool {
	return un.CreatedAt.Equal(&txnif.UncommitTS)
}

func (un *EntryMVCCNode) Clone() *EntryMVCCNode {
	return &EntryMVCCNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
	}
}

func (un *EntryMVCCNode) CloneData() *EntryMVCCNode {
	return &EntryMVCCNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
	}
}

func (un *EntryMVCCNode) Delete() {
	un.DeletedAt = txnif.UncommitTS
}

func (un *EntryMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	if sn, err = r.Read(EncodeEntryMVCCNode(un)); err != nil {
		return
	}
	n += int64(sn)
	return
}
func (un *EntryMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int
	if sn, err = w.Write(EncodeEntryMVCCNode(un)); err != nil {
		return
	}
	n += int64(sn)
	return
}
func (un *EntryMVCCNode) PrepareCommit() (err error) {
	return nil
}
func (un *EntryMVCCNode) String() string {
	return fmt.Sprintf("[C@%s,D@%s]", un.CreatedAt.ToString(), un.DeletedAt.ToString())
}
func (un *EntryMVCCNode) ApplyCommit(ts types.TS) (err error) {
	if un.CreatedAt == txnif.UncommitTS {
		un.CreatedAt = ts
	}
	if un.DeletedAt == txnif.UncommitTS {
		un.DeletedAt = ts
	}
	return nil
}

func (un *EntryMVCCNode) AppendTuple(bat *containers.Batch) {
	startTSVec := bat.GetVectorByName(EntryNode_CreateAt)
	vector.AppendFixed(
		startTSVec.GetDownstreamVector(),
		un.CreatedAt,
		false,
		startTSVec.GetAllocator(),
	)
	vector.AppendFixed(
		bat.GetVectorByName(EntryNode_DeleteAt).GetDownstreamVector(),
		un.DeletedAt,
		false,
		startTSVec.GetAllocator(),
	)
}

func (un *EntryMVCCNode) AppendTupleWithCommitTS(bat *containers.Batch, ts types.TS) {
	startTSVec := bat.GetVectorByName(EntryNode_CreateAt)
	createTS := un.CreatedAt
	if createTS.Equal(&txnif.UncommitTS) {
		createTS = ts
	}
	vector.AppendFixed(
		startTSVec.GetDownstreamVector(),
		createTS,
		false,
		startTSVec.GetAllocator(),
	)
	deleteTS := un.DeletedAt
	if deleteTS.Equal(&txnif.UncommitTS) {
		deleteTS = ts
	}
	vector.AppendFixed(
		bat.GetVectorByName(EntryNode_DeleteAt).GetDownstreamVector(),
		deleteTS,
		false,
		startTSVec.GetAllocator(),
	)
}

func ReadEntryNodeTuple(bat *containers.Batch, row int) (un *EntryMVCCNode) {
	un = &EntryMVCCNode{
		CreatedAt: bat.GetVectorByName(EntryNode_CreateAt).Get(row).(types.TS),
		DeletedAt: bat.GetVectorByName(EntryNode_DeleteAt).Get(row).(types.TS),
	}
	return
}

type BaseNode[T any] interface {
	CloneAll() T
	CloneData() T
	String() string
	Update(vun T)
	IdempotentUpdate(vun T)
	WriteTo(w io.Writer) (n int64, err error)
	ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error)
}

type MVCCNode[T BaseNode[T]] struct {
	*EntryMVCCNode
	*txnbase.TxnMVCCNode
	BaseNode T
}

func NewEmptyMVCCNodeFactory[T BaseNode[T]](factory func() T) func() *MVCCNode[T] {
	return func() *MVCCNode[T] {
		return &MVCCNode[T]{
			EntryMVCCNode: &EntryMVCCNode{},
			TxnMVCCNode:   &txnbase.TxnMVCCNode{},
			BaseNode:      factory(),
		}
	}
}

func CompareBaseNode[T BaseNode[T]](e, o *MVCCNode[T]) int {
	return e.Compare(o.TxnMVCCNode)
}

func (e *MVCCNode[T]) CloneAll() *MVCCNode[T] {
	node := &MVCCNode[T]{
		EntryMVCCNode: e.EntryMVCCNode.Clone(),
		TxnMVCCNode:   e.TxnMVCCNode.CloneAll(),
		BaseNode:      e.BaseNode.CloneAll(),
	}
	return node
}

func (e *MVCCNode[T]) CloneData() *MVCCNode[T] {
	return &MVCCNode[T]{
		EntryMVCCNode: e.EntryMVCCNode.CloneData(),
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
		BaseNode:      e.BaseNode.CloneData(),
	}
}
func (e *MVCCNode[T]) IsNil() bool {
	return e == nil
}
func (e *MVCCNode[T]) String() string {

	return fmt.Sprintf("%s%s%s",
		e.TxnMVCCNode.String(),
		e.EntryMVCCNode.String(),
		e.BaseNode.String())
}

// for create drop in one txn
func (e *MVCCNode[T]) Update(un *MVCCNode[T]) {
	e.CreatedAt = un.CreatedAt
	e.DeletedAt = un.DeletedAt
	e.BaseNode.Update(un.BaseNode)
}

func (e *MVCCNode[T]) IdempotentUpdate(un *MVCCNode[T]) {
	e.CreatedAt = un.CreatedAt
	e.DeletedAt = un.DeletedAt
	e.BaseNode.Update(un.BaseNode)
}

func (e *MVCCNode[T]) ApplyCommit() (err error) {
	var commitTS types.TS
	commitTS, err = e.TxnMVCCNode.ApplyCommit()
	if err != nil {
		return
	}
	err = e.EntryMVCCNode.ApplyCommit(commitTS)
	return err
}
func (e *MVCCNode[T]) PrepareRollback() (err error) {
	return e.TxnMVCCNode.PrepareRollback()
}
func (e *MVCCNode[T]) ApplyRollback() (err error) {
	var commitTS types.TS
	commitTS, err = e.TxnMVCCNode.ApplyRollback()
	if err != nil {
		return
	}
	err = e.EntryMVCCNode.ApplyCommit(commitTS)
	return
}

func (e *MVCCNode[T]) PrepareCommit() (err error) {
	_, err = e.TxnMVCCNode.PrepareCommit()
	if err != nil {
		return
	}
	err = e.EntryMVCCNode.PrepareCommit()
	return
}

func (e *MVCCNode[T]) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	sn, err = e.EntryMVCCNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	sn, err = e.TxnMVCCNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn

	sn, err = e.BaseNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	return
}

func (e *MVCCNode[T]) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	var sn int64
	sn, err = e.EntryMVCCNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn
	sn, err = e.TxnMVCCNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn
	sn, err = e.BaseNode.ReadFromWithVersion(r, ver)
	if err != nil {
		return
	}
	n += sn
	return
}
