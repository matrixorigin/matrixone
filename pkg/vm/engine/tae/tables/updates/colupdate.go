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
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type ColumnNode struct {
	*common.DLNode
	*sync.RWMutex
	txnMask  *roaring.Bitmap
	txnVals  map[uint32]interface{}
	chain    *ColumnChain
	startTs  uint64
	commitTs uint64
	txn      txnif.AsyncTxn
	logIndex *wal.Index
	id       *common.ID
}

func NewSimpleColumnNode() *ColumnNode {
	node := &ColumnNode{
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
	}
	return node
}
func NewCommittedColumnNode(startTs, commitTs uint64, id *common.ID, rwlocker *sync.RWMutex) *ColumnNode {
	if rwlocker == nil {
		rwlocker = &sync.RWMutex{}
	}
	node := &ColumnNode{
		RWMutex:  rwlocker,
		txnMask:  roaring.NewBitmap(),
		txnVals:  make(map[uint32]interface{}),
		startTs:  startTs,
		commitTs: commitTs,
		id:       id,
	}
	return node
}
func NewColumnNode(txn txnif.AsyncTxn, id *common.ID, rwlocker *sync.RWMutex) *ColumnNode {
	if rwlocker == nil {
		rwlocker = &sync.RWMutex{}
	}
	node := &ColumnNode{
		RWMutex: rwlocker,
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
		txn:     txn,
		id:      id,
	}
	if txn != nil {
		node.startTs = txn.GetStartTS()
		node.commitTs = txn.GetCommitTS()
	}
	return node
}

func (node *ColumnNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewUpdateCmd(id, node)
	return
}

func (node *ColumnNode) AttachTo(chain *ColumnChain) {
	node.chain = chain
	node.DLNode = chain.Insert(node)
}

func (node *ColumnNode) GetID() *common.ID {
	return node.id
}

func (node *ColumnNode) GetChain() txnif.UpdateChain {
	return node.chain
}

func (node *ColumnNode) GetDLNode() *common.DLNode {
	return node.DLNode
}

func (node *ColumnNode) Compare(o common.NodePayload) int {
	op := o.(*ColumnNode)
	node.RLock()
	defer node.RUnlock()
	op.RLock()
	defer op.RUnlock()
	if node.commitTs == op.commitTs {
		if node.startTs < op.startTs {
			return -1
		} else if node.startTs > op.startTs {
			return 1
		}
		return 0
	}
	if node.commitTs == txnif.UncommitTS {
		return 1
	} else if op.commitTs == txnif.UncommitTS {
		return -1
	}
	return 0
}

func (node *ColumnNode) GetValueLocked(row uint32) (v interface{}, err error) {
	v = node.txnVals[row]
	if v == nil {
		err = txnbase.ErrNotFound
	}
	return
}
func (node *ColumnNode) HasUpdateLocked(row uint32) bool {
	return node.txnVals[row] != nil
}

func (node *ColumnNode) EqualLocked(o *ColumnNode) bool {
	if o == nil {
		return node == nil
	}
	for k, v := range node.txnVals {
		if v != o.txnVals[k] {
			return false
		}
	}
	return true
}

func (node *ColumnNode) GetUpdateCntLocked() int {
	return int(node.txnMask.GetCardinality())
}

// TODO: rewrite
func (node *ColumnNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	buf := make([]byte, txnbase.IDSize)
	if sn, err = r.Read(buf); err != nil {
		return
	}
	n = int64(sn)
	node.id = txnbase.UnmarshalID(buf)
	node.txnMask = roaring.New()

	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	buf = make([]byte, length)
	if sn, err = r.Read(buf); err != nil {
		return
	}
	n += int64(sn)
	if err = node.txnMask.UnmarshalBinary(buf); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	buf = make([]byte, length)
	if sn, err = r.Read(buf); err != nil {
		return
	}
	n += int64(sn)
	vals := gvec.Vector{}
	vals.Nsp = &nulls.Nulls{}
	if err = vals.Read(buf); err != nil {
		return
	}
	it := node.txnMask.Iterator()
	row := uint32(0)
	for it.HasNext() {
		key := it.Next()
		v := compute.GetValue(&vals, row)
		node.txnVals[key] = v
		row++
	}
	return
}

// TODO: rewrite later
func (node *ColumnNode) WriteTo(w io.Writer) (n int64, err error) {
	cn, err := w.Write(txnbase.MarshalID(node.chain.id))
	if err != nil {
		return
	}
	n += int64(cn)
	buf, err := node.txnMask.ToBytes()
	if err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
		return
	}
	n += 4

	if _, err = w.Write(buf); err != nil {
		return
	}
	n += int64(len(buf))

	col := gvec.New(node.chain.GetMeta().GetSchema().ColDefs[node.chain.id.Idx].Type)
	it := node.txnMask.Iterator()
	for it.HasNext() {
		row := it.Next()
		compute.AppendValue(col, node.txnVals[row])
	}
	buf, err = col.Show()
	if err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
		return
	}
	n += 4
	cn, err = w.Write(buf)
	n += int64(cn)
	return
}

func (node *ColumnNode) UpdateLocked(row uint32, v interface{}) error {
	node.txnMask.Add(row)
	node.txnVals[row] = v
	return nil
}

func (node *ColumnNode) MergeLocked(o *ColumnNode) error {
	for k, v := range o.txnVals {
		if vv := node.txnVals[k]; vv == nil {
			node.txnMask.Add(k)
			node.txnVals[k] = v
		}
	}
	return nil
}

func (node *ColumnNode) GetStartTS() uint64        { return node.startTs }
func (node *ColumnNode) GetCommitTSLocked() uint64 { return node.commitTs }

func (node *ColumnNode) ApplyToColumn(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	vec = compute.ApplyUpdateToVector(vec, node.txnMask, node.txnVals)
	vec = compute.ApplyDeleteToVector(vec, deletes)
	return vec
}

func (node *ColumnNode) String() string {
	node.RLock()
	defer node.RUnlock()
	return node.StringLocked()
}

func (node *ColumnNode) StringLocked() string {
	commitState := "C"
	if node.commitTs == txnif.UncommitTS {
		commitState = "UC"
	}
	s := fmt.Sprintf("[%s:%s](%d-%d)[", commitState, node.id.ToBlockFileName(), node.startTs, node.commitTs)
	for k, v := range node.txnVals {
		s = fmt.Sprintf("%s%d:%v,", s, k, v)
	}
	s = fmt.Sprintf("%s]%s", s, node.logIndex.String())
	return s
}

func (node *ColumnNode) PrepareCommit() (err error) {
	node.chain.Lock()
	defer node.chain.Unlock()
	if node.commitTs != txnif.UncommitTS {
		return
	}
	// if node.commitTs != txnif.UncommitTS {
	// 	panic("logic error")
	// }
	node.commitTs = node.txn.GetCommitTS()
	node.chain.UpdateLocked(node)
	// TODO: merge updates
	return
}

func (node *ColumnNode) ApplyCommit(index *wal.Index) (err error) {
	node.Lock()
	defer node.Unlock()
	if node.txn == nil {
		panic("not expected")
	}
	node.txn = nil
	node.logIndex = index
	node.chain.controller.SetMaxVisible(node.commitTs)
	node.chain.controller.IncChangeNodeCnt()
	return
}

func (node *ColumnNode) PrepareRollback() (err error) {
	node.chain.DeleteNode(node.DLNode)
	return
}

func (node *ColumnNode) ApplyRollback() (err error) { return }
