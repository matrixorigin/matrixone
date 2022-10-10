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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

func compareUpdateNode(a, b *ColumnUpdateNode) int {
	a.RLock()
	defer a.RUnlock()
	b.RLock()
	defer b.RUnlock()
	if a.commitTs == b.commitTs {
		if a.startTs.Less(b.startTs) {
			return -1
		} else if a.startTs.Greater(b.startTs) {
			return 1
		}
		return 0
	}
	if a.commitTs == txnif.UncommitTS {
		return 1
	} else if b.commitTs == txnif.UncommitTS {
		return -1
	}
	return 0
}

type ColumnUpdateNode struct {
	*common.GenericDLNode[*ColumnUpdateNode]
	*sync.RWMutex
	mask *roaring.Bitmap
	vals map[uint32]any
	// nulls    *roaring.Bitmap
	chain     *ColumnChain
	startTs   types.TS
	commitTs  types.TS
	prepareTs types.TS
	txn       txnif.AsyncTxn
	logIndex  *wal.Index
	id        *common.ID
}

func NewSimpleColumnUpdateNode() *ColumnUpdateNode {
	node := &ColumnUpdateNode{
		mask: roaring.NewBitmap(),
		vals: make(map[uint32]any),
	}
	return node
}
func NewCommittedColumnUpdateNode(startTs, commitTs types.TS, id *common.ID, rwlocker *sync.RWMutex) *ColumnUpdateNode {
	if rwlocker == nil {
		rwlocker = &sync.RWMutex{}
	}
	node := &ColumnUpdateNode{
		RWMutex:  rwlocker,
		mask:     roaring.NewBitmap(),
		vals:     make(map[uint32]any),
		startTs:  startTs,
		commitTs: commitTs,
		id:       id,
	}
	return node
}
func NewColumnUpdateNode(txn txnif.AsyncTxn, id *common.ID, rwlocker *sync.RWMutex) *ColumnUpdateNode {
	if rwlocker == nil {
		rwlocker = &sync.RWMutex{}
	}
	node := &ColumnUpdateNode{
		RWMutex: rwlocker,
		mask:    roaring.NewBitmap(),
		vals:    make(map[uint32]any),
		txn:     txn,
		id:      id,
	}
	if txn != nil {
		node.startTs = txn.GetStartTS()
		node.commitTs = txn.GetCommitTS()
	}
	return node
}

func (node *ColumnUpdateNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewUpdateCmd(id, node)
	return
}

func (node *ColumnUpdateNode) Set1PC()     {}
func (node *ColumnUpdateNode) Is1PC() bool { return false }
func (node *ColumnUpdateNode) AttachTo(chain *ColumnChain) {
	node.chain = chain
	node.GenericDLNode = chain.Insert(node)
}

func (node *ColumnUpdateNode) GetID() *common.ID {
	return node.id
}

func (node *ColumnUpdateNode) SetLogIndex(idx *wal.Index) {
	node.logIndex = idx
}

func (node *ColumnUpdateNode) GetChain() txnif.UpdateChain {
	return node.chain
}

// func (node *ColumnUpdateNode) GetDLNode() *common.DLNode {
// 	return node.GenericDLNode
// }

func (node *ColumnUpdateNode) SetMask(mask *roaring.Bitmap) { node.mask = mask }

func (node *ColumnUpdateNode) GetMask() *roaring.Bitmap {
	return node.mask
}
func (node *ColumnUpdateNode) SetValues(vals map[uint32]any) { node.vals = vals }
func (node *ColumnUpdateNode) GetValues() map[uint32]any {
	return node.vals
}

func (node *ColumnUpdateNode) GetValueLocked(row uint32) (v any, err error) {
	v = node.vals[row]
	if v == nil {
		err = moerr.NewNotFound()
	}
	return
}

// func (node *ColumnUpdateNode) HasAnyNullLocked() bool {
// 	if node.nulls == nil {
// 		return false
// 	}
// 	return !node.nulls.IsEmpty()
// }

func (node *ColumnUpdateNode) EqualLocked(o *ColumnUpdateNode) bool {
	if o == nil {
		return node == nil
	}
	for k, v := range node.vals {
		if v != o.vals[k] {
			return false
		}
	}
	return true
}

func (node *ColumnUpdateNode) GetUpdateCntLocked() int {
	return int(node.mask.GetCardinality())
}

// ReadFrom TODO: rewrite
func (node *ColumnUpdateNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	buf := make([]byte, txnbase.IDSize)
	if sn, err = r.Read(buf); err != nil {
		return
	}
	n = int64(sn)
	node.id = txnbase.UnmarshalID(buf)
	node.mask = roaring.New()

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
	if err = node.mask.UnmarshalBinary(buf); err != nil {
		return
	}
	buf = make([]byte, types.TSize)
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(len(buf))
	typ := types.DecodeType(buf)
	var tmpn int64
	vec := containers.MakeVector(typ, true)
	defer vec.Close()
	if tmpn, err = vec.ReadFrom(r); err != nil {
		return
	}
	n += tmpn
	it := node.mask.Iterator()
	row := uint32(0)
	for it.HasNext() {
		key := it.Next()
		v := vec.Get(int(row))
		node.vals[key] = v
		row++
	}
	if err = binary.Read(r, binary.BigEndian, &node.commitTs); err != nil {
		return
	}
	n += 8
	return
}

func (node *ColumnUpdateNode) WriteTo(w io.Writer) (n int64, err error) {
	cn, err := w.Write(txnbase.MarshalID(node.chain.id))
	if err != nil {
		return
	}
	n += int64(cn)
	buf, err := node.mask.ToBytes()
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

	def := node.chain.GetMeta().GetSchema().ColDefs[node.chain.id.Idx]
	if cn, err = w.Write(types.EncodeType(&def.Type)); err != nil {
		return
	}
	n += int64(cn)

	col := containers.MakeVector(def.Type, def.Nullable())
	defer col.Close()
	it := node.mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		col.Append(node.vals[row])
	}
	var tmpn int64
	if tmpn, err = col.WriteTo(w); err != nil {
		return
	}
	n += tmpn
	if err = binary.Write(w, binary.BigEndian, node.commitTs); err != nil {
		return
	}
	n += 8
	return
}

func (node *ColumnUpdateNode) UpdateLocked(row uint32, v any) error {
	node.mask.Add(row)
	if src, ok := v.([]byte); ok {
		dst := make([]byte, len(src))
		copy(dst, src)
		node.vals[row] = dst
	} else {
		node.vals[row] = v
	}
	return nil
}

func (node *ColumnUpdateNode) MergeLocked(o *ColumnUpdateNode) {
	for k, v := range o.vals {
		if vv := node.vals[k]; vv == nil {
			node.mask.Add(k)
			node.vals[k] = v
		}
	}
}

func (node *ColumnUpdateNode) GetStartTS() types.TS        { return node.startTs }
func (node *ColumnUpdateNode) GetCommitTSLocked() types.TS { return node.commitTs }

func (node *ColumnUpdateNode) ApplyToColumn(vec containers.Vector, deletes *roaring.Bitmap) containers.Vector {
	containers.ApplyUpdates(vec, node.mask, node.vals)
	vec.Compact(deletes)
	return vec
}

func (node *ColumnUpdateNode) GeneralDesc() string {
	return fmt.Sprintf("TS=%d;Cnt=%d", node.commitTs, len(node.vals))
}

func (node *ColumnUpdateNode) GeneralString() string {
	return fmt.Sprintf("TS=%d;Cnt=%d", node.commitTs, len(node.vals))
}

func (node *ColumnUpdateNode) GeneralVerboseString() string {
	return fmt.Sprintf("TS=%d;Cnt=%d;Vals=%v", node.commitTs, len(node.vals), node.vals)
}

func (node *ColumnUpdateNode) String() string {
	node.RLock()
	defer node.RUnlock()
	return node.StringLocked()
}

func (node *ColumnUpdateNode) StringLocked() string {
	commitState := "C"
	if node.commitTs == txnif.UncommitTS {
		commitState = "UC"
	}
	s := fmt.Sprintf("[%s:%s](%d-%d)[", commitState, node.id.BlockString(), node.startTs, node.commitTs)
	for k, v := range node.vals {
		s = fmt.Sprintf("%s%d:%v,", s, k, v)
	}
	s = fmt.Sprintf("%s]%s", s, node.logIndex.String())
	return s
}

func (node *ColumnUpdateNode) PrepareCommit() (err error) {
	node.chain.Lock()
	defer node.chain.Unlock()
	if node.commitTs != txnif.UncommitTS {
		return
	}
	// if node.commitTs != txnif.UncommitTS {
	// 	panic("logic error")
	// }
	node.commitTs = node.txn.GetCommitTS()
	node.prepareTs = node.txn.GetCommitTS()
	node.chain.UpdateLocked(node)
	// TODO: merge updates
	return
}

func (node *ColumnUpdateNode) ApplyCommit(index *wal.Index) (err error) {
	node.Lock()
	defer node.Unlock()
	if node.txn == nil {
		panic("ColumnUpdateNode | ApplyCommit | LogicErr")
	}
	node.txn = nil
	node.logIndex = index
	node.chain.mvcc.IncChangeNodeCnt()
	return
}

func (node *ColumnUpdateNode) PrepareRollback() (err error) {
	node.chain.DeleteNode(node)
	return
}

func (node *ColumnUpdateNode) ApplyRollback(index *wal.Index) (err error) {
	node.Lock()
	defer node.Unlock()
	if node.txn == nil {
		panic("ColumnUpdateNode | ApplyCommit | LogicErr")
	}
	node.logIndex = index
	return

}
