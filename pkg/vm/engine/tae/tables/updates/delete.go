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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type NodeType int8

const (
	NT_Normal NodeType = iota
	NT_Merge
)

func (node *DeleteNode) Less(b *DeleteNode) int {
	return node.TxnMVCCNode.Compare2(b.TxnMVCCNode)
}

type DeleteNode struct {
	*common.GenericDLNode[*DeleteNode]
	*txnbase.TxnMVCCNode
	chain      *DeleteChain
	logIndexes []*wal.Index
	mask       *roaring.Bitmap
	nt         NodeType
	id         *common.ID
	dt         handle.DeleteType
	viewNodes  map[uint32]*common.GenericDLNode[*DeleteNode]
}

func NewMergedNode(commitTs types.TS) *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(commitTs),
		mask:        roaring.New(),
		nt:          NT_Merge,
		logIndexes:  make([]*wal.Index, 0),
		viewNodes:   make(map[uint32]*common.GenericDLNode[*DeleteNode]),
	}
	return n
}
func NewEmptyDeleteNode() *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(nil),
		mask:        roaring.New(),
		nt:          NT_Normal,
		viewNodes:   make(map[uint32]*common.GenericDLNode[*DeleteNode]),
		id:          &common.ID{},
	}
	return n
}
func NewDeleteNode(txn txnif.AsyncTxn, dt handle.DeleteType) *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		mask:        roaring.New(),
		nt:          NT_Normal,
		dt:          dt,
		viewNodes:   make(map[uint32]*common.GenericDLNode[*DeleteNode]),
	}
	if n.dt == handle.DT_MergeCompact {
		_, err := n.TxnMVCCNode.PrepareCommit()
		if err != nil {
			panic(err)
		}
	}
	return n
}

func (node *DeleteNode) CloneAll() *DeleteNode  { panic("todo") }
func (node *DeleteNode) CloneData() *DeleteNode { panic("todo") }
func (node *DeleteNode) Update(*DeleteNode)     { panic("todo") }
func (node *DeleteNode) IsNil() bool            { return node == nil }
func (node *DeleteNode) GetPrepareTS() types.TS {
	return node.TxnMVCCNode.GetPrepare()
}

func (node *DeleteNode) GetID() *common.ID {
	return node.id
}
func (node *DeleteNode) AddLogIndexesLocked(indexes []*wal.Index) {
	node.logIndexes = append(node.logIndexes, indexes...)
}

func (node *DeleteNode) SetDeletes(mask *roaring.Bitmap) {
	node.mask = mask
}

func (node *DeleteNode) AddLogIndexLocked(index *wal.Index) {
	node.logIndexes = append(node.logIndexes, index)
}

func (node *DeleteNode) IsMerged() bool { return node.nt == NT_Merge }
func (node *DeleteNode) AttachTo(chain *DeleteChain) {
	node.chain = chain
	node.GenericDLNode = chain.Insert(node)
}

func (node *DeleteNode) GetChain() txnif.DeleteChain          { return node.chain }
func (node *DeleteNode) GetDeleteMaskLocked() *roaring.Bitmap { return node.mask }

func (node *DeleteNode) HasOverlapLocked(start, end uint32) bool {
	if node.mask == nil || node.mask.IsEmpty() {
		return false
	}
	var yes bool
	if start == end {
		yes = node.mask.Contains(start)
	} else {
		x2 := roaring.New()
		x2.AddRange(uint64(start), uint64(end+1))
		yes = node.mask.Intersects(x2)
	}
	return yes
}

func (node *DeleteNode) MergeLocked(o *DeleteNode, collectIndex bool) {
	if node.mask == nil {
		node.mask = roaring.New()
	}
	node.mask.Or(o.mask)
	if collectIndex {
		if o.GetLogIndex() != nil {
			node.AddLogIndexLocked(o.GetLogIndex())
		}
		if o.logIndexes != nil {
			node.AddLogIndexesLocked(o.logIndexes)
		}
	}
}
func (node *DeleteNode) GetCommitTSLocked() types.TS { return node.TxnMVCCNode.GetEnd() }
func (node *DeleteNode) GetStartTS() types.TS        { return node.TxnMVCCNode.GetStart() }

func (node *DeleteNode) IsDeletedLocked(row uint32) bool {
	return node.mask.Contains(row)
}

func (node *DeleteNode) RangeDeleteLocked(start, end uint32) {
	// logutil.Debugf("RangeDelete BLK-%d Start=%d End=%d",
	// 	node.chain.mvcc.meta.ID,
	// 	start,
	// 	end)
	node.mask.AddRange(uint64(start), uint64(end+1))
	for i := start; i < end+1; i++ {
		node.chain.InsertInDeleteView(i, node)
	}
}
func (node *DeleteNode) DeletedRows() (rows []uint32) {
	if node.mask == nil {
		return
	}
	rows = node.mask.ToArray()
	return
}
func (node *DeleteNode) GetCardinalityLocked() uint32 { return uint32(node.mask.GetCardinality()) }

func (node *DeleteNode) PrepareCommit() (err error) {
	node.chain.mvcc.Lock()
	defer node.chain.mvcc.Unlock()
	_, err = node.TxnMVCCNode.PrepareCommit()
	if err != nil {
		return
	}
	node.chain.UpdateLocked(node)
	return
}

func (node *DeleteNode) ApplyCommit(index *wal.Index) (err error) {
	node.chain.mvcc.Lock()
	defer node.chain.mvcc.Unlock()
	_, err = node.TxnMVCCNode.ApplyCommit(index)
	if err != nil {
		return
	}
	node.chain.AddDeleteCnt(uint32(node.mask.GetCardinality()))
	node.chain.mvcc.IncChangeNodeCnt()
	return node.OnApply()
}

func (node *DeleteNode) ApplyRollback(index *wal.Index) (err error) {
	node.chain.mvcc.Lock()
	defer node.chain.mvcc.Unlock()
	_, err = node.TxnMVCCNode.ApplyRollback(index)
	return
}

func (node *DeleteNode) GeneralString() string {
	return fmt.Sprintf("%s;Cnt=%d", node.TxnMVCCNode.String(), node.mask.GetCardinality())
}

func (node *DeleteNode) GeneralDesc() string {
	return fmt.Sprintf("%s;Cnt=%d", node.TxnMVCCNode.String(), node.mask.GetCardinality())
}

func (node *DeleteNode) GeneralVerboseString() string {
	return fmt.Sprintf("%s;Cnt=%d;Deletes=%v", node.TxnMVCCNode.String(), node.mask.GetCardinality(), node.mask)
}

func (node *DeleteNode) StringLocked() string {
	ntype := "TXN"
	if node.nt == NT_Merge {
		ntype = "MERGE"
	}
	commitState := "C"
	if node.GetEnd() == txnif.UncommitTS {
		commitState = "UC"
	}
	s := fmt.Sprintf("[%s:%s][%d:%s]%s", ntype, commitState, node.mask.GetCardinality(), node.mask.String(), node.TxnMVCCNode.String())
	return s
}

func (node *DeleteNode) WriteTo(w io.Writer) (n int64, err error) {
	cn, err := w.Write(common.EncodeID(node.chain.mvcc.GetID()))
	if err != nil {
		return
	}
	n += int64(cn)
	buf, err := node.mask.ToBytes()
	if err != nil {
		return
	}
	var sn int64
	if sn, err = common.WriteBytes(buf, w); err != nil {
		return
	}
	n += int64(sn)
	var sn2 int64
	if sn2, err = node.TxnMVCCNode.WriteTo(w); err != nil {
		return
	}
	n += sn2
	return
}

func (node *DeleteNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	if node.id == nil {
		node.id = &common.ID{}
	}
	if sn, err = r.Read(common.EncodeID(node.id)); err != nil {
		return
	}
	n += int64(sn)
	var sn2 int64
	var buf []byte
	if buf, sn2, err = common.ReadBytes(r); err != nil {
		return
	}
	n += sn2
	node.mask = roaring.New()
	err = node.mask.UnmarshalBinary(buf)
	if err != nil {
		return
	}
	if sn2, err = node.TxnMVCCNode.ReadFrom(r); err != nil {
		return
	}
	n += sn2
	return
}
func (node *DeleteNode) SetLogIndex(idx *wal.Index) {
	node.TxnMVCCNode.SetLogIndex(idx)
}
func (node *DeleteNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewDeleteCmd(id, node)
	return
}
func (node *DeleteNode) GetPrefix() []byte {
	return node.chain.mvcc.meta.MakeKey()
}
func (node *DeleteNode) Set1PC()     { node.TxnMVCCNode.Set1PC() }
func (node *DeleteNode) Is1PC() bool { return node.TxnMVCCNode.Is1PC() }
func (node *DeleteNode) PrepareRollback() (err error) {
	node.chain.mvcc.Lock()
	defer node.chain.mvcc.Unlock()
	node.chain.RemoveNodeLocked(node)
	node.chain.DeleteInDeleteView(node)
	node.TxnMVCCNode.PrepareRollback()
	return
}

func (node *DeleteNode) OnApply() (err error) {
	if node.dt == handle.DT_Normal {
		listener := node.chain.mvcc.GetDeletesListener()
		if listener == nil {
			return
		}
		err = listener(node.mask.GetCardinality(), node.mask.Iterator(), node.GetCommitTSLocked())
	}
	return
}

func (node *DeleteNode) GetRowMaskRefLocked() *roaring.Bitmap {
	return node.mask
}
