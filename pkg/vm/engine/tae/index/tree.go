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

package index

import (
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	art "github.com/plar/go-adaptive-radix-tree"
)

var _ SecondaryIndex = new(simpleARTMap)

type IndexMVCCNode struct {
	Row             uint32
	Deleted         bool
	DeletedByUpsert bool
	blkdata         data.Block
}

func NewAppendIndexMVCCNode(row uint32, blk data.Block) (idxNode *IndexMVCCNode) {
	idxNode = &IndexMVCCNode{
		Row:     row,
		blkdata: blk,
	}
	return
}

func NewDeleteIndexMVCCNode(row uint32, blk data.Block) (idxNode *IndexMVCCNode) {
	idxNode = &IndexMVCCNode{
		Row:     row,
		Deleted: true,
		blkdata: blk,
	}
	return
}
func NewDeleteIndexMVCCNodeInUpsert(row uint32, blk data.Block) (idxNode *IndexMVCCNode) {
	idxNode = &IndexMVCCNode{
		Row:             row,
		Deleted:         true,
		DeletedByUpsert: true,
		blkdata:         blk,
	}
	return
}
func (node *IndexMVCCNode) getTxnNode() txnif.MVCCNode {
	if node.Deleted && !node.DeletedByUpsert {
		return node.blkdata.GetDeleteNodeByRow(node.Row)
	} else {
		return node.blkdata.GetAppendNodeByRow(node.Row)
	}
}
func (node *IndexMVCCNode) String() string {
	return fmt.Sprintf("Row=%d,Delete=%v", node.Row, node.Deleted)
}
func (node *IndexMVCCNode) ApplyCommit(*wal.Index) error {
	panic("not supported")
}

func (node *IndexMVCCNode) PrepareCommit() error {
	panic("not supported")
}

func (node *IndexMVCCNode) CloneAll() txnif.MVCCNode {
	panic("not supported")
}

func (node *IndexMVCCNode) CloneData() txnif.MVCCNode {
	panic("not supported")
}

func (node *IndexMVCCNode) Update(txnif.MVCCNode)                      { panic("not supported") }
func (node *IndexMVCCNode) ApplyRollback(index *wal.Index) (err error) { panic("not supported") }
func (node *IndexMVCCNode) CheckConflict(ts types.TS) error            { panic("not supported") }
func (node *IndexMVCCNode) WriteTo(w io.Writer) (n int64, err error)   { panic("not supported") }
func (node *IndexMVCCNode) ReadFrom(r io.Reader) (n int64, err error)  { panic("not supported") }
func (node *IndexMVCCNode) GetEnd() types.TS {
	if node.blkdata == nil {
		return types.TS{}
	}
	return node.getTxnNode().GetEnd()
}
func (node *IndexMVCCNode) GetStart() types.TS {
	if node.blkdata == nil {
		return types.TS{}
	}
	return node.getTxnNode().GetStart()
}
func (node *IndexMVCCNode) GetPrepare() types.TS {
	if node.blkdata == nil {
		return types.TS{}
	}
	return node.getTxnNode().GetPrepare()
}
func (node *IndexMVCCNode) GetTxn() txnif.TxnReader    { panic("not supported") }
func (node *IndexMVCCNode) SetLogIndex(idx *wal.Index) { panic("not supported") }
func (node *IndexMVCCNode) GetLogIndex() *wal.Index    { panic("not supported") }
func (node *IndexMVCCNode) IsVisible(ts types.TS) (visible bool) {
	if node.blkdata == nil {
		return true
	}
	return node.getTxnNode().IsVisible(ts)
}
func (node *IndexMVCCNode) PreparedIn(minTS, maxTS types.TS) (in, before bool) {
	if node.blkdata == nil {
		return true, false
	}
	return node.getTxnNode().PreparedIn(minTS, maxTS)
}
func (node *IndexMVCCNode) CommittedIn(minTS, maxTS types.TS) (in, before bool) {
	if node.blkdata == nil {
		return true, false
	}
	return node.getTxnNode().CommittedIn(minTS, maxTS)
}
func (node *IndexMVCCNode) NeedWaitCommitting(ts types.TS) (bool, txnif.TxnReader) {
	if node.blkdata == nil {
		return false, nil
	}
	return node.getTxnNode().NeedWaitCommitting(ts)
}
func (node *IndexMVCCNode) IsSameTxn(ts types.TS) bool {
	if node.blkdata == nil {
		return false
	}
	return node.getTxnNode().IsSameTxn(ts)
}
func (node *IndexMVCCNode) IsActive() bool {
	if node.blkdata == nil {
		return false
	}
	return node.getTxnNode().IsActive()
}
func (node *IndexMVCCNode) IsCommitting() bool {
	if node.blkdata == nil {
		return false
	}
	return node.getTxnNode().IsCommitted()
}
func (node *IndexMVCCNode) IsCommitted() bool {
	if node.blkdata == nil {
		return true
	}
	return node.getTxnNode().IsCommitting()
}
func (node *IndexMVCCNode) IsAborted() bool {
	if node.blkdata == nil {
		return false
	}
	return node.getTxnNode().IsAborted()
}
func (node *IndexMVCCNode) Set1PC()     { panic("not supported") }
func (node *IndexMVCCNode) Is1PC() bool { panic("not supported") }
func CompareIndexMVCCNode(va, vb txnif.MVCCNode) int {
	if va.GetPrepare().Less(vb.GetPrepare()) {
		return -1
	}
	if va.GetPrepare().Greater(vb.GetPrepare()) {
		return 1
	}
	return 0
}

type IndexMVCCChain struct {
	MVCC *txnbase.MVCCSlice
}

func NewIndexMVCCChain() *IndexMVCCChain {
	return &IndexMVCCChain{
		MVCC: txnbase.NewMVCCSlice(nil, CompareIndexMVCCNode),
	}
}

// row -> appendnode/deletenode -> ts,
func (chain *IndexMVCCChain) Existed() bool {
	un := chain.MVCC.GetLastNonAbortedNode().(*IndexMVCCNode)
	if un == nil {
		return false
	}
	return !un.Deleted
}

func (chain *IndexMVCCChain) Visible(ts types.TS) bool {
	un := chain.MVCC.GetVisibleNode(ts)
	if un == nil {
		return false
	}
	return !un.(*IndexMVCCNode).Deleted
}

func (chain *IndexMVCCChain) GetRow() uint32 {
	un := chain.MVCC.GetLastNonAbortedNode().(*IndexMVCCNode)
	return un.Row
}

func (chain *IndexMVCCChain) Merge(o *IndexMVCCChain) {
	o.MVCC.ForEach(func(un txnif.MVCCNode) bool {
		chain.MVCC.InsertNode(un)
		return true
	})
}
func (chain *IndexMVCCChain) Insert(n *IndexMVCCNode) {
	chain.MVCC.InsertNode(n)
}

type simpleARTMap struct {
	blkdata data.Block
	typ     types.Type
	tree    art.Tree
}

func NewSimpleARTMap(typ types.Type, blk data.Block) *simpleARTMap {
	return &simpleARTMap{
		blkdata: blk,
		typ:     typ,
		tree:    art.New(),
	}
}

func (art *simpleARTMap) Size() int { return art.tree.Size() }

func (art *simpleARTMap) Insert(key any, offset uint32, txn txnif.TxnReader) (txnnode *txnbase.TxnMVCCNode, err error) {
	appendnode := NewAppendIndexMVCCNode(offset, art.blkdata)
	chain := NewIndexMVCCChain()
	chain.Insert(appendnode)
	ikey := types.EncodeValue(key, art.typ)
	old, _ := art.tree.Insert(ikey, chain)
	if old != nil {
		oldChain := old.(*IndexMVCCChain)
		if oldChain.Existed() {
			art.tree.Insert(ikey, old)
			err = ErrDuplicate
		} else {
			oldChain.Merge(chain)
			art.tree.Insert(ikey, old)
		}
	}
	return
}

func (art *simpleARTMap) BatchInsert(keys *KeysCtx, startRow uint32, upsert bool, txn txnif.TxnReader) (txnNode *txnbase.TxnMVCCNode, err error) {
	existence := make(map[any]bool)
	txnNode = txnbase.NewTxnMVCCNodeWithTxn(txn)

	op := func(v any, i int) error {
		var appendnode *IndexMVCCNode
		encoded := types.EncodeValue(v, art.typ)
		appendnode = NewAppendIndexMVCCNode(startRow, art.blkdata)
		chain := NewIndexMVCCChain()
		chain.Insert(appendnode)
		if keys.NeedVerify {
			if _, found := existence[string(encoded)]; found {
				return ErrDuplicate
			}
			existence[string(encoded)] = true
		}
		old, _ := art.tree.Insert(encoded, chain)
		if old != nil {
			oldChain := old.(*IndexMVCCChain)
			if !upsert {
				txnNode.Aborted = true
				return ErrDuplicate
			}
			deleteNode := NewDeleteIndexMVCCNodeInUpsert(oldChain.GetRow(), art.blkdata)
			oldChain.Insert(deleteNode)
			oldChain.Merge(chain)
			art.tree.Insert(encoded, old)
		}
		startRow++
		return nil
	}

	err = keys.Keys.ForeachWindow(keys.Start, keys.Count, op, nil)
	return
}

func (art *simpleARTMap) IsKeyDeleted(key any, ts types.TS) (deleted bool, existed bool) {
	encoded := types.EncodeValue(key, art.typ)
	v, existed := art.tree.Search(encoded)
	if !existed {
		return false, false
	}
	existed = false
	chain := v.(*IndexMVCCChain)
	chain.MVCC.ForEach(func(un txnif.MVCCNode) bool {
		node := un.(*IndexMVCCNode)
		if node.Deleted && !node.IsAborted() {
			existed = true
			if node.GetEnd().LessEq(ts) {
				deleted = true
			}
		}
		return true
	})
	return
}

func (art *simpleARTMap) HasDeleteFrom(key any, fromTs types.TS) bool {
	encoded := types.EncodeValue(key, art.typ)
	v, existed := art.tree.Search(encoded)
	if !existed {
		return false
	}
	chain := v.(*IndexMVCCChain)
	var deleted bool
	chain.MVCC.ForEach(func(un txnif.MVCCNode) bool {
		node := un.(*IndexMVCCNode)
		if node.GetEnd().Greater(fromTs) && node.Deleted && !node.IsAborted() {
			deleted = true
		}
		return true
	})
	return deleted
}

func (art *simpleARTMap) Delete(key any, ts types.TS) (old uint32, txnNode *txnbase.TxnMVCCNode, err error) {
	ikey := types.EncodeValue(key, art.typ)
	v, found := art.tree.Search(ikey)
	if !found {
		err = ErrNotFound
	} else {
		oldChain := v.(*IndexMVCCChain)
		old = oldChain.GetRow()
		deleteNode := NewDeleteIndexMVCCNode(old, art.blkdata)
		oldChain.Insert(deleteNode)
	}
	return
}
func (art *simpleARTMap) Search(key any) (uint32, error) {
	ikey := types.EncodeValue(key, art.typ)
	v, found := art.tree.Search(ikey)
	if !found {
		return 0, ErrNotFound
	}
	chain := v.(*IndexMVCCChain)
	if !chain.Existed() {
		return 0, ErrNotFound
	}
	return chain.GetRow(), nil
}

func (art *simpleARTMap) Contains(key any) bool {
	ikey := types.EncodeValue(key, art.typ)
	v, exists := art.tree.Search(ikey)
	if !exists {
		return false
	}
	chain := v.(*IndexMVCCChain)
	return chain.Existed()
}

// ContainsAny returns whether at least one of the specified keys exists.
// If the keysCtx.Selects is not nil, only the keys indicated by the keyselects bitmap will
// participate in the calculation.
// When deduplication occurs, the corresponding row number will be taken out. If the row
// number is included in the rowmask, the error will be ignored
func (art *simpleARTMap) ContainsAny(keysCtx *KeysCtx, rowmask *roaring.Bitmap) bool {
	op := func(v any, _ int) error {
		row, err := art.Search(v)
		// 1. If duplication found
		if err == nil {
			// 1.1 If no rowmask, quick return with duplication error
			if rowmask == nil {
				return ErrDuplicate
			}
			// 1.2 If duplicated row is marked, ignore this duplication error
			if rowmask.Contains(row) {
				return nil
			}
			// 1.3 If duplicated row is not marked, return with duplication error
			return ErrDuplicate

		}
		return nil
	}
	if err := keysCtx.Keys.ForeachWindow(int(keysCtx.Start), int(keysCtx.Count), op, keysCtx.Selects); err != nil {
		if err == ErrDuplicate {
			return true
		} else {
			panic(err)
		}
	}
	return false
}

func (art *simpleARTMap) String() string {
	s := fmt.Sprintf("<ART>[Size=%d](\n", art.tree.Size())
	it := art.tree.Iterator()
	for it.HasNext() {
		n, err := it.Next()
		if err != nil {
			break
		}
		s = fmt.Sprintf("%sNode: %v:%v\n", s, n.Key(), n.Value().(*IndexMVCCChain).MVCC.StringLocked())
	}
	s = fmt.Sprintf("%s)", s)
	return s
}
