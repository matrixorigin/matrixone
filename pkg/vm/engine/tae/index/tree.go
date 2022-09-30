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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	art "github.com/plar/go-adaptive-radix-tree"
)

var _ SecondaryIndex = new(simpleARTMap)

type IndexMVCCNode struct {
	Row     uint32
	Deleted bool
	*txnbase.TxnMVCCNode
}

func NewAppendIndexMVCCNode(row uint32, txn txnif.TxnReader) (idxNode *IndexMVCCNode, txnNode *txnbase.TxnMVCCNode) {
	txnNode = txnbase.NewTxnMVCCNodeWithTxn(txn)
	idxNode = &IndexMVCCNode{
		Row:         row,
		TxnMVCCNode: txnNode,
	}
	return
}

func NewAppendIndexMVCCNodeWithTxnMVCCNode(row uint32, txnNode *txnbase.TxnMVCCNode) (idxNode *IndexMVCCNode) {
	idxNode = &IndexMVCCNode{
		Row:         row,
		TxnMVCCNode: txnNode,
	}
	return
}
func NewDeleteIndexMVCCNode(row uint32, ts types.TS) (idxNode *IndexMVCCNode, txnNode *txnbase.TxnMVCCNode) {
	txnNode = txnbase.NewTxnMVCCNodeWithTS(ts)
	idxNode = &IndexMVCCNode{
		Row:         row,
		Deleted:     true,
		TxnMVCCNode: txnNode,
	}
	return
}
func NewDeleteIndexMVCCNodeWithTxnNode(row uint32, txnNode *txnbase.TxnMVCCNode) (idxNode *IndexMVCCNode) {
	idxNode = &IndexMVCCNode{
		Row:         row,
		Deleted:     true,
		TxnMVCCNode: txnNode,
	}
	return
}
func (node *IndexMVCCNode) String() string {
	return fmt.Sprintf("End=%s,Aborted=%v,Row=%d,Delete=%v", node.GetEnd().ToString(), node.Aborted, node.Row, node.Deleted)
}
func (node *IndexMVCCNode) ApplyCommit(*wal.Index) error {
	node.TxnMVCCNode.ApplyCommit(nil)
	return nil
}

func (node *IndexMVCCNode) PrepareCommit() error {
	node.TxnMVCCNode.PrepareCommit()
	return nil
}

func (node *IndexMVCCNode) CloneAll() txnbase.MVCCNode {
	panic("not supported")
}

func (node *IndexMVCCNode) CloneData() txnbase.MVCCNode {
	panic("not supported")
}

func (node *IndexMVCCNode) Update(txnbase.MVCCNode) {
	panic("not supported")
}

func CompareIndexMVCCNode(va, vb txnbase.MVCCNode) int {
	a := va.(*IndexMVCCNode)
	b := vb.(*IndexMVCCNode)
	return a.Compare(b.TxnMVCCNode)
}

type IndexMVCCChain struct {
	MVCC *txnbase.MVCCSlice
}

func NewIndexMVCCChain() *IndexMVCCChain {
	return &IndexMVCCChain{
		MVCC: txnbase.NewMVCCSlice(nil, CompareIndexMVCCNode),
	}
}

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
	o.MVCC.ForEach(func(un txnbase.MVCCNode) {
		chain.MVCC.InsertNode(un)
	})
}
func (chain *IndexMVCCChain) Insert(n *IndexMVCCNode) {
	chain.MVCC.InsertNode(n)
}

type simpleARTMap struct {
	typ  types.Type
	tree art.Tree
}

func NewSimpleARTMap(typ types.Type) *simpleARTMap {
	return &simpleARTMap{
		typ:  typ,
		tree: art.New(),
	}
}

func (art *simpleARTMap) Size() int { return art.tree.Size() }

func (art *simpleARTMap) Insert(key any, offset uint32, txn txnif.TxnReader) (txnnode *txnbase.TxnMVCCNode, err error) {
	var appendnode *IndexMVCCNode
	appendnode, txnnode = NewAppendIndexMVCCNode(offset, txn)
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
		appendnode = NewAppendIndexMVCCNodeWithTxnMVCCNode(startRow, txnNode)
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
			deleteNode := NewDeleteIndexMVCCNodeWithTxnNode(oldChain.GetRow(), txnNode)
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
	chain.MVCC.ForEach(func(un txnbase.MVCCNode) {
		node := un.(*IndexMVCCNode)
		if node.Deleted && !node.Aborted {
			existed = true
			if node.GetEnd().LessEq(ts) {
				deleted = true
			}
		}
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
	chain.MVCC.ForEach(func(un txnbase.MVCCNode) {
		node := un.(*IndexMVCCNode)
		if node.GetEnd().Greater(fromTs) && node.Deleted && !node.Aborted {
			deleted = true
		}
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
		var deleteNode *IndexMVCCNode
		deleteNode, txnNode = NewDeleteIndexMVCCNode(old, ts)
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
