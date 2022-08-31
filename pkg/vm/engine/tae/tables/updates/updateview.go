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

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type ColumnView struct {
	links map[uint32]*common.GenericSortedDList[*ColumnUpdateNode]
	mask  *roaring.Bitmap
}

func NewColumnView() *ColumnView {
	// func NewColumnView(chain *ColumnChain) *ColumnView {
	return &ColumnView{
		links: make(map[uint32]*common.GenericSortedDList[*ColumnUpdateNode]),
		mask:  roaring.New(),
	}
}

func (view *ColumnView) CollectUpdates(ts types.TS) (mask *roaring.Bitmap, vals map[uint32]any, err error) {
	if len(view.links) == 0 {
		return
	}
	mask = roaring.New()
	vals = make(map[uint32]any)
	it := view.mask.Iterator()
	var v any
	for it.HasNext() {
		row := it.Next()
		v, err = view.GetValue(row, ts)
		if err == nil {
			vals[row] = v
			mask.Add(row)
		} else if err == txnif.ErrTxnInternal {
			break
		}
		err = nil
	}
	return
}

func (view *ColumnView) GetValue(key uint32, startTs types.TS) (v any, err error) {
	link := view.links[key]
	if link == nil {
		err = data.ErrNotFound
		return
	}
	head := link.GetHead()
	for head != nil {
		node := head.GetPayload()
		if node.GetStartTS().Less(startTs) {
			node.RLock()
			//        |
			// start \|/ commit
			// --+----+----+-------------->
			//   |_________|  next
			// --|   NODE  |------->
			//   +---------+
			// 1. Read ts is between node start and commit. Go to prev node
			if node.GetCommitTSLocked().Greater(startTs) {
				node.RUnlock()
				head = head.GetNext()
				continue
			} else {
				v, err = node.GetValueLocked(key)
				nTxn := node.txn
				node.RUnlock()
				// 2. Node was committed and can be used as the data node
				if nTxn == nil {
					break
				}
				// 3. Node is committing and wait committed or rollbacked
				state := nTxn.GetTxnState(true)
				// logutil.Infof("%d -- wait --> %s: state", startTs, nTxn.Repr(), state)
				if state == txnif.TxnStateCommitted {
					// 3.1 If committed. use this node
					break
				} else if state == txnif.TxnStateRollbacked || state == txnif.TxnStateRollbacking {
					// 3.2 If rollbacked. go to prev node
					err = nil
					v = nil
					head = head.GetNext()
					continue
				} else if state == txnif.TxnStateCommitting {
					logutil.Fatal("txn state error")
				} else if state == txnif.TxnStateUnknown {
					err = txnif.ErrTxnInternal
					v = nil
					break
				}
			}
		}
		if node.GetStartTS().Greater(startTs) {
			head = head.GetNext()
			continue
		}

		node.RLock()
		v, err = node.GetValueLocked(key)
		node.RUnlock()
		break
	}
	if v == nil && err == nil {
		err = data.ErrNotFound
	}
	return
}

func (view *ColumnView) PrepapreInsert(key uint32, ts types.TS) (err error) {
	// First update to key
	var link *common.GenericSortedDList[*ColumnUpdateNode]
	if link = view.links[key]; link == nil {
		return
	}

	node := link.GetHead().GetPayload()
	node.RLock()
	// 1. The specified row has committed update
	if node.txn == nil {
		// 1.1 The update was committed after txn start. w-w conflict
		if node.GetCommitTSLocked().Greater(ts) {
			err = txnif.ErrTxnWWConflict
			node.RUnlock()
			return
		}
		node.RUnlock()
		// 1.2 The update was committed before txn start. use it
		return
	}
	// 2. The specified row was updated by the same txn
	if node.txn.GetStartTS() == ts {
		node.RUnlock()
		return
	}
	// 3. The specified row has other uncommitted change
	// Note: Here we have some overkill to proactivelly w-w with committing txn
	node.RUnlock()
	err = txnif.ErrTxnWWConflict
	return
}

func (view *ColumnView) Insert(key uint32, un txnif.UpdateNode) (err error) {
	n := un.(*ColumnUpdateNode)
	// First update to key
	var link *common.GenericSortedDList[*ColumnUpdateNode]
	if link = view.links[key]; link == nil {
		link = common.NewGenericSortedDList[*ColumnUpdateNode](compareUpdateNode)
		link.Insert(n)
		view.mask.Add(key)
		view.links[key] = link
		return
	}

	node := link.GetHead().GetPayload()
	node.RLock()
	// 1. The specified row has committed update
	if node.txn == nil {
		// 1.1 The update was committed after txn start. w-w conflict
		if node.GetCommitTSLocked().Greater(n.GetStartTS()) {
			err = txnif.ErrTxnWWConflict
			node.RUnlock()
			return
		}
		node.RUnlock()
		// 1.2 The update was committed before txn start. use it
		link.Insert(n)
		view.mask.Add(key)
		return
	}
	// 2. The specified row was updated by the same txn
	if node.txn.GetStartTS() == n.GetStartTS() {
		node.RUnlock()
		return
	}
	// 3. The specified row has other uncommitted change
	// Note: Here we have some overkill to proactivelly w-w with committing txn
	node.RUnlock()
	err = txnif.ErrTxnWWConflict
	return
}

func (view *ColumnView) Delete(key uint32, n *ColumnUpdateNode) (err error) {
	link := view.links[key]
	var target *common.GenericDLNode[*ColumnUpdateNode]
	link.Loop(func(dlnode *common.GenericDLNode[*ColumnUpdateNode]) bool {
		node := dlnode.GetPayload()
		if node.GetStartTS() == n.GetStartTS() {
			target = dlnode
			return false
		}
		return true
	}, false)
	if target == nil {
		panic("logic error")
	}
	link.Delete(target)
	if link.GetHead() == nil {
		delete(view.links, key)
	}
	return
}

func (view *ColumnView) RowStringLocked(row uint32,
	link *common.GenericSortedDList[*ColumnUpdateNode]) string {
	s := fmt.Sprintf("[ROW=%d]:", row)
	link.Loop(func(dlnode *common.GenericDLNode[*ColumnUpdateNode]) bool {
		n := dlnode.GetPayload()
		n.RLock()
		s = fmt.Sprintf("%s\n%s", s, n.StringLocked())
		n.RUnlock()
		return true
	}, false)
	return s
}

func (view *ColumnView) StringLocked() string {
	mask := roaring.New()
	for k := range view.links {
		mask.Add(k)
	}
	s := "[VIEW]"
	it := mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		s = fmt.Sprintf("%s\n%s", s, view.RowStringLocked(row, view.links[row]))
	}
	return s
}

func (view *ColumnView) RowCnt() int {
	return len(view.links)
}
