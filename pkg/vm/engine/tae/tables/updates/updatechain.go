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
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type ColumnChain struct {
	*common.Link
	*sync.RWMutex
	id         *common.ID
	view       *ColumnView
	controller *MVCCHandle
	cnt        uint32
}

func MockColumnUpdateChain() *ColumnChain {
	chain := &ColumnChain{
		Link:    new(common.Link),
		RWMutex: new(sync.RWMutex),
		id:      &common.ID{},
	}
	chain.view = NewColumnView()
	return chain
}

func NewColumnChain(rwlocker *sync.RWMutex, colIdx uint16, controller *MVCCHandle) *ColumnChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	id := *controller.GetID()
	id.Idx = colIdx
	chain := &ColumnChain{
		Link:       new(common.Link),
		RWMutex:    rwlocker,
		controller: controller,
		id:         &id,
	}
	chain.view = NewColumnView()
	return chain
}

func (chain *ColumnChain) LoadUpdateCnt() uint32 {
	return atomic.LoadUint32(&chain.cnt)
}

func (chain *ColumnChain) SetUpdateCnt(cnt uint32) {
	atomic.StoreUint32(&chain.cnt, cnt)
}

func (chain *ColumnChain) GetMeta() *catalog.BlockEntry { return chain.controller.meta }
func (chain *ColumnChain) GetBlockID() *common.ID       { id := chain.id.AsBlockID(); return &id }
func (chain *ColumnChain) GetID() *common.ID            { return chain.id }
func (chain *ColumnChain) GetColumnIdx() uint16         { return chain.id.Idx }
func (chain *ColumnChain) GetController() *MVCCHandle   { return chain.controller }

func (chain *ColumnChain) GetColumnName() string {
	return chain.controller.meta.GetSchema().ColDefs[chain.id.Idx].Name
}

func (chain *ColumnChain) TryUpdateNodeLocked(row uint32, v interface{}, n txnif.UpdateNode) (err error) {
	if err = chain.PrepareUpdate(row, n); err != nil {
		return
	}
	n.UpdateLocked(row, v)
	return
}

func (chain *ColumnChain) AddNodeLocked(txn txnif.AsyncTxn) txnif.UpdateNode {
	node := NewColumnNode(txn, chain.id, nil)
	node.AttachTo(chain)
	return node
}

func (chain *ColumnChain) DeleteNode(node *common.DLNode) {
	chain.Lock()
	defer chain.Unlock()
	chain.DeleteNodeLocked(node)
}

func (chain *ColumnChain) DeleteNodeLocked(node *common.DLNode) {
	n := node.GetPayload().(*ColumnNode)
	for row := range n.txnVals {
		chain.view.Delete(row, n)
	}
	chain.Delete(node)
	chain.SetUpdateCnt(uint32(chain.view.mask.GetCardinality()))
}

func (chain *ColumnChain) AddNode(txn txnif.AsyncTxn) txnif.UpdateNode {
	col := NewColumnNode(txn, chain.id, nil)
	chain.Lock()
	defer chain.Unlock()
	col.AttachTo(chain)
	return col
}

func (chain *ColumnChain) LoopChainLocked(fn func(col *ColumnNode) bool, reverse bool) {
	wrapped := func(node *common.DLNode) bool {
		col := node.GetPayload().(*ColumnNode)
		return fn(col)
	}
	chain.Loop(wrapped, reverse)
}

func (chain *ColumnChain) DepthLocked() int {
	depth := 0
	chain.LoopChainLocked(func(n *ColumnNode) bool {
		depth++
		return true
	}, false)
	return depth
}

func (chain *ColumnChain) PrepareUpdate(row uint32, n txnif.UpdateNode) error {
	err := chain.view.Insert(row, n)
	if err == nil {
		chain.SetUpdateCnt(uint32(chain.view.mask.GetCardinality()))
	}
	return err
}

func (chain *ColumnChain) UpdateLocked(node *ColumnNode) {
	chain.Update(node.DLNode)
}

func (chain *ColumnChain) StringLocked() string {
	return chain.view.StringLocked()
	// msg := fmt.Sprintf("Block-%s-Col[%d]-Chain:", chain.id.ToBlockFileName(), chain.id.Idx)
	// line := 1
	// chain.LoopChainLocked(func(n *ColumnNode) bool {
	// 	n.RLock()
	// 	msg = fmt.Sprintf("%s\n%d. %s", msg, line, n.StringLocked())
	// 	n.RUnlock()
	// 	line++
	// 	return true
	// }, false)
	// return msg
}

func (chain *ColumnChain) GetValueLocked(row uint32, ts uint64) (v interface{}, err error) {
	return chain.view.GetValue(row, ts)
}

func (chain *ColumnChain) CollectUpdatesLocked(ts uint64) (*roaring.Bitmap, map[uint32]interface{}) {
	return chain.view.CollectUpdates(ts)
}

func (chain *ColumnChain) CollectCommittedInRangeLocked(startTs, endTs uint64) (mask *roaring.Bitmap, vals map[uint32]interface{}, indexes []*wal.Index) {
	var merged *ColumnNode
	chain.LoopChainLocked(func(n *ColumnNode) bool {
		n.RLock()
		// 1. Committed in [endTs, +inf)
		if n.GetCommitTSLocked() >= endTs {
			n.RUnlock()
			return true
		}
		// 2. Committed in (-inf, startTs). Skip it and stop looping
		if n.GetCommitTSLocked() < startTs {
			n.RUnlock()
			return false
		}
		// 3. Committed in [startTs, endTs)
		if n.txn != nil {
			// 3.1. Committing. Wait committed or rollbacked
			txn := n.txn
			n.RUnlock()
			state := txn.GetTxnState(true)
			// 3.1.1. Rollbacked. Skip it and go to next
			if state == txnif.TxnStateRollbacked {
				return true
			}
			// 3.1.2. Committed
			n.RLock()
		}
		if merged == nil {
			merged = NewSimpleColumnNode()
		}
		indexes = append(indexes, n.logIndex)
		merged.MergeLocked(n)
		n.RUnlock()
		return true
	}, false)

	if merged != nil {
		mask = merged.txnMask
		vals = merged.txnVals
	}
	return
}
