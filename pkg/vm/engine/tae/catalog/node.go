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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type nodeList[T any] struct {
	common.SSLLNode
	getter       func(uint64) *common.GenericDLNode[T]
	visibilityFn func(*common.GenericDLNode[T], types.TS) (bool, bool)
	rwlocker     *sync.RWMutex
	name         string
}

func newNodeList[T any](getter func(uint64) *common.GenericDLNode[T],
	visibilityFn func(*common.GenericDLNode[T], types.TS) (bool, bool),
	rwlocker *sync.RWMutex,
	name string) *nodeList[T] {
	return &nodeList[T]{
		SSLLNode:     *common.NewSSLLNode(),
		getter:       getter,
		visibilityFn: visibilityFn,
		rwlocker:     rwlocker,
		name:         name,
	}
}

func (n *nodeList[T]) CreateNode(id uint64) *nameNode[T] {
	nn := newNameNode(id, n.getter)
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	n.Insert(nn)
	return nn
}

func (n *nodeList[T]) DeleteNode(id uint64) (deleted *nameNode[T], empty bool) {
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	var prev common.ISSLLNode
	prev = n
	curr := n.GetNext()
	depth := 0
	for curr != nil {
		nid := curr.(*nameNode[T]).id
		if id == nid {
			prev.ReleaseNextNode()
			deleted = curr.(*nameNode[T])
			next := curr.GetNext()
			if next == nil && depth == 0 {
				empty = true
			}
			break
		}
		prev = curr
		curr = curr.GetNext()
		depth++
	}
	return
}

func (n *nodeList[T]) ForEachNodes(fn func(*nameNode[T]) bool) {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	n.ForEachNodesLocked(fn)
}

func (n *nodeList[T]) ForEachNodesLocked(fn func(*nameNode[T]) bool) {
	curr := n.GetNext()
	for curr != nil {
		nn := curr.(*nameNode[T])
		if ok := fn(nn); !ok {
			break
		}
		curr = curr.GetNext()
	}
}

func (n *nodeList[T]) LengthLocked() int {
	length := 0
	fn := func(*nameNode[T]) bool {
		length++
		return true
	}
	n.ForEachNodesLocked(fn)
	return length
}

func (n *nodeList[T]) Length() int {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.LengthLocked()
}

func (n *nodeList[T]) GetNode() *common.GenericDLNode[T] {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.GetNext().(*nameNode[T]).GetNode()
}

//	Create                  Deleted
//	  |                        |
//
// --+------+-+------------+--+------+-+--+-------+--+----->
//
//	|      | |            |  |      | |  |       |  |
//	|      +-|------Txn2--|--+      | |  +--Txn5-|--+
//	+--Txn1--+            +----Txn3-|-+          |
//	                                +----Txn4----+
//
// 1. Txn1 start and create a table "tb1"
// 2. Txn2 start and cannot find "tb1".
// 3. Txn1 commit
// 4. Txn3 start and drop table "tb1"
// 6. Txn4 start and can find "tb1"
// 7. Txn3 commit
// 8. Txn4 can still find "tb1"
// 9. Txn5 start and cannot find "tb1"
func (n *nodeList[T]) TxnGetNodeLocked(ts types.TS) (
	dn *common.GenericDLNode[T], err error) {
	fn := func(nn *nameNode[T]) bool {
		dlNode := nn.GetNode()
		visible, dropped := n.visibilityFn(dlNode, ts)
		if !visible {
			return true
		}
		if dropped {
			return false
		}
		dn = dlNode
		return true
	}
	n.ForEachNodes(fn)
	if dn == nil && err == nil {
		err = moerr.GetOkExpectedEOB()
	}
	return
}

func (n *nodeList[T]) PString(level common.PPLevel) string {
	curr := n.GetNext()
	if curr == nil {
		return fmt.Sprintf("TableNode[\"%s\"](Len=0)", n.name)
	}
	node := curr.(*nameNode[T])
	s := fmt.Sprintf("TableNode[\"%s\"](Len=%d)->[%d", n.name, n.Length(), node.id)
	if level == common.PPL0 {
		s = fmt.Sprintf("%s]", s)
		return s
	}

	curr = curr.GetNext()
	for curr != nil {
		node := curr.(*nameNode[T])
		s = fmt.Sprintf("%s->%d", s, node.id)
		curr = curr.GetNext()
	}
	s = fmt.Sprintf("%s]", s)
	return s
}

type nameNode[T any] struct {
	common.SSLLNode
	getter func(uint64) *common.GenericDLNode[T]
	id     uint64
}

func newNameNode[T any](id uint64,
	getter func(uint64) *common.GenericDLNode[T]) *nameNode[T] {
	return &nameNode[T]{
		SSLLNode: *common.NewSSLLNode(),
		getter:   getter,
		id:       id,
	}
}

func (n *nameNode[T]) GetNode() *common.GenericDLNode[T] {
	if n == nil {
		return nil
	}
	return n.getter(n.id)
}
