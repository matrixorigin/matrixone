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

package metadata

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"

	"github.com/google/btree"
)

type nodeList struct {
	common.SSLLNode
	host     interface{}
	rwlocker *sync.RWMutex
	name     string
}

func newNodeList(host interface{}, rwlocker *sync.RWMutex, name string) *nodeList {
	return &nodeList{
		SSLLNode: *common.NewSSLLNode(),
		host:     host,
		rwlocker: rwlocker,
		name:     name,
	}
}

func (n *nodeList) Less(item btree.Item) bool {
	return n.name < item.(*nodeList).name
}

func (n *nodeList) CreateNode(id uint64) *nameNode {
	nn := newNameNode(n.host, id)
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	n.Insert(nn)
	return nn
}

func (n *nodeList) DeleteNode(id uint64) (deleted *nameNode, empty bool) {
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	var prev common.ISSLLNode
	prev = n
	curr := n.GetNext()
	depth := 0
	for curr != nil {
		nid := curr.(*nameNode).Id
		if id == nid {
			prev.ReleaseNextNode()
			deleted = curr.(*nameNode)
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

func (n *nodeList) ForEachNodes(fn func(*nameNode) bool) {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	n.ForEachNodesLocked(fn)
}

func (n *nodeList) ForEachNodesLocked(fn func(*nameNode) bool) {
	curr := n.GetNext()
	for curr != nil {
		nn := curr.(*nameNode)
		if ok := fn(nn); !ok {
			break
		}
		curr = curr.GetNext()
	}
}

func (n *nodeList) LengthLocked() int {
	length := 0
	fn := func(*nameNode) bool {
		length++
		return true
	}
	n.ForEachNodesLocked(fn)
	return length
}

func (n *nodeList) Length() int {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.LengthLocked()
}

func (n *nodeList) GetTable() *Table {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.GetNext().(*nameNode).GetTable()
}

func (n *nodeList) GetDatabase() *Database {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.GetNext().(*nameNode).GetDatabase()
}

func (n *nodeList) PString(level PPLevel) string {
	curr := n.GetNext()
	if curr == nil {
		return fmt.Sprintf("TableNode[\"%s\"](Len=0)", n.name)
	}
	node := curr.(*nameNode)
	s := fmt.Sprintf("TableNode[\"%s\"](Len=%d)->[%d", n.name, n.Length(), node.Id)
	if level == PPL0 {
		s = fmt.Sprintf("%s]", s)
		return s
	}
	curr = curr.GetNext()
	for curr != nil {
		node := curr.(*nameNode)
		s = fmt.Sprintf("%s->%d", s, node.Id)
		curr = curr.GetNext()
	}
	s = fmt.Sprintf("%s]", s)
	return s
}

type nameNode struct {
	common.SSLLNode
	Id   uint64
	host interface{}
}

func newNameNode(host interface{}, id uint64) *nameNode {
	return &nameNode{
		Id:       id,
		SSLLNode: *common.NewSSLLNode(),
		host:     host,
	}
}

func (n *nameNode) GetDatabase() *Database {
	if n == nil {
		return nil
	}
	return n.host.(*Catalog).Databases[n.Id]
}

func (n *nameNode) GetTable() *Table {
	if n == nil {
		return nil
	}
	return n.host.(*Database).TableSet[n.Id]
}
