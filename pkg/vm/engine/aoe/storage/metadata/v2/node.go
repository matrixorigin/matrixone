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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"

	"github.com/google/btree"
)

type tableNode struct {
	common.SSLLNode
	Catalog *Catalog
	name    string
}

func newTableNode(catalog *Catalog, name string) *tableNode {
	return &tableNode{
		SSLLNode: *common.NewSSLLNode(),
		Catalog:  catalog,
		name:     name,
	}
}

func (n *tableNode) Less(item btree.Item) bool {
	return n.name < item.(*tableNode).name
}

func (n *tableNode) CreateNode(id uint64) *nameNode {
	nn := newNameNode(n.Catalog, id)
	n.Catalog.nodesMu.Lock()
	defer n.Catalog.nodesMu.Unlock()
	n.Insert(nn)
	return nn
}

func (n *tableNode) DeleteNode(id uint64) (deleted *nameNode, empty bool) {
	n.Catalog.nodesMu.Lock()
	defer n.Catalog.nodesMu.Unlock()
	var prev common.ISSLLNode
	prev = n
	curr := n.GetNext()
	depth := 0
	for curr != nil {
		tableId := curr.(*nameNode).Id
		if id == tableId {
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

func (n *tableNode) LengthLocked() int {
	curr := n.GetNext()
	length := 0
	for curr != nil {
		curr = curr.GetNext()
		length++
	}
	return length
}

func (n *tableNode) Length() int {
	n.Catalog.nodesMu.RLock()
	defer n.Catalog.nodesMu.RUnlock()
	return n.LengthLocked()
}

func (n *tableNode) GetEntry() *Table {
	n.Catalog.nodesMu.RLock()
	defer n.Catalog.nodesMu.RUnlock()
	return n.GetNext().(*nameNode).GetEntry()
}

func (n *tableNode) PString(level PPLevel) string {
	curr := n.GetNext()
	if curr == nil {
		return fmt.Sprintf("TableNode[\"%s\"](Len=0)", n.name)
	}
	entry := curr.(*nameNode).GetEntry()
	s := fmt.Sprintf("TableNode[\"%s\"](Len=%d)->[%d", entry.Schema.Name, n.Length(), entry.Id)
	if level == PPL0 {
		s = fmt.Sprintf("%s]", s)
		return s
	}
	curr = curr.GetNext()
	for curr != nil {
		entry := curr.(*nameNode).GetEntry()
		s = fmt.Sprintf("%s->%d", s, entry.Id)
		curr = curr.GetNext()
	}
	s = fmt.Sprintf("%s]", s)
	return s
}

type nameNode struct {
	common.SSLLNode
	Id      uint64
	Catalog *Catalog
}

func newNameNode(catalog *Catalog, id uint64) *nameNode {
	return &nameNode{
		Id:       id,
		SSLLNode: *common.NewSSLLNode(),
		Catalog:  catalog,
	}
}

func (n *nameNode) GetEntry() *Table {
	if n == nil {
		return nil
	}
	return n.Catalog.TableSet[n.Id]
}
