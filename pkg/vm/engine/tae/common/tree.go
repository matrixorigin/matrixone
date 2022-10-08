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

package common

import "fmt"

type Tree struct {
	Tables map[uint64]*TableTree
}

type TableTree struct {
	DbID uint64
	ID   uint64
	Segs map[uint64]*SegmentTree
}

type SegmentTree struct {
	ID   uint64
	Blks map[uint64]bool
}

func NewTree() *Tree {
	return &Tree{
		Tables: make(map[uint64]*TableTree),
	}
}

func NewTableTree(dbID, id uint64) *TableTree {
	return &TableTree{
		DbID: dbID,
		ID:   id,
		Segs: make(map[uint64]*SegmentTree),
	}
}

func NewSegmentTree(id uint64) *SegmentTree {
	return &SegmentTree{
		ID:   id,
		Blks: make(map[uint64]bool),
	}
}

func (tree *Tree) TableCount() int               { return len(tree.Tables) }
func (tree *Tree) GetTable(id uint64) *TableTree { return tree.Tables[id] }
func (tree *Tree) HasTable(id uint64) bool {
	_, found := tree.Tables[id]
	return found
}

func (tree *Tree) AddSegment(dbID, tableID, id uint64) {
	var table *TableTree
	var exist bool
	if table, exist = tree.Tables[tableID]; !exist {
		table = NewTableTree(dbID, tableID)
		tree.Tables[tableID] = table
	}
	table.AddSegment(id)
}

func (tree *Tree) AddBlock(dbID, tableID, segID, id uint64) {
	tree.AddSegment(dbID, tableID, segID)
	tree.Tables[tableID].AddBlock(segID, id)
}

func (tree *Tree) Merge(ot *Tree) {
	if ot == nil {
		return
	}
	for _, ott := range ot.Tables {
		t, found := tree.Tables[ott.ID]
		if !found {
			t = NewTableTree(ott.DbID, ott.ID)
			tree.Tables[ott.ID] = t
		}
		t.Merge(ott)
	}
}

func (ttree *TableTree) AddSegment(id uint64) {
	if _, exist := ttree.Segs[id]; !exist {
		ttree.Segs[id] = NewSegmentTree(id)
	}
}

func (ttree *TableTree) AddBlock(segID, id uint64) {
	ttree.AddSegment(segID)
	ttree.Segs[segID].AddBlock(id)
}

func (ttree *TableTree) Merge(ot *TableTree) {
	if ot == nil {
		return
	}
	if ot.ID != ttree.ID {
		panic(fmt.Sprintf("Cannot merge 2 different table tree: %d, %d", ttree.ID, ot.ID))
	}
	for _, seg := range ot.Segs {
		ttree.AddSegment(seg.ID)
		ttree.Segs[seg.ID].Merge(seg)
	}
}

func (stree *SegmentTree) AddBlock(id uint64) {
	if _, exist := stree.Blks[id]; !exist {
		stree.Blks[id] = true
	}
}

func (stree *SegmentTree) Merge(ot *SegmentTree) {
	if ot == nil {
		return
	}
	if ot.ID != stree.ID {
		panic(fmt.Sprintf("Cannot merge 2 different seg tree: %d, %d", stree.ID, ot.ID))
	}
	for id := range ot.Blks {
		stree.AddBlock(id)
	}
}
