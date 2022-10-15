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

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type TreeVisitor interface {
	VisitTable(dbID, id uint64) error
	VisitSegment(dbID, tableID, id uint64) error
	VisitBlock(dbID, tableID, segmentID, id uint64) error
	String() string
}

type stringVisitor struct {
	buf bytes.Buffer
}

func (visitor *stringVisitor) VisitTable(dbID, id uint64) (err error) {
	if visitor.buf.Len() != 0 {
		_ = visitor.buf.WriteByte('\n')
	}
	_, _ = visitor.buf.WriteString(fmt.Sprintf("Tree-TBL(%d,%d)", dbID, id))
	return
}

func (visitor *stringVisitor) VisitSegment(dbID, tableID, id uint64) (err error) {
	_, _ = visitor.buf.WriteString(fmt.Sprintf("\n[SEG-%d]", id))
	return
}

func (visitor *stringVisitor) VisitBlock(dbID, tableID, segmentID, id uint64) (err error) {
	_, _ = visitor.buf.WriteString(fmt.Sprintf(" BLK-%d", id))
	return
}

func (visitor *stringVisitor) String() string { return visitor.buf.String() }

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

func (tree *Tree) Reset() {
	tree.Tables = make(map[uint64]*TableTree)
}

func (tree *Tree) String() string {
	visitor := new(stringVisitor)
	_ = tree.Visit(visitor)
	return visitor.String()
}

func (tree *Tree) visitSegment(visitor TreeVisitor, table *TableTree, segment *SegmentTree) (err error) {
	for id := range segment.Blks {
		if err = visitor.VisitBlock(table.DbID, table.ID, segment.ID, id); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
			}
			return
		}
	}
	return
}

func (tree *Tree) visitTable(visitor TreeVisitor, table *TableTree) (err error) {
	for _, segment := range table.Segs {
		if err = visitor.VisitSegment(table.DbID, table.ID, segment.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
			}
			return
		}
		if err = tree.visitSegment(visitor, table, segment); err != nil {
			return
		}
	}
	return
}

func (tree *Tree) Visit(visitor TreeVisitor) (err error) {
	for _, table := range tree.Tables {
		if err = visitor.VisitTable(table.DbID, table.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				return
			}
			return
		}
		if err = tree.visitTable(visitor, table); err != nil {
			return
		}
	}
	return
}
func (tree *Tree) IsEmpty() bool                 { return tree.TableCount() == 0 }
func (tree *Tree) TableCount() int               { return len(tree.Tables) }
func (tree *Tree) GetTable(id uint64) *TableTree { return tree.Tables[id] }
func (tree *Tree) HasTable(id uint64) bool {
	_, found := tree.Tables[id]
	return found
}

func (tree *Tree) AddTable(dbID, id uint64) {
	if _, exist := tree.Tables[id]; !exist {
		table := NewTableTree(dbID, id)
		tree.Tables[id] = table
	}
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

func (tree *Tree) GetSegment(tableID, segID uint64) *SegmentTree {
	table := tree.GetTable(tableID)
	if table == nil {
		return nil
	}
	return table.GetSegment(segID)
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

func (ttree *TableTree) GetSegment(id uint64) *SegmentTree {
	return ttree.Segs[id]
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
