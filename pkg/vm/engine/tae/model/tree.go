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

package model

import (
	"bytes"
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type keyT struct {
	Num, Seq uint16
}

func encodeKey(k *keyT) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(k)), 4)
}

type TreeVisitor interface {
	VisitTable(dbID, id uint64) error
	VisitSegment(uint64, uint64, *objectio.Segmentid) error
	VisitBlock(uint64, uint64, *objectio.Segmentid, uint16, uint16) error
	String() string
}

type BaseTreeVisitor struct {
	TableFn   func(uint64, uint64) error
	SegmentFn func(uint64, uint64, *objectio.Segmentid) error
	BlockFn   func(uint64, uint64, *objectio.Segmentid, uint16, uint16) error
}

func (visitor *BaseTreeVisitor) String() string { return "" }

func (visitor *BaseTreeVisitor) VisitTable(dbID, tableID uint64) (err error) {
	if visitor.TableFn != nil {
		return visitor.TableFn(dbID, tableID)
	}
	return
}

func (visitor *BaseTreeVisitor) VisitSegment(dbID, tableID uint64, segmentID *objectio.Segmentid) (err error) {
	if visitor.SegmentFn != nil {
		return visitor.SegmentFn(dbID, tableID, segmentID)
	}
	return
}

func (visitor *BaseTreeVisitor) VisitBlock(
	dbID, tableID uint64, segmentID *objectio.Segmentid, Num, Seq uint16) (err error) {
	if visitor.BlockFn != nil {
		return visitor.BlockFn(dbID, tableID, segmentID, Num, Seq)
	}
	return
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

func (visitor *stringVisitor) VisitSegment(dbID, tableID uint64, id *objectio.Segmentid) (err error) {
	_, _ = visitor.buf.WriteString(fmt.Sprintf("\nTree-SEG[%s]", id.ToString()))
	return
}

func (visitor *stringVisitor) VisitBlock(dbID, tableID uint64, segmentID *objectio.Segmentid, Num, Seq uint16) (err error) {
	_, _ = visitor.buf.WriteString(fmt.Sprintf(" BLK[%d-%d]", Num, Seq))
	return
}

func (visitor *stringVisitor) String() string {
	if visitor.buf.Len() == 0 {
		return "<Empty Tree>"
	}
	return visitor.buf.String()
}

type Tree struct {
	Tables map[uint64]*TableTree
}

type TableTree struct {
	DbID uint64
	ID   uint64
	Segs map[objectio.Segmentid]*SegmentTree
}

type SegmentTree struct {
	ID   *objectio.Segmentid
	Blks map[keyT]struct{}
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
		Segs: make(map[objectio.Segmentid]*SegmentTree),
	}
}

func NewSegmentTree(id *objectio.Segmentid) *SegmentTree {
	return &SegmentTree{
		ID:   id,
		Blks: make(map[keyT]struct{}),
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
		if err = visitor.VisitBlock(table.DbID, table.ID, segment.ID, id.Num, id.Seq); err != nil {
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

func (tree *Tree) Equal(o *Tree) bool {
	if tree == nil && o == nil {
		return true
	} else if tree == nil || o == nil {
		return false
	}
	if len(tree.Tables) != len(o.Tables) {
		return false
	}
	for id, table := range tree.Tables {
		if otable, found := o.Tables[id]; !found {
			return false
		} else {
			if !table.Equal(otable) {
				return false
			}
		}
	}
	return true
}
func (tree *Tree) AddTable(dbID, id uint64) {
	if _, exist := tree.Tables[id]; !exist {
		table := NewTableTree(dbID, id)
		tree.Tables[id] = table
	}
}

func (tree *Tree) AddSegment(dbID, tableID uint64, id *objectio.Segmentid) {
	var table *TableTree
	var exist bool
	if table, exist = tree.Tables[tableID]; !exist {
		table = NewTableTree(dbID, tableID)
		tree.Tables[tableID] = table
	}
	table.AddSegment(id)
}

func (tree *Tree) AddBlock(dbID, tableID uint64, id *objectio.Blockid) {
	sid := objectio.ToSegmentId(id)
	tree.AddSegment(dbID, tableID, sid)
	tree.Tables[tableID].AddBlock(id)
}

func (tree *Tree) Shrink(tableID uint64) (empty bool) {
	delete(tree.Tables, tableID)
	empty = tree.IsEmpty()
	return
}

func (tree *Tree) GetSegment(tableID uint64, segID types.Uuid) *SegmentTree {
	table := tree.GetTable(tableID)
	if table == nil {
		return nil
	}
	return table.GetSegment(segID)
}

func (tree *Tree) Compact() (empty bool) {
	toDelete := make([]uint64, 0)
	for id, table := range tree.Tables {
		if table.Compact() {
			toDelete = append(toDelete, id)
		}
	}
	for _, id := range toDelete {
		delete(tree.Tables, id)
	}
	empty = tree.IsEmpty()
	return
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

func (tree *Tree) WriteTo(w io.Writer) (n int64, err error) {
	cnt := uint32(len(tree.Tables))
	if _, err = w.Write(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 4
	var tmpn int64
	for _, table := range tree.Tables {
		if tmpn, err = table.WriteTo(w); err != nil {
			return
		}
		n += tmpn
	}
	return
}

func (tree *Tree) ReadFrom(r io.Reader) (n int64, err error) {
	var cnt uint32
	if _, err = r.Read(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 4
	if cnt == 0 {
		return
	}
	var tmpn int64
	for i := 0; i < int(cnt); i++ {
		table := NewTableTree(0, 0)
		if tmpn, err = table.ReadFrom(r); err != nil {
			return
		}
		tree.Tables[table.ID] = table
		n += tmpn
	}
	return
}
func (ttree *TableTree) GetSegment(id types.Uuid) *SegmentTree {
	return ttree.Segs[id]
}

func (ttree *TableTree) AddSegment(sid *objectio.Segmentid) {
	id := *sid
	if _, exist := ttree.Segs[id]; !exist {
		ttree.Segs[id] = NewSegmentTree(&id)
	}
}

func (ttree *TableTree) AddBlock(id *objectio.Blockid) {
	sid := objectio.ToSegmentId(id)
	ttree.AddSegment(sid)
	ttree.Segs[*sid].AddBlock(id.Offsets())
}

func (ttree *TableTree) IsEmpty() bool {
	return len(ttree.Segs) == 0
}

func (ttree *TableTree) Shrink(segID types.Uuid) (empty bool) {
	delete(ttree.Segs, segID)
	empty = len(ttree.Segs) == 0
	return
}

func (ttree *TableTree) Compact() (empty bool) {
	toDelete := make([]types.Uuid, 0)
	for id, seg := range ttree.Segs {
		if len(seg.Blks) == 0 {
			toDelete = append(toDelete, id)
		}
	}
	for _, id := range toDelete {
		delete(ttree.Segs, id)
	}
	empty = len(ttree.Segs) == 0
	return
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
		ttree.Segs[*seg.ID].Merge(seg)
	}
}

func (ttree *TableTree) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(types.EncodeUint64(&ttree.DbID)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint64(&ttree.ID)); err != nil {
		return
	}
	cnt := uint32(len(ttree.Segs))
	if _, err = w.Write(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 8 + 8 + 4
	var tmpn int64
	for _, seg := range ttree.Segs {
		if tmpn, err = seg.WriteTo(w); err != nil {
			return
		}
		n += tmpn
	}
	return
}

func (ttree *TableTree) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(types.EncodeUint64(&ttree.DbID)); err != nil {
		return
	}
	if _, err = r.Read(types.EncodeUint64(&ttree.ID)); err != nil {
		return
	}
	var cnt uint32
	if _, err = r.Read(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 8 + 8 + 4
	if cnt == 0 {
		return
	}
	var tmpn int64
	for i := 0; i < int(cnt); i++ {
		id := objectio.NewSegmentid()
		seg := NewSegmentTree(&id)
		if tmpn, err = seg.ReadFrom(r); err != nil {
			return
		}
		ttree.Segs[*seg.ID] = seg
		n += tmpn
	}
	return
}

func (ttree *TableTree) Equal(o *TableTree) bool {
	if ttree == nil && o == nil {
		return true
	} else if ttree == nil || o == nil {
		return false
	}
	if ttree.ID != o.ID || ttree.DbID != o.DbID {
		return false
	}
	if len(ttree.Segs) != len(o.Segs) {
		return false
	}
	for id, seg := range ttree.Segs {
		if oseg, found := o.Segs[id]; !found {
			return false
		} else {
			if !seg.Equal(oseg) {
				return false
			}
		}
	}
	return true
}

func (stree *SegmentTree) AddBlock(Num, Seq uint16) {
	k := keyT{Num, Seq}
	if _, exist := stree.Blks[k]; !exist {
		stree.Blks[k] = struct{}{}
	}
}

func (stree *SegmentTree) Merge(ot *SegmentTree) {
	if ot == nil {
		return
	}
	if !stree.ID.Eq(*ot.ID) {
		panic(fmt.Sprintf("Cannot merge 2 different seg tree: %d, %d", stree.ID, ot.ID))
	}
	for id := range ot.Blks {
		stree.AddBlock(id.Num, id.Seq)
	}
}

func (stree *SegmentTree) Equal(o *SegmentTree) bool {
	if stree == nil && o == nil {
		return true
	} else if stree == nil || o == nil {
		return false
	}
	if !stree.ID.Eq(*o.ID) {
		return false
	}
	if len(stree.Blks) != len(o.Blks) {
		return false
	}
	for id := range stree.Blks {
		if _, found := o.Blks[id]; !found {
			return false
		}
	}
	return true
}

func (stree *SegmentTree) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(stree.ID[:]); err != nil {
		return
	}
	n += int64(types.UuidSize)
	cnt := uint32(len(stree.Blks))
	if _, err = w.Write(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 4
	if cnt == 0 {
		return
	}
	for id := range stree.Blks {
		if _, err = w.Write(encodeKey(&id)); err != nil {
			return
		}
		n += 4
	}
	return
}

func (stree *SegmentTree) Shrink(id *objectio.Blockid) (empty bool) {
	var k keyT
	k.Num, k.Seq = id.Offsets()
	delete(stree.Blks, k)
	empty = len(stree.Blks) == 0
	return
}

func (stree *SegmentTree) IsEmpty() bool {
	return len(stree.Blks) == 0
}

func (stree *SegmentTree) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(stree.ID[:]); err != nil {
		return
	}
	n += int64(types.UuidSize)
	var cnt uint32
	if _, err = r.Read(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 4
	if cnt == 0 {
		return
	}
	var id keyT
	for i := 0; i < int(cnt); i++ {
		if _, err = r.Read(encodeKey(&id)); err != nil {
			return
		}
		stree.Blks[id] = struct{}{}
	}
	n += 4 * int64(cnt)
	return
}
