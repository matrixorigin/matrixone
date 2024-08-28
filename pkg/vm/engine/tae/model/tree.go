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
	"encoding/hex"
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	MemoTreeVersion1 uint16 = iota
	MemoTreeVersion2
	MemoTreeVersion3
)

type keyT struct {
	Num, Seq uint16
}

func encodeKey(k *keyT) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(k)), 4)
}

type TreeVisitor interface {
	VisitTable(dbID, id uint64) error
	VisitObject(uint64, uint64, *objectio.ObjectId) error
	VisitTombstone(uint64, uint64, *objectio.ObjectId) error
	String() string
}

type BaseTreeVisitor struct {
	TableFn     func(uint64, uint64) error
	ObjectFn    func(uint64, uint64, *objectio.ObjectId) error
	TombstoneFn func(uint64, uint64, *objectio.ObjectId) error
}

func (visitor *BaseTreeVisitor) String() string { return "" }

func (visitor *BaseTreeVisitor) VisitTable(dbID, tableID uint64) (err error) {
	if visitor.TableFn != nil {
		return visitor.TableFn(dbID, tableID)
	}
	return
}

func (visitor *BaseTreeVisitor) VisitObject(dbID, tableID uint64, ObjectID *objectio.ObjectId) (err error) {
	if visitor.ObjectFn != nil {
		return visitor.ObjectFn(dbID, tableID, ObjectID)
	}
	return
}

func (visitor *BaseTreeVisitor) VisitTombstone(dbID, tableID uint64, ObjectID *objectio.ObjectId) (err error) {
	if visitor.TombstoneFn != nil {
		return visitor.TombstoneFn(dbID, tableID, ObjectID)
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

func (visitor *stringVisitor) VisitObject(dbID, tableID uint64, id *objectio.ObjectId) (err error) {
	_, _ = visitor.buf.WriteString(fmt.Sprintf("\nTree-OBJ[%s]", id.String()))
	return
}

func (visitor *stringVisitor) VisitTombstone(dbID, tableID uint64, id *objectio.ObjectId) (err error) {
	_, _ = visitor.buf.WriteString(fmt.Sprintf("\nTree-Tombstone[%s]", id.String()))
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
	DbID       uint64
	ID         uint64
	Objs       map[objectio.ObjectId]*ObjectTree
	Tombstones map[objectio.ObjectId]*ObjectTree
}

type ObjectTree struct {
	ID *objectio.ObjectId
}

func NewTree() *Tree {
	return &Tree{
		Tables: make(map[uint64]*TableTree),
	}
}

func NewTableTree(dbID, id uint64) *TableTree {
	return &TableTree{
		DbID:       dbID,
		ID:         id,
		Objs:       make(map[objectio.ObjectId]*ObjectTree),
		Tombstones: make(map[objectio.ObjectId]*ObjectTree),
	}
}

func NewObjectTree(id *objectio.ObjectId) *ObjectTree {
	return &ObjectTree{
		ID: id,
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

func (tree *Tree) visitTable(visitor TreeVisitor, table *TableTree) (err error) {
	for _, Object := range table.Objs {
		if err = visitor.VisitObject(table.DbID, table.ID, Object.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				continue
			}
			return
		}
	}
	for _, Object := range table.Tombstones {
		if err = visitor.VisitTombstone(table.DbID, table.ID, Object.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				continue
			}
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
				continue
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

func (tree *Tree) AddObject(dbID, tableID uint64, id *objectio.ObjectId, isTombstone bool) {
	var table *TableTree
	var exist bool
	if table, exist = tree.Tables[tableID]; !exist {
		table = NewTableTree(dbID, tableID)
		tree.Tables[tableID] = table
	}
	table.AddObject(id, isTombstone)
}

func (tree *Tree) Shrink(tableID uint64) (empty bool) {
	delete(tree.Tables, tableID)
	empty = tree.IsEmpty()
	return
}

func (tree *Tree) GetObject(tableID uint64, objID types.Objectid, isTombstone bool) *ObjectTree {
	table := tree.GetTable(tableID)
	if table == nil {
		return nil
	}
	return table.GetObject(objID, isTombstone)
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

func (tree *Tree) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
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
		if tmpn, err = table.ReadFromWithVersion(r, ver); err != nil {
			return
		}
		tree.Tables[table.ID] = table
		n += tmpn
	}
	return
}
func (ttree *TableTree) GetObject(id types.Objectid, isTombstone bool) *ObjectTree {
	if isTombstone {
		return ttree.Tombstones[id]
	} else {
		return ttree.Objs[id]
	}
}

func (ttree *TableTree) AddObject(sid *objectio.ObjectId, isTombstone bool) {
	id := *sid
	if isTombstone {
		if _, exist := ttree.Tombstones[id]; !exist {
			ttree.Tombstones[id] = NewObjectTree(&id)
		}
	} else {
		if _, exist := ttree.Objs[id]; !exist {
			ttree.Objs[id] = NewObjectTree(&id)
		}
	}
}

func (ttree *TableTree) ShortBlocksString() string {
	buf := bytes.Buffer{}
	for _, obj := range ttree.Objs {
		var shortuuid [8]byte
		hex.Encode(shortuuid[:], obj.ID[:4])
		buf.WriteString(fmt.Sprintf(" %s-%d", string(shortuuid[:]), obj.ID.Offset()))
	}
	for _, obj := range ttree.Tombstones {
		var shortuuid [8]byte
		hex.Encode(shortuuid[:], obj.ID[:4])
		buf.WriteString(fmt.Sprintf(" %s-%d", string(shortuuid[:]), obj.ID.Offset()))
	}
	return buf.String()
}

func (ttree *TableTree) IsEmpty() bool {
	return len(ttree.Objs) == 0 && len(ttree.Tombstones) == 0
}

func (ttree *TableTree) Shrink(objID types.Objectid, isTombstone bool) (empty bool) {
	if isTombstone {
		delete(ttree.Tombstones, objID)
		empty = ttree.IsEmpty()
		return
	}
	delete(ttree.Objs, objID)
	empty = ttree.IsEmpty()
	return
}

func (ttree *TableTree) Compact() (empty bool) {
	empty = ttree.IsEmpty()
	return
}

func (ttree *TableTree) Merge(ot *TableTree) {
	if ot == nil {
		return
	}
	if ot.ID != ttree.ID {
		panic(fmt.Sprintf("Cannot merge 2 different table tree: %d, %d", ttree.ID, ot.ID))
	}
	for _, obj := range ot.Objs {
		ttree.AddObject(obj.ID, false)
	}
	for _, obj := range ot.Tombstones {
		ttree.AddObject(obj.ID, true)
	}
}

func (ttree *TableTree) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(types.EncodeUint64(&ttree.DbID)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint64(&ttree.ID)); err != nil {
		return
	}
	cnt := uint32(len(ttree.Objs))
	if _, err = w.Write(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 8 + 8 + 4
	var tmpn int64
	for _, obj := range ttree.Objs {
		if tmpn, err = obj.WriteTo(w); err != nil {
			return
		}
		n += tmpn
	}
	cnt = uint32(len(ttree.Tombstones))
	if _, err = w.Write(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 8 + 8 + 4
	for _, obj := range ttree.Tombstones {
		if tmpn, err = obj.WriteTo(w); err != nil {
			return
		}
		n += tmpn
	}
	return
}

func (ttree *TableTree) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
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
	var tmpn int64
	if cnt != 0 {
		for i := 0; i < int(cnt); i++ {
			id := objectio.NewObjectid()
			if ver < MemoTreeVersion2 {
				objs, tmpn, err := ReadObjectTreesV1(r)
				if err != nil {
					return n, err
				}
				for _, obj := range objs {
					ttree.Objs[*obj.ID] = obj
				}
				n += tmpn

			} else if ver < MemoTreeVersion3 {
				obj := NewObjectTree(id)
				if tmpn, err = obj.ReadFromV2(r); err != nil {
					return
				}
				ttree.Objs[*obj.ID] = obj
				n += tmpn
			} else {
				obj := NewObjectTree(id)
				if tmpn, err = obj.ReadFromV3(r); err != nil {
					return
				}
				ttree.Objs[*obj.ID] = obj
				n += tmpn

			}
		}
	}

	if _, err = r.Read(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 8 + 8 + 4
	if cnt == 0 {
		return
	}
	for i := 0; i < int(cnt); i++ {
		id := objectio.NewObjectid()
		obj := NewObjectTree(id)
		if tmpn, err = obj.ReadFromV3(r); err != nil {
			return
		}
		ttree.Tombstones[*obj.ID] = obj
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
	if len(ttree.Objs) != len(o.Objs) {
		return false
	}
	if len(ttree.Tombstones) != len(o.Tombstones) {
		return false
	}
	for id, obj := range ttree.Objs {
		if oobj, found := o.Objs[id]; !found {
			return false
		} else {
			if !obj.Equal(oobj) {
				return false
			}
		}
	}
	for id, obj := range ttree.Tombstones {
		if oobj, found := o.Tombstones[id]; !found {
			return false
		} else {
			if !obj.Equal(oobj) {
				return false
			}
		}
	}
	return true
}

func (stree *ObjectTree) Merge(ot *ObjectTree) {
	if ot == nil {
		return
	}
	if !stree.ID.Eq(*ot.ID) {
		panic(fmt.Sprintf("Cannot merge 2 different obj tree: %d, %d", stree.ID, ot.ID))
	}
}

func (stree *ObjectTree) Equal(o *ObjectTree) bool {
	if stree == nil && o == nil {
		return true
	} else if stree == nil || o == nil {
		return false
	}
	return stree.ID.Eq(*o.ID)
}

func (stree *ObjectTree) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(stree.ID[:]); err != nil {
		return
	}
	n += int64(types.UuidSize)
	return
}

func ReadObjectTreesV1(r io.Reader) (strees []*ObjectTree, n int64, err error) {
	segmentID := new(types.Segmentid)
	if _, err = r.Read(segmentID[:]); err != nil {
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
	numSeqMap := make(map[uint16][]uint16)
	var id keyT
	for i := 0; i < int(cnt); i++ {
		if _, err = r.Read(encodeKey(&id)); err != nil {
			return
		}
		seqs, ok := numSeqMap[id.Num]
		if !ok {
			seqs = make([]uint16, 0)
		}
		seqs = append(seqs, id.Seq)
		numSeqMap[id.Num] = seqs
	}
	n += 4 * int64(cnt)

	return
}

func (stree *ObjectTree) ReadFromV2(r io.Reader) (n int64, err error) {
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
	var id uint16
	for i := 0; i < int(cnt); i++ {
		if _, err = r.Read(types.EncodeUint16(&id)); err != nil {
			return
		}
	}
	n += 4 * int64(cnt)
	return
}

func (stree *ObjectTree) ReadFromV3(r io.Reader) (n int64, err error) {
	if _, err = r.Read(stree.ID[:]); err != nil {
		return
	}
	n += int64(types.UuidSize)
	return
}
