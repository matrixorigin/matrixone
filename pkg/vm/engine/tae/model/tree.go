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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	MemoTreeVersion1 uint16 = iota
	MemoTreeVersion2
	MemoTreeVersion3
	MemoTreeVersion4
)

type TreeVisitor interface {
	VisitTable(dbID, id uint64) error
	String() string
}

type BaseTreeVisitor struct {
	TableFn func(uint64, uint64) error
}

func (visitor *BaseTreeVisitor) String() string { return "" }

func (visitor *BaseTreeVisitor) VisitTable(dbID, tableID uint64) (err error) {
	if visitor.TableFn != nil {
		return visitor.TableFn(dbID, tableID)
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

func (visitor *stringVisitor) String() string {
	if visitor.buf.Len() == 0 {
		return "<Empty Tree>"
	}
	return visitor.buf.String()
}

type Tree struct {
	Tables map[uint64]*TableRecord
}

func NewTree() *Tree {
	return &Tree{
		Tables: make(map[uint64]*TableRecord),
	}
}

func (tree *Tree) Reset() {
	tree.Tables = make(map[uint64]*TableRecord)
}

func (tree *Tree) String() string {
	visitor := new(stringVisitor)
	_ = tree.Visit(visitor)
	return visitor.String()
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
	}
	return
}
func (tree *Tree) IsEmpty() bool                   { return tree.TableCount() == 0 }
func (tree *Tree) TableCount() int                 { return len(tree.Tables) }
func (tree *Tree) GetTable(id uint64) *TableRecord { return tree.Tables[id] }
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
	for id := range tree.Tables {
		if _, found := o.Tables[id]; !found {
			return false
		}
	}
	return true
}

func (tree *Tree) AddTable(dbID, id uint64) {
	if _, exist := tree.Tables[id]; !exist {
		tree.Tables[id] = NewTableTree(dbID, id)
	}
}

func (tree *Tree) AddObject(dbID, tableID uint64, _ *objectio.ObjectId, _ bool) {
	tree.AddTable(dbID, tableID)
}

func (tree *Tree) Shrink(tableID uint64) (empty bool) {
	delete(tree.Tables, tableID)
	empty = tree.IsEmpty()
	return
}

func (tree *Tree) Merge(ot *Tree) {
	if ot == nil {
		return
	}
	for _, ott := range ot.Tables {
		if _, found := tree.Tables[ott.ID]; !found {
			tree.Tables[ott.ID] = NewTableTree(ott.DbID, ott.ID)
		}
	}
}

func (tree *Tree) ApproxSize() int64 {
	var (
		size int64
	)

	size += 4 // len of tables
	for i := range tree.Tables {
		size += tree.Tables[i].ApproxSize()
	}

	return size
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

type TableRecord struct {
	DbID uint64
	ID   uint64
}

func NewTableTree(dbID, id uint64) *TableRecord {
	return &TableRecord{
		DbID: dbID,
		ID:   id,
	}
}

func (ttree *TableRecord) ApproxSize() int64 {
	var (
		size int64
	)

	size += 8 + 8 // dbId, tblId

	return size
}

func (ttree *TableRecord) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(types.EncodeUint64(&ttree.DbID)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint64(&ttree.ID)); err != nil {
		return
	}
	n += 8 + 8
	return
}

func (ttree *TableRecord) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	if _, err = r.Read(types.EncodeUint64(&ttree.DbID)); err != nil {
		return
	}
	if _, err = r.Read(types.EncodeUint64(&ttree.ID)); err != nil {
		return
	}
	n += 8 + 8
	if ver >= MemoTreeVersion4 {
		return
	}
	// ver3 skip old data
	var cnt uint32
	if _, err = r.Read(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 4
	if cnt != 0 {
		for i := 0; i < int(cnt); i++ {
			id := objectio.NewObjectid()
			if ver == MemoTreeVersion3 {
				if _, err = r.Read(id[:]); err != nil {
					return
				}
				n += types.ObjectidSize
			} else {
				panic("not supported")
			}
		}
	}

	if _, err = r.Read(types.EncodeUint32(&cnt)); err != nil {
		return
	}
	n += 4
	if cnt == 0 {
		return
	}
	for i := 0; i < int(cnt); i++ {
		id := objectio.NewObjectid()
		if _, err = r.Read(id[:]); err != nil {
			return
		}
		n += types.ObjectidSize
	}
	return
}
