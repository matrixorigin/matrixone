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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"
)

type TableMVCCNode struct {
	SchemaConstraints string // store as immutable, bytes actually
}

func NewEmptyTableMVCCNode() *TableMVCCNode {
	return &TableMVCCNode{}
}

func (e *TableMVCCNode) CloneAll() *TableMVCCNode {
	node := &TableMVCCNode{}
	node.SchemaConstraints = e.SchemaConstraints
	return node
}

func (e *TableMVCCNode) CloneData() *TableMVCCNode {
	return &TableMVCCNode{
		SchemaConstraints: e.SchemaConstraints,
	}
}

func (e *TableMVCCNode) String() string {

	return fmt.Sprintf("cstr[%d]",
		len(e.SchemaConstraints))
}

// for create drop in one txn
func (e *TableMVCCNode) Update(un *TableMVCCNode) {
	e.SchemaConstraints = un.SchemaConstraints
}

func (e *TableMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	n, err = common.WriteString(e.SchemaConstraints, w)
	return
}

func (e *TableMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	e.SchemaConstraints, n, err = common.ReadString(r)
	return
}

type TableNode struct {
	schema *Schema
}

func (node *TableNode) WriteTo(w io.Writer) (n int64, err error) {
	var schemaBuf []byte
	if schemaBuf, err = node.schema.Marshal(); err != nil {
		return
	}
	if _, err = w.Write(schemaBuf); err != nil {
		return
	}
	n += int64(len(schemaBuf))
	return
}

func (node *TableNode) ReadFrom(r io.Reader) (n int64, err error) {
	node.schema = NewEmptySchema("")
	if n, err = node.schema.ReadFrom(r); err != nil {
		return
	}
	return
}
