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

package txnimpl

import (
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var notFoundErr = moerr.NewNotFoundNoCtx()

func newDuplicateEntryWithLog(attr, entry, _ string) error {
	return moerr.NewDuplicateEntryNoCtx(entry, attr)
}

type TableIndex interface {
	io.Closer
	BatchDedup(string, containers.Vector) error
	BatchInsert(string, containers.Vector, int, int, uint32, bool) error
	Insert(any, uint32) error
	Delete(any) error
	Search(any) (uint32, error)
	Name() string
	Count() int
	KeyToVector(types.Type) containers.Vector
	KeyToVectors(types.Type) []containers.Vector
}

type simpleTableIndex struct {
	sync.RWMutex
	tree map[any]uint32
}

func NewSimpleTableIndex() *simpleTableIndex {
	return &simpleTableIndex{
		tree: make(map[any]uint32),
	}
}

// DedupOp returns a duplicate-entry error for the first row of col whose key
// is in tree.
func DedupOp(attr string, col containers.Vector, tree map[any]uint32) error {
	for i := 0; i < col.Length(); i++ {
		if _, ok := tree[vector.RowKeyView(col, i)]; ok {
			entry := common.TypeStringValue(*col.GetType(), vector.GetAny(col, i, false), false)
			return newDuplicateEntryWithLog(attr, entry, "txnimpl.DedupOp")
		}
	}
	return nil
}

// InsertOp adds rows [start, start+count) of col to tree, numbering them from
// fromRow, and fails on a key already there. With dedupInput it only checks
// that the rows have distinct keys.
func InsertOp(
	attr string,
	col containers.Vector,
	start, count int,
	fromRow uint32,
	dedupInput bool,
	tree map[any]uint32) error {
	t := col.GetType()
	if dedupInput {
		set := make(map[any]bool, count)
		for i := start; i < start+count; i++ {
			key := vector.RowKeyView(col, i)
			if _, ok := set[key]; ok {
				entry := common.TypeStringValue(*t, vector.GetAny(col, i, false), false)
				return newDuplicateEntryWithLog(attr, entry, "txnimpl.InsertOp.dedupInput")
			}
			set[key] = true
		}
		return nil
	}
	for i := start; i < start+count; i++ {
		key := vector.RowKey(col, i)
		if _, ok := tree[key]; ok {
			entry := common.TypeStringValue(*t, vector.GetAny(col, i, false), false)
			return newDuplicateEntryWithLog(attr, entry, "txnimpl.InsertOp.tree")
		}
		tree[key] = fromRow
		fromRow++
	}
	return nil
}

func (idx *simpleTableIndex) KeyToVector(kType types.Type) containers.Vector {
	vec := makeWorkspaceVector(kType)
	for k := range idx.tree {
		vec.MustAppendValue(vector.RowKeyValue(k), false)
	}
	return vec
}

func (idx *simpleTableIndex) KeyToVectors(kType types.Type) []containers.Vector {
	vec := makeWorkspaceVector(kType)
	var vecs []containers.Vector
	for k := range idx.tree {
		if vec.Length() > int(MaxNodeRows) {
			vecs = append(vecs, vec)
			vec = makeWorkspaceVector(kType)
		}
		vec.MustAppendValue(vector.RowKeyValue(k), false)
	}
	if vec.Length() > 0 {
		vecs = append(vecs, vec)
	}
	return vecs
}

func (idx *simpleTableIndex) Close() error {
	idx.tree = nil
	return nil
}
func (idx *simpleTableIndex) Name() string { return "SimpleIndex" }
func (idx *simpleTableIndex) Count() int {
	idx.RLock()
	cnt := len(idx.tree)
	idx.RUnlock()
	return cnt
}

func (idx *simpleTableIndex) Insert(v any, row uint32) error {
	idx.Lock()
	defer idx.Unlock()
	_, ok := idx.tree[v]
	if ok {
		return moerr.GetOkExpectedDup()
	}
	idx.tree[v] = row
	return nil
}
func (idx *simpleTableIndex) Delete(vv any) error {
	idx.Lock()
	defer idx.Unlock()
	var v any
	switch vv := vv.(type) {
	case []uint8:
		v = string(vv)
	default:
		v = vv
	}
	_, ok := idx.tree[v]
	if !ok {
		return moerr.GetOkExpectedDup()
	}
	delete(idx.tree, v)
	return nil
}

func (idx *simpleTableIndex) Search(v any) (uint32, error) {
	idx.RLock()
	defer idx.RUnlock()
	row, ok := idx.tree[v]
	if !ok {
		return 0, notFoundErr
	}
	return uint32(row), nil
}

func (idx *simpleTableIndex) BatchInsert(
	attr string,
	col containers.Vector,
	start, count int,
	row uint32,
	dedupInput bool) error {
	idx.Lock()
	defer idx.Unlock()
	if !vector.SupportsAny(col.GetType().Oid) {
		panic(moerr.NewInternalErrorNoCtxf("%s not supported", col.GetType().String()))
	}
	return InsertOp(attr, col, start, count, row, dedupInput, idx.tree)
}

// TODO: rewrite
func (idx *simpleTableIndex) BatchDedup(attr string, col containers.Vector) error {
	idx.RLock()
	defer idx.RUnlock()
	if !vector.SupportsAny(col.GetType().Oid) {
		panic(moerr.NewInternalErrorNoCtxf("%s not supported", col.GetType().String()))
	}
	return DedupOp(attr, col, idx.tree)
}
