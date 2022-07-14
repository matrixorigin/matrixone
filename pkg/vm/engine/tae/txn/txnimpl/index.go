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
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type TableIndex interface {
	io.Closer
	BatchDedup(containers.Vector) error
	BatchInsert(containers.Vector, int, int, uint32, bool) error
	Insert(any, uint32) error
	Delete(any) error
	Search(any) (uint32, error)
	Name() string
	Count() int
	KeyToVector(types.Type) containers.Vector
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

func DedupOp[T comparable](vs any, tree map[any]uint32) (err error) {
	vals := vs.([]T)
	for _, v := range vals {
		if _, ok := tree[v]; ok {
			return data.ErrDuplicate
		}
	}
	return
}

func InsertOp[T comparable](input any, start, count int, fromRow uint32, dedupInput bool, tree map[any]uint32) (err error) {
	vals := input.([]T)
	if dedupInput {
		set := make(map[T]bool)
		for _, v := range vals[start : start+count] {
			if _, ok := set[v]; ok {
				return data.ErrDuplicate
			}
			set[v] = true
		}
		return
	}
	for _, v := range vals[start : start+count] {
		if _, ok := tree[v]; ok {
			return data.ErrDuplicate
		}
		tree[v] = fromRow
		fromRow++
	}
	return
}

func (idx *simpleTableIndex) KeyToVector(kType types.Type) containers.Vector {
	vec := containers.MakeVector(kType, false)
	switch kType.Oid {
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		for k := range idx.tree {
			vec.Append([]byte(k.(string)))
		}
	default:
		for k := range idx.tree {
			vec.Append(k)
		}
	}
	return vec
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
		return data.ErrDuplicate
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
		return data.ErrNotFound
	}
	delete(idx.tree, v)
	return nil
}

func (idx *simpleTableIndex) Search(v any) (uint32, error) {
	idx.RLock()
	defer idx.RUnlock()
	row, ok := idx.tree[v]
	if !ok {
		return 0, data.ErrNotFound
	}
	return uint32(row), nil
}

func (idx *simpleTableIndex) BatchInsert(col containers.Vector, start, count int, row uint32, dedupInput bool) error {
	idx.Lock()
	defer idx.Unlock()
	switch col.GetType().Oid {
	case types.Type_BOOL:
		return InsertOp[bool](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_INT8:
		return InsertOp[int8](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_INT16:
		return InsertOp[int16](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_INT32:
		return InsertOp[int32](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_INT64:
		return InsertOp[int64](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_UINT8:
		return InsertOp[uint8](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_UINT16:
		return InsertOp[uint16](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_UINT32:
		return InsertOp[uint32](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_UINT64:
		return InsertOp[uint64](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_DECIMAL64:
		return InsertOp[types.Decimal64](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_DECIMAL128:
		return InsertOp[types.Decimal128](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_FLOAT32:
		return InsertOp[float32](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_FLOAT64:
		return InsertOp[float64](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_DATE:
		return InsertOp[types.Date](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_TIMESTAMP:
		return InsertOp[types.Timestamp](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_DATETIME:
		return InsertOp[types.Datetime](col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vs := col.Slice().(*containers.Bytes)
		if dedupInput {
			set := make(map[string]bool)
			for i, s := range vs.Offset[start : start+count] {
				e := s + vs.Length[i+start]
				v := string(vs.Data[s:e])
				if _, ok := set[v]; ok {
					return data.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for i, s := range vs.Offset[start : start+count] {
			e := s + vs.Length[i+start]
			v := string(vs.Data[s:e])
			if _, ok := idx.tree[v]; ok {
				return data.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	default:
		panic(fmt.Errorf("%s not supported", col.GetType().String()))
	}
	return nil
}

// TODO: rewrite
func (idx *simpleTableIndex) BatchDedup(col containers.Vector) error {
	idx.RLock()
	defer idx.RUnlock()
	vals := col.Slice()
	switch col.GetType().Oid {
	case types.Type_BOOL:
		return DedupOp[bool](vals, idx.tree)
	case types.Type_INT8:
		return DedupOp[int8](vals, idx.tree)
	case types.Type_INT16:
		return DedupOp[int16](vals, idx.tree)
	case types.Type_INT32:
		return DedupOp[int32](vals, idx.tree)
	case types.Type_INT64:
		return DedupOp[int64](vals, idx.tree)
	case types.Type_UINT8:
		return DedupOp[uint8](vals, idx.tree)
	case types.Type_UINT16:
		return DedupOp[uint16](vals, idx.tree)
	case types.Type_UINT32:
		return DedupOp[uint32](vals, idx.tree)
	case types.Type_UINT64:
		return DedupOp[uint64](vals, idx.tree)
	case types.Type_DECIMAL64:
		return DedupOp[types.Decimal64](vals, idx.tree)
	case types.Type_DECIMAL128:
		return DedupOp[types.Decimal128](vals, idx.tree)
	case types.Type_FLOAT32:
		return DedupOp[float32](vals, idx.tree)
	case types.Type_FLOAT64:
		return DedupOp[float64](vals, idx.tree)
	case types.Type_DATE:
		return DedupOp[types.Date](vals, idx.tree)
	case types.Type_DATETIME:
		return DedupOp[types.Datetime](vals, idx.tree)
	case types.Type_TIMESTAMP:
		return DedupOp[types.Timestamp](vals, idx.tree)
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vals := vals.(*containers.Bytes)
		for i, s := range vals.Offset {
			e := s + vals.Length[i]
			v := string(vals.Data[s:e])
			if _, ok := idx.tree[v]; ok {
				return data.ErrDuplicate
			}
		}
	default:
		panic(fmt.Errorf("%s not supported", col.GetType().String()))
	}
	return nil
}
