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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

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

func DedupOp[T comparable](
	t types.Type,
	attr string,
	vs any,
	tree map[any]uint32) (err error) {
	vals := vs.([]T)
	for _, v := range vals {
		if _, ok := tree[v]; ok {
			entry := common.TypeStringValue(t, v, false)
			return moerr.NewDuplicateEntryNoCtx(entry, attr)
		}
	}
	return
}

func InsertOp[T comparable](
	t types.Type,
	attr string,
	input any,
	start, count int,
	fromRow uint32,
	dedupInput bool,
	tree map[any]uint32) (err error) {
	vals := input.([]T)
	if dedupInput {
		set := make(map[T]bool)
		for _, v := range vals[start : start+count] {
			if _, ok := set[v]; ok {
				entry := common.TypeStringValue(t, v, false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			set[v] = true
		}
		return
	}
	for _, v := range vals[start : start+count] {
		if _, ok := tree[v]; ok {
			entry := common.TypeStringValue(t, v, false)
			return moerr.NewDuplicateEntryNoCtx(entry, attr)
		}
		tree[v] = fromRow
		fromRow++
	}
	return
}

func (idx *simpleTableIndex) KeyToVector(kType types.Type) containers.Vector {
	vec := containers.MakeVector(kType)
	switch kType.Oid {
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		for k := range idx.tree {
			vec.Append([]byte(k.(string)), false)
		}
	default:
		for k := range idx.tree {
			vec.Append(k, false)
		}
	}
	return vec
}

func (idx *simpleTableIndex) KeyToVectors(kType types.Type) []containers.Vector {
	vec := containers.MakeVector(kType)
	var vecs []containers.Vector
	switch kType.Oid {
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		for k := range idx.tree {
			if vec.Length() > int(txnbase.MaxNodeRows) {
				vecs = append(vecs, vec)
				vec = containers.MakeVector(kType)
			}
			vec.Append([]byte(k.(string)), false)
		}
	default:
		for k := range idx.tree {
			if vec.Length() > int(txnbase.MaxNodeRows) {
				vecs = append(vecs, vec)
				vec = containers.MakeVector(kType)
			}
			vec.Append(k, false)
		}
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
		return 0, moerr.NewNotFoundNoCtx()
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
	colType := col.GetType()
	switch colType.Oid {
	case types.T_bool:
		return InsertOp[bool](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_int8:
		return InsertOp[int8](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_int16:
		return InsertOp[int16](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_int32:
		return InsertOp[int32](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_int64:
		return InsertOp[int64](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_uint8:
		return InsertOp[uint8](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_uint16:
		return InsertOp[uint16](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_uint32:
		return InsertOp[uint32](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_uint64:
		return InsertOp[uint64](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_decimal64:
		return InsertOp[types.Decimal64](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_decimal128:
		return InsertOp[types.Decimal128](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_uuid:
		return InsertOp[types.Uuid](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_float32:
		return InsertOp[float32](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_float64:
		return InsertOp[float64](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_date:
		return InsertOp[types.Date](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_timestamp:
		return InsertOp[types.Timestamp](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_time:
		return InsertOp[types.Time](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_datetime:
		return InsertOp[types.Datetime](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_TS:
		return InsertOp[types.TS](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_Rowid:
		return InsertOp[types.Rowid](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_Blockid:
		return InsertOp[types.Blockid](colType, attr, col.Slice(), start, count, row, dedupInput, idx.tree)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		vs := col
		if dedupInput {
			set := make(map[string]bool)
			for i := start; i < start+count; i++ {
				v := string(vs.Get(i).([]byte))
				if _, ok := set[v]; ok {
					entry := common.TypeStringValue(colType, []byte(v), false)
					return moerr.NewDuplicateEntryNoCtx(entry, attr)
				}
				set[v] = true
			}
			break
		}
		for i := start; i < start+count; i++ {
			v := string(vs.Get(i).([]byte))
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(colType, []byte(v), false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			idx.tree[v] = row
			row++
		}
	default:
		panic(moerr.NewInternalErrorNoCtx("%s not supported", col.GetType().String()))
	}
	return nil
}

// TODO: rewrite
func (idx *simpleTableIndex) BatchDedup(attr string, col containers.Vector) error {
	idx.RLock()
	defer idx.RUnlock()
	colType := col.GetType()
	switch colType.Oid {
	case types.T_bool:
		vals := col.Slice()
		return DedupOp[bool](colType, attr, vals, idx.tree)
	case types.T_int8:
		vals := col.Slice()
		return DedupOp[int8](colType, attr, vals, idx.tree)
	case types.T_int16:
		vals := col.Slice()
		return DedupOp[int16](colType, attr, vals, idx.tree)
	case types.T_int32:
		vals := col.Slice()
		return DedupOp[int32](colType, attr, vals, idx.tree)
	case types.T_int64:
		vals := col.Slice()
		return DedupOp[int64](colType, attr, vals, idx.tree)
	case types.T_uint8:
		vals := col.Slice()
		return DedupOp[uint8](colType, attr, vals, idx.tree)
	case types.T_uint16:
		vals := col.Slice()
		return DedupOp[uint16](colType, attr, vals, idx.tree)
	case types.T_uint32:
		vals := col.Slice()
		return DedupOp[uint32](colType, attr, vals, idx.tree)
	case types.T_uint64:
		vals := col.Slice()
		return DedupOp[uint64](colType, attr, vals, idx.tree)
	case types.T_decimal64:
		vals := col.Slice()
		return DedupOp[types.Decimal64](colType, attr, vals, idx.tree)
	case types.T_decimal128:
		vals := col.Slice()
		return DedupOp[types.Decimal128](colType, attr, vals, idx.tree)
	case types.T_float32:
		vals := col.Slice()
		return DedupOp[float32](colType, attr, vals, idx.tree)
	case types.T_float64:
		vals := col.Slice()
		return DedupOp[float64](colType, attr, vals, idx.tree)
	case types.T_date:
		vals := col.Slice()
		return DedupOp[types.Date](colType, attr, vals, idx.tree)
	case types.T_time:
		vals := col.Slice()
		return DedupOp[types.Time](colType, attr, vals, idx.tree)
	case types.T_datetime:
		vals := col.Slice()
		return DedupOp[types.Datetime](colType, attr, vals, idx.tree)
	case types.T_timestamp:
		vals := col.Slice()
		return DedupOp[types.Timestamp](colType, attr, vals, idx.tree)
	case types.T_TS:
		vals := col.Slice()
		return DedupOp[types.TS](colType, attr, vals, idx.tree)
	case types.T_Rowid:
		vals := col.Slice()
		return DedupOp[types.Rowid](colType, attr, vals, idx.tree)
	case types.T_Blockid:
		vals := col.Slice()
		return DedupOp[types.Blockid](colType, attr, vals, idx.tree)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		bs := col
		for i := 0; i < col.Length(); i++ {
			v := string(bs.Get(i).([]byte))
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(colType, []byte(v), false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
		}
	default:
		panic(moerr.NewInternalErrorNoCtx("%s not supported", col.GetType().String()))
	}
	return nil
}
