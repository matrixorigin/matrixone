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
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	t *types.Type,
	attr string,
	vs []T,
	tree map[any]uint32) (err error) {
	for _, v := range vs {
		if _, ok := tree[v]; ok {
			entry := common.TypeStringValue(*t, v, false)
			return moerr.NewDuplicateEntryNoCtx(entry, attr)
		}
	}
	return
}

func InsertOp[T comparable](
	t *types.Type,
	attr string,
	vals []T,
	start, count int,
	fromRow uint32,
	dedupInput bool,
	tree map[any]uint32) (err error) {
	if dedupInput {
		set := make(map[T]bool)
		for _, v := range vals[start : start+count] {
			if _, ok := set[v]; ok {
				entry := common.TypeStringValue(*t, v, false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			set[v] = true
		}
		return
	}
	for _, v := range vals[start : start+count] {
		if _, ok := tree[v]; ok {
			entry := common.TypeStringValue(*t, v, false)
			return moerr.NewDuplicateEntryNoCtx(entry, attr)
		}
		tree[v] = fromRow
		fromRow++
	}
	return
}

func (idx *simpleTableIndex) KeyToVector(kType types.Type) containers.Vector {
	vec := makeWorkspaceVector(kType)
	switch kType.Oid {
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		for k := range idx.tree {
			vec.Append([]byte(k.(string)), false)
		}
	case types.T_array_float32, types.T_array_float64:
		// No usage for this func.
		for k := range idx.tree {
			vec.Append(k.([]byte), false)
		}
	default:
		for k := range idx.tree {
			vec.Append(k, false)
		}
	}
	return vec
}

func (idx *simpleTableIndex) KeyToVectors(kType types.Type) []containers.Vector {
	vec := makeWorkspaceVector(kType)
	var vecs []containers.Vector
	switch kType.Oid {
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		for k := range idx.tree {
			if vec.Length() > int(MaxNodeRows) {
				vecs = append(vecs, vec)
				vec = makeWorkspaceVector(kType)
			}
			vec.Append([]byte(k.(string)), false)
		}
	case types.T_array_float32:
		// No usage for this func.
		for k := range idx.tree {
			if vec.Length() > int(MaxNodeRows) {
				vecs = append(vecs, vec)
				vec = makeWorkspaceVector(kType)
			}
			vec.Append(types.BytesToArrayToString[float32](k.([]byte)), false)
		}
	case types.T_array_float64:
		for k := range idx.tree {
			if vec.Length() > int(MaxNodeRows) {
				vecs = append(vecs, vec)
				vec = makeWorkspaceVector(kType)
			}
			vec.Append(types.BytesToArrayToString[float64](k.([]byte)), false)
		}
	default:
		for k := range idx.tree {
			if vec.Length() > int(MaxNodeRows) {
				vecs = append(vecs, vec)
				vec = makeWorkspaceVector(kType)
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
		vs := vector.MustFixedCol[bool](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_bit:
		vs := vector.MustFixedCol[uint64](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_int8:
		vs := vector.MustFixedCol[int8](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_int16:
		vs := vector.MustFixedCol[int16](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_int32:
		vs := vector.MustFixedCol[int32](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_int64:
		vs := vector.MustFixedCol[int64](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_uint8:
		vs := vector.MustFixedCol[uint8](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_uint16:
		vs := vector.MustFixedCol[uint16](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_uint32:
		vs := vector.MustFixedCol[uint32](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_uint64:
		vs := vector.MustFixedCol[uint64](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_decimal64:
		vs := vector.MustFixedCol[types.Decimal64](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_decimal128:
		vs := vector.MustFixedCol[types.Decimal128](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_uuid:
		vs := vector.MustFixedCol[types.Uuid](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_float32:
		vs := vector.MustFixedCol[float32](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_float64:
		vs := vector.MustFixedCol[float64](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_date:
		vs := vector.MustFixedCol[types.Date](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_timestamp:
		vs := vector.MustFixedCol[types.Timestamp](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_time:
		vs := vector.MustFixedCol[types.Time](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_datetime:
		vs := vector.MustFixedCol[types.Datetime](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_enum:
		vs := vector.MustFixedCol[types.Enum](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_TS:
		vs := vector.MustFixedCol[types.TS](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_Rowid:
		vs := vector.MustFixedCol[types.Rowid](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_Blockid:
		vs := vector.MustFixedCol[types.Blockid](col.GetDownstreamVector())
		return InsertOp(colType, attr, vs, start, count, row, dedupInput, idx.tree)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		vec := col.GetDownstreamVector()
		if dedupInput {
			set := make(map[string]bool)
			for i := start; i < start+count; i++ {
				v := vec.UnsafeGetStringAt(i)
				if _, ok := set[v]; ok {
					entry := common.TypeStringValue(*colType, []byte(v), false)
					return moerr.NewDuplicateEntryNoCtx(entry, attr)
				}
				set[v] = true
			}
			break
		}
		for i := start; i < start+count; i++ {
			v := vec.UnsafeGetStringAt(i)
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(*colType, []byte(v), false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			idx.tree[v] = row
			row++
		}
	case types.T_array_float32:
		vec := col.GetDownstreamVector()
		if dedupInput {
			set := make(map[string]bool)
			for i := start; i < start+count; i++ {
				v := types.ArrayToString[float32](vector.GetArrayAt[float32](vec, i))
				if _, ok := set[v]; ok {
					entry := common.TypeStringValue(*colType, vec.GetBytesAt(i), false)
					return moerr.NewDuplicateEntryNoCtx(entry, attr)
				}
				set[v] = true
			}
			break
		}
		for i := start; i < start+count; i++ {
			v := types.ArrayToString[float32](vector.GetArrayAt[float32](vec, i))
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(*colType, vec.GetBytesAt(i), false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			idx.tree[v] = row
			row++
		}
	case types.T_array_float64:
		vec := col.GetDownstreamVector()
		if dedupInput {
			set := make(map[string]bool)
			for i := start; i < start+count; i++ {
				v := types.ArrayToString[float64](vector.GetArrayAt[float64](vec, i))
				if _, ok := set[v]; ok {
					entry := common.TypeStringValue(*colType, vec.GetBytesAt(i), false)
					return moerr.NewDuplicateEntryNoCtx(entry, attr)
				}
				set[v] = true
			}
			break
		}
		for i := start; i < start+count; i++ {
			v := types.ArrayToString[float64](vector.GetArrayAt[float64](vec, i))
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(*colType, vec.GetBytesAt(i), false)
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
		vals := vector.MustFixedCol[bool](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_bit:
		vals := vector.MustFixedCol[uint64](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_int8:
		vals := vector.MustFixedCol[int8](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_int16:
		vals := vector.MustFixedCol[int16](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_int32:
		vals := vector.MustFixedCol[int32](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_int64:
		vals := vector.MustFixedCol[int64](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_uint8:
		vals := vector.MustFixedCol[uint8](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_uint16:
		vals := vector.MustFixedCol[uint16](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_uint32:
		vals := vector.MustFixedCol[uint32](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_uint64:
		vals := vector.MustFixedCol[uint64](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_decimal64:
		vals := vector.MustFixedCol[types.Decimal64](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_decimal128:
		vals := vector.MustFixedCol[types.Decimal128](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_float32:
		vals := vector.MustFixedCol[float32](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_float64:
		vals := vector.MustFixedCol[float64](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_date:
		vals := vector.MustFixedCol[types.Date](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_time:
		vals := vector.MustFixedCol[types.Time](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_datetime:
		vals := vector.MustFixedCol[types.Datetime](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_timestamp:
		vals := vector.MustFixedCol[types.Timestamp](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_enum:
		vals := vector.MustFixedCol[types.Enum](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_TS:
		vals := vector.MustFixedCol[types.TS](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_Rowid:
		vals := vector.MustFixedCol[types.Rowid](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_Blockid:
		vals := vector.MustFixedCol[types.Blockid](col.GetDownstreamVector())
		return DedupOp(colType, attr, vals, idx.tree)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		vec := col.GetDownstreamVector()
		for i := 0; i < col.Length(); i++ {
			bs := vec.GetBytesAt(i)
			v := util.UnsafeBytesToString(bs)
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(*colType, bs, false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
		}
	case types.T_array_float32:
		vec := col.GetDownstreamVector()
		for i := 0; i < col.Length(); i++ {
			bs := vec.GetBytesAt(i)
			v := types.BytesToArrayToString[float32](bs)
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(*colType, bs, false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
		}
	case types.T_array_float64:
		vec := col.GetDownstreamVector()
		for i := 0; i < col.Length(); i++ {
			bs := vec.GetBytesAt(i)
			v := types.BytesToArrayToString[float64](bs)
			if _, ok := idx.tree[v]; ok {
				entry := common.TypeStringValue(*colType, bs, false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
		}
	default:
		panic(moerr.NewInternalErrorNoCtx("%s not supported", col.GetType().String()))
	}
	return nil
}
