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
	"bytes"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	idata "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type TableIndex interface {
	io.Closer
	BatchDedup(*gvec.Vector) error
	BatchDedupCompound(model.CompoundKeyEncoder, ...*gvec.Vector) error
	BatchInsert(*gvec.Vector, int, int, uint32, bool) error
	BatchInsertCompound(int, int, uint32, bool, model.CompoundKeyEncoder, ...*gvec.Vector) error
	Insert(any, uint32) error
	Delete(any) error
	Search(any) (uint32, error)
	Name() string
	Count() int
	KeyToVector(types.Type) *gvec.Vector
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

func (idx *simpleTableIndex) KeyToVector(kType types.Type) *gvec.Vector {
	vec := gvec.New(kType)
	switch kType.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		for k := range idx.tree {
			compute.AppendValue(vec, []byte(k.(string)))
		}
	default:
		for k := range idx.tree {
			compute.AppendValue(vec, k)
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
		return idata.ErrDuplicate
	}
	idx.tree[v] = row
	return nil
}
func (idx *simpleTableIndex) Delete(vv any) error {
	idx.Lock()
	defer idx.Unlock()
	var v any
	switch vv.(type) {
	case []uint8:
		v = string(vv.([]uint8))
	default:
		v = vv
	}
	_, ok := idx.tree[v]
	if !ok {
		return idata.ErrNotFound
	}
	delete(idx.tree, v)
	return nil
}

func (idx *simpleTableIndex) Search(v any) (uint32, error) {
	idx.RLock()
	defer idx.RUnlock()
	row, ok := idx.tree[v]
	if !ok {
		return 0, idata.ErrNotFound
	}
	return uint32(row), nil
}

func (idx *simpleTableIndex) BatchInsertCompound(
	start int,
	count int,
	row uint32,
	dedupCol bool,
	encoder model.CompoundKeyEncoder,
	keys ...*gvec.Vector) (err error) {
	pk := gvec.New(catalog.CompoundKeyType)
	vs := make([]any, len(keys))
	var buf bytes.Buffer
	for i := 0; i < gvec.Length(keys[0]); i++ {
		buf.Reset()
		for j := range vs {
			vs[i] = compute.GetValue(keys[j], uint32(i))
		}
		v := encoder(&buf, vs...)
		compute.AppendValue(pk, v)
	}
	err = idx.BatchInsert(pk, start, count, row, dedupCol)
	return
}

func (idx *simpleTableIndex) BatchInsert(col *gvec.Vector, start, count int, row uint32, dedupCol bool) error {
	idx.Lock()
	defer idx.Unlock()
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		if dedupCol {
			set := make(map[int8]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_int16:
		data := vals.([]int16)
		if dedupCol {
			set := make(map[int16]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_int32:
		data := vals.([]int32)
		if dedupCol {
			set := make(map[int32]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_int64:
		data := vals.([]int64)
		if dedupCol {
			set := make(map[int64]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_uint8:
		data := vals.([]uint8)
		if dedupCol {
			set := make(map[uint8]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_uint16:
		data := vals.([]uint16)
		if dedupCol {
			set := make(map[uint16]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_uint32:
		data := vals.([]uint32)
		if dedupCol {
			set := make(map[uint32]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_uint64:
		data := vals.([]uint64)
		if dedupCol {
			set := make(map[uint64]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_decimal64:
		data := vals.([]types.Decimal64)
		if dedupCol {
			set := make(map[types.Decimal64]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_float32:
		data := vals.([]float32)
		if dedupCol {
			set := make(map[float32]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_float64:
		data := vals.([]float64)
		if dedupCol {
			set := make(map[float64]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_date:
		data := vals.([]types.Date)
		if dedupCol {
			set := make(map[types.Date]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_datetime:
		data := vals.([]types.Datetime)
		if dedupCol {
			set := make(map[types.Datetime]bool)
			for _, v := range data[start : start+count] {
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for _, v := range data[start : start+count] {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	case types.T_char, types.T_varchar, types.T_json:
		data := vals.(*types.Bytes)
		if dedupCol {
			set := make(map[string]bool)
			for i, s := range data.Offsets[start : start+count] {
				e := s + data.Lengths[i+start]
				v := string(data.Data[s:e])
				if _, ok := set[v]; ok {
					return idata.ErrDuplicate
				}
				set[v] = true
			}
			break
		}
		for i, s := range data.Offsets[start : start+count] {
			e := s + data.Lengths[i+start]
			v := string(data.Data[s:e])
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
			idx.tree[v] = row
			row++
		}
	default:
		return vector.ErrVecTypeNotSupport
	}
	return nil
}

func (idx *simpleTableIndex) BatchDedupCompound(encoder model.CompoundKeyEncoder, cols ...*gvec.Vector) (err error) {
	key := gvec.New(catalog.CompoundKeyType)
	vs := make([]any, len(cols))
	var buf bytes.Buffer
	for i := 0; i < gvec.Length(cols[0]); i++ {
		buf.Reset()
		for j := range vs {
			vs[i] = compute.GetValue(cols[j], uint32(i))
		}
		v := encoder(&buf, vs...)
		compute.AppendValue(key, v)
	}
	err = idx.BatchDedup(key)
	return
}

// TODO: rewrite
func (idx *simpleTableIndex) BatchDedup(col *gvec.Vector) error {
	idx.RLock()
	defer idx.RUnlock()
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_int16:
		data := vals.([]int16)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_int32:
		data := vals.([]int32)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_int64:
		data := vals.([]int64)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_uint8:
		data := vals.([]uint8)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_uint16:
		data := vals.([]uint16)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_uint32:
		data := vals.([]uint32)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_uint64:
		data := vals.([]uint64)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_decimal64:
		data := vals.([]types.Decimal64)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_float32:
		data := vals.([]float32)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_float64:
		data := vals.([]float64)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_date:
		data := vals.([]types.Date)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_datetime:
		data := vals.([]types.Datetime)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	case types.T_char, types.T_varchar, types.T_json:
		data := vals.(*types.Bytes)
		// bytes := make([]string, 0, len(data.Lengths))
		for i, s := range data.Offsets {
			e := s + data.Lengths[i]
			v := string(data.Data[s:e])
			// bytes = append(bytes, v)
			if _, ok := idx.tree[v]; ok {
				return idata.ErrDuplicate
			}
		}
	default:
		return vector.ErrVecTypeNotSupport
	}
	return nil
}
