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

package index

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	art "github.com/plar/go-adaptive-radix-tree"
)

var _ SecondaryIndex = new(simpleARTMap)

type IndexMVCCChain struct {
	MVCC []uint32
}

func NewIndexMVCCChain() *IndexMVCCChain {
	return &IndexMVCCChain{
		MVCC: make([]uint32, 0),
	}
}

func (chain *IndexMVCCChain) Insert(node uint32) {
	chain.MVCC = append(chain.MVCC, node)
}

func (chain *IndexMVCCChain) GetRows() []uint32 {
	return chain.MVCC
}

type simpleARTMap struct {
	typ  types.Type
	tree art.Tree
}

func NewSimpleARTMap(typ types.Type) *simpleARTMap {
	return &simpleARTMap{
		typ:  typ,
		tree: art.New(),
	}
}

func (art *simpleARTMap) Size() int { return art.tree.Size() }

func (art *simpleARTMap) Insert(key any, offset uint32) (err error) {
	chain := NewIndexMVCCChain()
	chain.Insert(offset)
	ikey := types.EncodeValue(key, art.typ.Oid)
	old, _ := art.tree.Insert(ikey, chain)
	if old != nil {
		oldChain := old.(*IndexMVCCChain)
		oldChain.Insert(offset)
		art.tree.Insert(ikey, old)
	}
	return
}

func (art *simpleARTMap) BatchInsert(keys *KeysCtx, startRow uint32) (err error) {
	existence := make(map[any]bool)
	op := getBatchInsertClosure(keys.Keys.GetType().Oid, art, keys, existence, startRow)
	err = containers.ForeachVectorWindow(keys.Keys, keys.Start, keys.Count, op, nil)
	return
}

func (art *simpleARTMap) Delete(key any) (old uint32, err error) {
	return
}

func (art *simpleARTMap) Search(key any) ([]uint32, error) {
	ikey := types.EncodeValue(key, art.typ.Oid)
	v, found := art.tree.Search(ikey)
	if !found {
		return nil, ErrNotFound
	}
	chain := v.(*IndexMVCCChain)
	return chain.GetRows(), nil
}

func (art *simpleARTMap) String() string {
	s := fmt.Sprintf("<ART>[Size=%d](\n", art.tree.Size())
	it := art.tree.Iterator()
	for it.HasNext() {
		n, err := it.Next()
		if err != nil {
			break
		}
		s = fmt.Sprintf("%sNode: %v:%v\n", s, n.Key(), n.Value().(*IndexMVCCChain).GetRows())
	}
	s = fmt.Sprintf("%s)", s)
	return s
}

func getBatchInsertClosure(
	typ types.T,
	art *simpleARTMap,
	keys *KeysCtx,
	existence map[any]bool,
	startRow uint32) any {
	switch typ {
	case types.T_bool:
		return batchInsertClosureFactory[bool](art, keys, existence, startRow)
	case types.T_int8:
		return batchInsertClosureFactory[int8](art, keys, existence, startRow)
	case types.T_int16:
		return batchInsertClosureFactory[int16](art, keys, existence, startRow)
	case types.T_int32:
		return batchInsertClosureFactory[int32](art, keys, existence, startRow)
	case types.T_int64:
		return batchInsertClosureFactory[int64](art, keys, existence, startRow)
	case types.T_uint8:
		return batchInsertClosureFactory[uint8](art, keys, existence, startRow)
	case types.T_uint16:
		return batchInsertClosureFactory[uint16](art, keys, existence, startRow)
	case types.T_uint32:
		return batchInsertClosureFactory[uint32](art, keys, existence, startRow)
	case types.T_uint64:
		return batchInsertClosureFactory[uint64](art, keys, existence, startRow)
	case types.T_float32:
		return batchInsertClosureFactory[float32](art, keys, existence, startRow)
	case types.T_float64:
		return batchInsertClosureFactory[float64](art, keys, existence, startRow)
	case types.T_timestamp:
		return batchInsertClosureFactory[types.Timestamp](art, keys, existence, startRow)
	case types.T_date:
		return batchInsertClosureFactory[types.Date](art, keys, existence, startRow)
	case types.T_time:
		return batchInsertClosureFactory[types.Time](art, keys, existence, startRow)
	case types.T_datetime:
		return batchInsertClosureFactory[types.Datetime](art, keys, existence, startRow)
	case types.T_decimal64:
		return batchInsertClosureFactory[types.Decimal64](art, keys, existence, startRow)
	case types.T_decimal128:
		return batchInsertClosureFactory[types.Decimal128](art, keys, existence, startRow)
	case types.T_decimal256:
		return batchInsertClosureFactory[types.Decimal256](art, keys, existence, startRow)
	case types.T_TS:
		return batchInsertClosureFactory[types.TS](art, keys, existence, startRow)
	case types.T_Rowid:
		return batchInsertClosureFactory[types.Rowid](art, keys, existence, startRow)
	case types.T_Blockid:
		return batchInsertClosureFactory[types.Blockid](art, keys, existence, startRow)
	case types.T_uuid:
		return batchInsertClosureFactory[types.Uuid](art, keys, existence, startRow)
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_json, types.T_text:
		return batchInsertClosureFactory[[]byte](art, keys, existence, startRow)
	default:
		panic("unsupport")
	}
}

func batchInsertClosureFactory[T any](
	art *simpleARTMap,
	keys *KeysCtx,
	existence map[any]bool,
	startRow uint32) func(v T, _ bool, i int) error {
	return func(v T, _ bool, i int) error {
		encoded := types.EncodeValue(v, art.typ.Oid)
		if keys.NeedVerify {
			if _, found := existence[string(encoded)]; found {
				return ErrDuplicate
			}
			existence[string(encoded)] = true
		}
		chain := NewIndexMVCCChain()
		chain.Insert(startRow)
		old, _ := art.tree.Insert(encoded, chain)
		if old != nil {
			oldChain := old.(*IndexMVCCChain)
			oldChain.Insert(startRow)
			art.tree.Insert(encoded, old)
		}
		startRow++
		return nil
	}
}
