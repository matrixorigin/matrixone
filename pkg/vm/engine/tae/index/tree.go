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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	art "github.com/plar/go-adaptive-radix-tree"
)

var _ SecondaryIndex = new(simpleARTMap)

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
	ikey := types.EncodeValue(key, art.typ)
	old, _ := art.tree.Insert(ikey, offset)
	if old != nil {
		art.tree.Insert(ikey, old)
		err = ErrDuplicate
	}
	return
}

func (art *simpleARTMap) BatchInsert(keys *KeysCtx, startRow uint32, upsert bool) (resp *BatchResp, err error) {
	existence := make(map[any]bool)

	op := func(v any, i int) error {
		encoded := types.EncodeValue(v, art.typ)
		if keys.NeedVerify {
			if _, found := existence[string(encoded)]; found {
				return ErrDuplicate
			}
			existence[string(encoded)] = true
		}
		old, _ := art.tree.Insert(encoded, startRow)
		if old != nil {
			// TODO: rollback previous insertion if duplication comes up
			if !upsert {
				return ErrDuplicate
			}
			if resp == nil {
				resp = new(BatchResp)
				resp.UpdatedKeys = roaring.New()
				resp.UpdatedRows = roaring.New()
			}
			resp.UpdatedRows.Add(old.(uint32))
			resp.UpdatedKeys.Add(uint32(i))
		}
		startRow++
		return nil
	}

	err = keys.Keys.ForeachWindow(keys.Start, keys.Count, op, nil)
	return
}

func (art *simpleARTMap) Update(key any, offset uint32) (err error) {
	ikey := types.EncodeValue(key, art.typ)
	old, _ := art.tree.Insert(ikey, offset)
	if old == nil {
		art.tree.Delete(ikey)
		err = ErrDuplicate
	}
	return
}

func (art *simpleARTMap) BatchUpdate(keys containers.Vector, offsets []uint32, start uint32) (err error) {
	idx := 0

	op := func(v any, _ int) error {
		encoded := types.EncodeValue(v, art.typ)
		old, _ := art.tree.Insert(encoded, offsets[idx])
		if old == nil {
			art.tree.Delete(encoded)
			return ErrDuplicate
		}
		idx++
		return nil
	}

	err = keys.Foreach(op, nil)
	return
}

func (art *simpleARTMap) Delete(key any) (old uint32, err error) {
	ikey := types.EncodeValue(key, art.typ)
	v, found := art.tree.Delete(ikey)
	if !found {
		err = ErrNotFound
	} else {
		old = v.(uint32)
	}
	return
}

func (art *simpleARTMap) Search(key any) (uint32, error) {
	ikey := types.EncodeValue(key, art.typ)
	offset, found := art.tree.Search(ikey)
	if !found {
		return 0, ErrNotFound
	}
	return offset.(uint32), nil
}

func (art *simpleARTMap) Contains(key any) bool {
	ikey := types.EncodeValue(key, art.typ)
	_, exists := art.tree.Search(ikey)
	return exists
}

// ContainsAny returns whether at least one of the specified keys exists.
//
// If the keysCtx.Selects is not nil, only the keys indicated by the keyselects bitmap will
// participate in the calculation.
// When deduplication occurs, the corresponding row number will be taken out. If the row
// number is included in the rowmask, the error will be ignored
func (art *simpleARTMap) ContainsAny(keysCtx *KeysCtx, rowmask *roaring.Bitmap) bool {
	op := func(v any, _ int) error {
		encoded := types.EncodeValue(v, art.typ)
		// 1. If duplication found
		if v, found := art.tree.Search(encoded); found {
			// 1.1 If no rowmask, quick return with duplication error
			if rowmask == nil {
				return ErrDuplicate
			}
			// 1.2 If duplicated row is marked, ignore this duplication error
			if rowmask.Contains(v.(uint32)) {
				return nil
			}
			// 1.3 If duplicated row is not marked, return with duplication error
			return ErrDuplicate
		}
		return nil
	}
	if err := keysCtx.Keys.ForeachWindow(int(keysCtx.Start), int(keysCtx.Count), op, keysCtx.Selects); err != nil {
		if err == ErrDuplicate {
			return true
		} else {
			panic(err)
		}
	}
	return false
}

func (art *simpleARTMap) String() string {
	s := fmt.Sprintf("<ART>[Size=%d](\n", art.tree.Size())
	it := art.tree.Iterator()
	for it.HasNext() {
		n, err := it.Next()
		if err != nil {
			break
		}
		s = fmt.Sprintf("%sNode: %v:%v\n", s, n.Key(), n.Value())
	}
	s = fmt.Sprintf("%s)", s)
	return s
}
