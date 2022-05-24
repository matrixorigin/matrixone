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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
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

func (art *simpleARTMap) Insert(key any, offset uint32) (err error) {
	ikey, err := compute.EncodeKey(key, art.typ)
	if err != nil {
		return
	}
	old, _ := art.tree.Insert(ikey, offset)
	if old != nil {
		art.tree.Insert(ikey, old)
		err = ErrDuplicate
	}
	return
}

func (art *simpleARTMap) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify, upsert bool) (err error) {
	existence := make(map[any]bool)

	processor := func(v any) error {
		encoded, err := compute.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		if verify {
			if _, found := existence[string(encoded)]; found {
				return ErrDuplicate
			}
			existence[string(encoded)] = true
		}
		old, _ := art.tree.Insert(encoded, offset)
		if old != nil {
			// TODO: rollback previous insertion if duplication comes up
			if !upsert {
				return ErrDuplicate
			}
		}
		offset++
		return nil
	}

	err = compute.ProcessVector(keys, uint32(start), -1, processor, nil)
	return
}

func (art *simpleARTMap) Update(key any, offset uint32) (err error) {
	ikey, err := compute.EncodeKey(key, art.typ)
	if err != nil {
		return
	}
	old, _ := art.tree.Insert(ikey, offset)
	if old == nil {
		art.tree.Delete(ikey)
		err = ErrDuplicate
	}
	return
}

func (art *simpleARTMap) BatchUpdate(keys *vector.Vector, offsets []uint32, start uint32) (err error) {
	idx := 0

	processor := func(v any) error {
		encoded, err := compute.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		old, _ := art.tree.Insert(encoded, offsets[idx])
		if old == nil {
			art.tree.Delete(encoded)
			return ErrDuplicate
		}
		idx++
		return nil
	}

	err = compute.ProcessVector(keys, 0, -1, processor, nil)
	return
}

func (art *simpleARTMap) Delete(key any) (err error) {
	ikey, err := compute.EncodeKey(key, art.typ)
	if err != nil {
		return
	}
	_, found := art.tree.Delete(ikey)
	if !found {
		err = ErrNotFound
	}
	return
}

func (art *simpleARTMap) Search(key any) (uint32, error) {
	ikey, err := compute.EncodeKey(key, art.typ)
	if err != nil {
		return 0, err
	}
	offset, found := art.tree.Search(ikey)
	if !found {
		return 0, ErrNotFound
	}
	return offset.(uint32), nil
}

func (art *simpleARTMap) Contains(key any) bool {
	ikey, err := compute.EncodeKey(key, art.typ)
	if err != nil {
		panic(err)
	}
	_, exists := art.tree.Search(ikey)
	return exists
}

// 1. keys: keys to check
// 2. visibility: specify which key in keys to check
// 3. mask: row mask
func (art *simpleARTMap) ContainsAny(keys *vector.Vector, visibility, mask *roaring.Bitmap) bool {
	processor := func(v any) error {
		encoded, err := compute.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		if v, found := art.tree.Search(encoded); found {
			if mask == nil {
				return ErrDuplicate
			}
			if mask.Contains(v.(uint32)) {
				return nil
			}
			return ErrDuplicate
		}
		return nil
	}
	if err := compute.ProcessVector(keys, 0, -1, processor, visibility); err != nil {
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
		// key, _ := compute.EncodeKey(n.Key(), art.typ)
		s = fmt.Sprintf("%sNode: %v:%v\n", s, n.Key(), n.Value())
	}
	s = fmt.Sprintf("%s)", s)
	return s
}

// func (art *simpleARTMap) Freeze() *vector.Vector {
// 	// TODO: support all types
// 	iter := art.tree.Iterator()
// 	vec := vector.New(art.typ)
// 	keys := make([]int32, 0)
// 	for iter.HasNext() {
// 		node, err := iter.Next()
// 		if err != nil {
// 			panic(err)
// 		}
// 		key := compute.DecodeKey(node.Key(), art.typ).(int32)
// 		keys = append(keys, key)
// 	}
// 	err := vector.Append(vec, keys)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return vec
// }
