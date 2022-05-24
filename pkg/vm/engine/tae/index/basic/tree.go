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

package basic

import (
	"strconv"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	art "github.com/plar/go-adaptive-radix-tree"
)

type ARTMap interface {
	Insert(key any, offset uint32) error
	BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error
	Update(key any, offset uint32) error
	BatchUpdate(keys *vector.Vector, offsets []uint32, start uint32) error
	Delete(key any) error
	Search(key any) (uint32, error)
	Contains(key any) (bool, error)
	ContainsAny(keys *vector.Vector, visibility *roaring.Bitmap) (bool, error)
	Print() string
	Freeze() *vector.Vector
}

type simpleARTMap struct {
	typ  types.Type
	tree art.Tree
}

func NewSimpleARTMap(typ types.Type) ARTMap {
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
		err = data.ErrDuplicate
	}
	return
}

func (art *simpleARTMap) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) (err error) {
	existence := make(map[any]bool)

	processor := func(v any) error {
		encoded, err := compute.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		if verify {
			if _, found := existence[string(encoded)]; found {
				return data.ErrDuplicate
			}
			existence[string(encoded)] = true
		}
		old, _ := art.tree.Insert(encoded, offset)
		if old != nil {
			// TODO: rollback previous insertion if duplication comes up
			return data.ErrDuplicate
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
		err = data.ErrDuplicate
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
			return data.ErrDuplicate
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
		err = data.ErrNotFound
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
		return 0, data.ErrNotFound
	}
	return offset.(uint32), nil
}

func (art *simpleARTMap) Contains(key any) (bool, error) {
	ikey, err := compute.EncodeKey(key, art.typ)
	if err != nil {
		return false, err
	}
	_, exists := art.tree.Search(ikey)
	if exists {
		return true, nil
	}
	return false, nil
}

func (art *simpleARTMap) ContainsAny(keys *vector.Vector, visibility *roaring.Bitmap) (bool, error) {
	processor := func(v any) error {
		encoded, err := compute.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		if _, found := art.tree.Search(encoded); found {
			return data.ErrDuplicate
		}
		return nil
	}
	if err := compute.ProcessVector(keys, 0, -1, processor, visibility); err != nil {
		if err == data.ErrDuplicate {
			return true, nil
		} else {
			return false, err
		}
	}
	return false, nil
}

func (art *simpleARTMap) Print() string {
	min, _ := art.tree.Minimum()
	max, _ := art.tree.Maximum() // TODO: seems not accurate here
	return "<ART>\n" + "[" + strconv.Itoa(int(min.(uint32))) + ", " + strconv.Itoa(int(max.(uint32))) + "]" + "(" + strconv.Itoa(art.tree.Size()) + ")"
}

func (art *simpleARTMap) Freeze() *vector.Vector {
	// TODO: support all types
	iter := art.tree.Iterator()
	vec := vector.New(art.typ)
	keys := make([]int32, 0)
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			panic(err)
		}
		key := compute.DecodeKey(node.Key(), art.typ).(int32)
		keys = append(keys, key)
	}
	err := vector.Append(vec, keys)
	if err != nil {
		panic(err)
	}
	return vec
}
