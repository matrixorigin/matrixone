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
	ikey := types.EncodeValue(key, art.typ)
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
	op := func(v any, i int) error {
		encoded := types.EncodeValue(v, art.typ)
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

	err = keys.Keys.ForeachWindowShallow(keys.Start, keys.Count, op, nil)
	return
}

func (art *simpleARTMap) Delete(key any) (old uint32, err error) {
	return
}

func (art *simpleARTMap) Search(key any) ([]uint32, error) {
	ikey := types.EncodeValue(key, art.typ)
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
