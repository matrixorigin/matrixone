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
	tree art.Tree
}

func NewSimpleARTMap() *simpleARTMap {
	return &simpleARTMap{
		tree: art.New(),
	}
}

func (art *simpleARTMap) Size() int { return art.tree.Size() }

func (art *simpleARTMap) Insert(key []byte, offset uint32) (err error) {
	chain := NewIndexMVCCChain()
	chain.Insert(offset)
	old, _ := art.tree.Insert(key, chain)
	if old != nil {
		oldChain := old.(*IndexMVCCChain)
		oldChain.Insert(offset)
		art.tree.Insert(key, old)
	}
	return
}

func (art *simpleARTMap) BatchInsert(keys *KeysCtx, startRow uint32) (err error) {
	existence := make(map[any]bool)
	op := func(v []byte, _ bool, i int) error {
		if keys.NeedVerify {
			if _, found := existence[string(v)]; found {
				return ErrDuplicate
			}
			existence[string(v)] = true
		}
		chain := NewIndexMVCCChain()
		chain.Insert(startRow)
		old, _ := art.tree.Insert(v, chain)
		if old != nil {
			oldChain := old.(*IndexMVCCChain)
			oldChain.Insert(startRow)
			art.tree.Insert(v, old)
		}
		startRow++
		return nil
	}
	err = containers.ForeachWindowBytes(keys.Keys, keys.Start, keys.Count, op, nil)
	return
}

func (art *simpleARTMap) Delete(key any) (old uint32, err error) {
	return
}

func (art *simpleARTMap) Search(key []byte) ([]uint32, error) {
	v, found := art.tree.Search(key)
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
