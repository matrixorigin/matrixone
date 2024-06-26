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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	art "github.com/plar/go-adaptive-radix-tree"
)

var _ SecondaryIndex = new(simpleARTMap)

type Positions []uint32

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
	positions := Positions{offset}
	old, _ := art.tree.Insert(key, positions)
	if old != nil {
		oldPositions := old.(Positions)
		oldPositions = append(oldPositions, offset)
		art.tree.Insert(key, oldPositions)
	}
	return
}

func (art *simpleARTMap) BatchInsert(
	keys *vector.Vector,
	offset, length int,
	startRow uint32,
) (err error) {
	op := func(v []byte, _ bool, i int) error {
		positions := Positions{startRow}
		old, _ := art.tree.Insert(v, positions)
		if old != nil {
			oldPositions := old.(Positions)
			oldPositions = append(oldPositions, startRow)
			art.tree.Insert(v, oldPositions)
		}
		startRow++
		return nil
	}
	err = containers.ForeachWindowBytes(keys, offset, length, op, nil)
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
	positions := v.(Positions)
	return positions, nil
}

func (art *simpleARTMap) String() string {
	s := fmt.Sprintf("<ART>[Size=%d](\n", art.tree.Size())
	it := art.tree.Iterator()
	for it.HasNext() {
		n, err := it.Next()
		if err != nil {
			break
		}
		s = fmt.Sprintf("%sNode: %v:%v\n", s, n.Key(), n.Value().(Positions))
	}
	s = fmt.Sprintf("%s)", s)
	return s
}
