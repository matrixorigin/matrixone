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

package batch

import (
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	_ IBatch = (*Batch)(nil)
)

func NewBatch(attrs []int, vecs []vector.IVector) IBatch {
	if len(attrs) != len(vecs) || len(vecs) == 0 {
		panic("logic error")
	}
	bat := &Batch{
		ClosedMask: roaring.NewBitmap(),
		Vecs:       vecs,
		Attrs:      attrs,
		AttrsMap:   make(map[int]int),
	}

	for i, idx := range attrs {
		bat.AttrsMap[idx] = i
	}
	if len(bat.AttrsMap) != len(attrs) {
		panic("logic error")
	}

	return bat
}

func (bat *Batch) GetAttrs() []int {
	return bat.Attrs
}

func (bat *Batch) Length() int {
	return bat.Vecs[len(bat.Vecs)-1].Length()
}

func (bat *Batch) IsReadonly() bool {
	return bat.Vecs[len(bat.Vecs)-1].IsReadonly()
}

func (bat *Batch) GetReaderByAttr(attr int) dbi.IVectorReader {
	vec := bat.GetVectorByAttr(attr)
	if vec == nil {
		return vec
	}
	return vec.(dbi.IVectorReader)
}

func (bat *Batch) GetVectorByAttr(attr int) vector.IVector {
	pos, ok := bat.AttrsMap[attr]
	if !ok {
		panic(BatNotFoundErr.Error())
	}
	bat.RLock()
	if bat.ClosedMask.Contains(uint64(pos)) {
		bat.RUnlock()
		return nil
	}
	bat.RUnlock()
	return bat.Vecs[pos]
}

func (bat *Batch) IsVectorClosed(attr int) bool {
	pos, ok := bat.AttrsMap[attr]
	if !ok {
		panic(BatNotFoundErr.Error())
	}
	bat.RLock()
	defer bat.RUnlock()
	return bat.ClosedMask.Contains(uint64(pos))
}

func (bat *Batch) CloseVector(attr int) error {
	pos, ok := bat.AttrsMap[attr]
	if !ok {
		panic(BatNotFoundErr.Error())
	}
	bat.Lock()
	defer bat.Unlock()
	if bat.ClosedMask.Contains(uint64(pos)) {
		return BatAlreadyClosedErr
	}
	err := bat.Vecs[pos].Close()
	if err != nil {
		panic(err)
	}
	bat.ClosedMask.Add(uint64(pos))
	return nil
}

func (bat *Batch) Close() error {
	bat.Lock()
	defer bat.Unlock()
	if bat.ClosedMask.GetCardinality() == uint64(len(bat.Attrs)) {
		return nil
	}
	var err error
	for i := 0; i < len(bat.Attrs); i++ {
		if !bat.ClosedMask.Contains(uint64(i)) {
			if err = bat.Vecs[i].Close(); err != nil {
				panic(err)
			}
			bat.ClosedMask.Add(uint64(i))
		}
	}
	return nil
}
