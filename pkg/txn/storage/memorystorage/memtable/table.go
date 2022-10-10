// Copyright 2022 Matrix Origin
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

package memtable

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Table[
	K Ordered[K],
	V any,
	R Row[K, V],
] struct {
	sync.Mutex
	piece atomic.Pointer[Piece[K, V]]
}

type Ordered[To any] interface {
	Less(to To) bool
}

func NewTable[
	K Ordered[K],
	V any,
	R Row[K, V],
]() *Table[K, V, R] {
	ret := &Table[K, V, R]{}
	tree := NewTree[K, V, R]()
	piece := (Piece[K, V])(tree)
	ret.piece.Store(&piece)
	return ret
}

func (t *Table[K, V, R]) Insert(
	tx *Transaction,
	row R,
) error {
	return t.update(func(piece Piece[K, V]) error {
		mutablePiece, ok := (any)(piece).(MutablePiece[K, V, R])
		if !ok {
			panic(fmt.Sprintf("%T is not a MutablePiece", piece))
		}
		return mutablePiece.Insert(tx, row)
	})
}

func (t *Table[K, V, R]) Update(
	tx *Transaction,
	row R,
) error {
	return t.update(func(piece Piece[K, V]) error {
		mutablePiece, ok := (any)(piece).(MutablePiece[K, V, R])
		if !ok {
			panic(fmt.Sprintf("%T is not a MutablePiece", piece))
		}
		return mutablePiece.Update(tx, row)
	})
}

func (t *Table[K, V, R]) Delete(
	tx *Transaction,
	key K,
) error {
	return t.update(func(piece Piece[K, V]) error {
		mutablePiece, ok := (any)(piece).(MutablePiece[K, V, R])
		if !ok {
			panic(fmt.Sprintf("%T is not a MutablePiece", piece))
		}
		return mutablePiece.Delete(tx, key)
	})
}

func (t *Table[K, V, R]) Get(
	tx *Transaction,
	key K,
) (
	value V,
	err error,
) {
	ptr := t.piece.Load()
	return (*ptr).Get(tx, key)
}

func (t *Table[K, V, R]) Index(tx *Transaction, index Tuple) (entries []*IndexEntry[K, V], err error) {
	ptr := t.piece.Load()
	indexedPiece, ok := (any)(*ptr).(IndexedPiece[K, V])
	if !ok {
		panic(fmt.Sprintf("%T is not an IndexedPiece", *ptr))
	}
	return indexedPiece.Index(tx, index)
}

func (t *Table[K, V, R]) CommitTx(tx *Transaction) error {
	return t.update(func(piece Piece[K, V]) error {
		mutablePiece, ok := (any)(piece).(MutablePiece[K, V, R])
		if !ok {
			panic(fmt.Sprintf("%T is not a MutablePiece", piece))
		}
		return mutablePiece.CommitTx(tx)
	})
}

func (t *Table[K, V, R]) AbortTx(tx *Transaction) {
	t.update(func(piece Piece[K, V]) error {
		mutablePiece, ok := (any)(piece).(MutablePiece[K, V, R])
		if !ok {
			panic(fmt.Sprintf("%T is not a MutablePiece", piece))
		}
		mutablePiece.AbortTx(tx)
		return nil
	})
}

func (t *Table[K, V, R]) update(
	fn func(piece Piece[K, V]) error,
) error {
	t.Lock()
	defer t.Unlock()
	ptr := t.piece.Load()
	newPiece := (*ptr).Copy()
	if err := fn(newPiece); err != nil {
		return err
	}
	t.piece.Store(&newPiece)
	return nil
}

func (t *Table[K, V, R]) NewIter(tx *Transaction) Iter[K, V] {
	ptr := t.piece.Load()
	return (*ptr).NewIter(tx)
}

func (t *Table[K, V, R]) NewDiffIter(fromTime, toTime *Time) DiffIter[K, V] {
	ptr := t.piece.Load()
	diffIterPiece, ok := (any)(*ptr).(DiffIterPiece[K, V])
	if !ok {
		panic(fmt.Sprintf("%T is not an DiffIterPiece", *ptr))
	}
	return diffIterPiece.NewDiffIter(fromTime, toTime)
}
