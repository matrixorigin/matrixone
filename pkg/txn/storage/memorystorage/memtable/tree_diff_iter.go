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
	"database/sql"

	"github.com/tidwall/btree"
)

var _ DiffIterPiece[dumbRow, dumbRow] = new(Tree[dumbRow, dumbRow, dumbRow])

func (t *Tree[K, V, R]) NewDiffIter(fromTime, toTime *Time) DiffIter[K, V] {
	ret := &TreeDiffIter[K, V]{
		fromTime: fromTime,
		toTime:   toTime,
		iter:     t.rows.Copy().Iter(),
	}
	return ret
}

type TreeDiffIter[
	K Ordered[K],
	V any,
] struct {
	fromTime *Time
	toTime   *Time
	iter     btree.GenericIter[*PhysicalRow[K, V]]

	currentRow      *PhysicalRow[K, V]
	currentVersion  Version[V]
	currentIsDelete bool
}

func (t *TreeDiffIter[K, V]) Close() error {
	t.iter.Release()
	return nil
}

func (t *TreeDiffIter[K, V]) Seek(pivot K) bool {
	if !t.iter.Seek(&PhysicalRow[K, V]{
		Key: pivot,
	}) {
		return false
	}
	return t.read()
}

func (t *TreeDiffIter[K, V]) First() bool {
	if !t.iter.First() {
		return false
	}
check:
	if t.read() {
		return true
	}
	if !t.iter.Next() {
		return false
	}
	goto check
}

func (t *TreeDiffIter[K, V]) Next() bool {
	if !t.iter.Next() {
		return false
	}
check:
	if t.read() {
		return true
	}
	if !t.iter.Next() {
		return false
	}
	goto check
}

func (t *TreeDiffIter[K, V]) read() bool {
	physicalRow := t.iter.Item()

	for i := len(physicalRow.Versions) - 1; i >= 0; i-- {
		version := physicalRow.Versions[i]

		if version.LockTx != nil &&
			version.LockTx.State.Load() == Committed &&
			(t.fromTime == nil || version.LockTime.After(*t.fromTime)) &&
			(t.toTime == nil || version.LockTime.Before(*t.toTime)) {
			// committed delete
			t.currentRow = physicalRow
			t.currentVersion = version
			t.currentIsDelete = true
			return true

		} else if version.BornTx.State.Load() == Committed &&
			(t.fromTime == nil || version.BornTime.After(*t.fromTime)) &&
			(t.toTime == nil || version.BornTime.Before(*t.toTime)) {
			// committed insert
			t.currentRow = physicalRow
			t.currentVersion = version
			t.currentIsDelete = false
			return true
		}

	}

	t.currentRow = nil
	return false
}

func (t *TreeDiffIter[K, V]) Read() (
	key K,
	value V,
	bornTime Time,
	lockTime *Time,
	err error,
) {

	if t.currentRow == nil {
		err = sql.ErrNoRows
		return
	}

	key = t.currentRow.Key
	value = t.currentVersion.Value
	bornTime = t.currentVersion.BornTime
	if t.currentIsDelete {
		lockTime = &t.currentVersion.LockTime
	}

	return
}
