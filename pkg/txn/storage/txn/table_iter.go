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

package txnstorage

import (
	"github.com/tidwall/btree"
)

type TableIter[
	K Ordered[K],
	R Row[K],
] struct {
	tx       *Transaction
	iter     btree.GenericIter[*PhysicalRow[K, R]]
	readTime Time
}

func (t *Table[K, R]) NewIter(
	tx *Transaction,
) (
	iter *TableIter[K, R],
) {
	iter = &TableIter[K, R]{
		tx:       tx,
		iter:     t.Rows.Copy().Iter(),
		readTime: tx.CurrentTime,
	}
	return
}

func (t *TableIter[K, R]) Read() (key K, row *R, err error) {
	physicalRow := t.iter.Item()
	key = physicalRow.PrimaryKey
	row, err = physicalRow.Values.Read(t.tx, t.readTime)
	if err != nil {
		return
	}
	return
}

func (t *TableIter[K, R]) Next() bool {
	for {
		if ok := t.iter.Next(); !ok {
			return false
		}
		// skip unreadable values
		value, _ := t.iter.Item().Values.Read(t.tx, t.readTime)
		if value == nil {
			continue
		}
		return true
	}
}

func (t *TableIter[K, R]) First() bool {
	if ok := t.iter.First(); !ok {
		return false
	}
	for {
		// skip unreadable values
		value, _ := t.iter.Item().Values.Read(t.tx, t.readTime)
		if value == nil {
			if ok := t.iter.Next(); !ok {
				return false
			}
			continue
		}
		return true
	}
}

func (t *TableIter[K, R]) Close() error {
	t.iter.Release()
	return nil
}
