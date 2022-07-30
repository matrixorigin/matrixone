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
	PrimaryKey Ordered[PrimaryKey],
	Attrs Attributes[PrimaryKey],
] struct {
	tx       *Transaction
	iter     btree.GenericIter[*Row[PrimaryKey, Attrs]]
	readTime Timestamp
}

func (t *Table[PrimaryKey, Attrs]) NewIter(
	tx *Transaction,
) (
	iter *TableIter[PrimaryKey, Attrs],
) {
	iter = &TableIter[PrimaryKey, Attrs]{
		tx:       tx,
		iter:     t.Rows.Copy().Iter(),
		readTime: tx.CurrentTime,
	}
	return
}

func (t *TableIter[PrimaryKey, Attrs]) Read() (key PrimaryKey, attrs *Attrs) {
	row := t.iter.Item()
	key = row.PrimaryKey
	attrs = row.Values.Read(t.tx, t.readTime)
	return
}

func (t *TableIter[PrimaryKey, Attrs]) Next() bool {
	for {
		if ok := t.iter.Next(); !ok {
			return false
		}
		// skip invisible values
		if !t.iter.Item().Values.Visible(t.tx, t.tx.CurrentTime) {
			continue
		}
		return true
	}
}

func (t *TableIter[PrimaryKey, Attrs]) First() bool {
	if ok := t.iter.First(); !ok {
		return false
	}
	for {
		// skip invisible values
		if !t.iter.Item().Values.Visible(t.tx, t.tx.CurrentTime) {
			if ok := t.iter.Next(); !ok {
				return false
			}
			continue
		}
		return true
	}
}

func (t *TableIter[PrimaryKey, Attrs]) Close() error {
	t.iter.Release()
	return nil
}
