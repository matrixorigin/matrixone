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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

type TestRow struct {
	key   Int
	value int
}

func (t TestRow) Key() Int {
	return t.key
}

func (t TestRow) Value() int {
	return t.value
}

func (t TestRow) Indexes() []Tuple {
	return []Tuple{
		{Text("foo"), Int(t.value)},
	}
}

func TestTable(t *testing.T) {

	table := NewTable[Int, int, TestRow]()
	tx := NewTransaction("1", Time{}, Serializable)
	row := TestRow{key: 42, value: 1}

	// insert
	err := table.Insert(tx, row)
	assert.Nil(t, err)

	// get
	r, err := table.Get(tx, Int(42))
	assert.Nil(t, err)
	assert.Equal(t, 1, r)

	// update
	row.value = 2
	err = table.Update(tx, row)
	assert.Nil(t, err)

	r, err = table.Get(tx, Int(42))
	assert.Nil(t, err)
	assert.Equal(t, 2, r)

	// index
	entries, err := table.Index(tx, Tuple{
		Text("foo"), Int(1),
	})
	assert.Nil(t, err)
	assert.Equal(t, 0, len(entries))
	entries, err = table.Index(tx, Tuple{
		Text("foo"), Int(2),
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, Int(42), entries[0].Key)
	assert.Equal(t, 2, entries[0].Value)

	// delete
	err = table.Delete(tx, Int(42))
	assert.Nil(t, err)

}

func TestTableIsolation(t *testing.T) {

	// table
	table := NewTable[Int, int, TestRow]()

	// time util
	ts := func(i int64) Time {
		return Time{
			Timestamp: timestamp.Timestamp{
				PhysicalTime: i,
			},
		}
	}

	// tx 1
	tx1 := NewTransaction("1", ts(1), SnapshotIsolation)

	// tx 2
	tx2 := NewTransaction("2", ts(2), SnapshotIsolation)
	err := table.Insert(tx2, TestRow{
		key:   1,
		value: 2,
	})
	assert.Nil(t, err)
	v, err := table.Get(tx2, 1)
	assert.Nil(t, err)
	assert.Equal(t, 2, v)
	err = tx2.Commit(ts(3))
	assert.Nil(t, err)

	// duplicated key
	err = table.Insert(tx1, TestRow{
		key:   1,
		value: 3,
	})
	assert.NotNil(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicate))

	// read committed
	iter := table.NewIter(tx1)
	n := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		n++
	}
	assert.Equal(t, 0, n)

	tx3 := NewTransaction("3", Time{
		Timestamp: timestamp.Timestamp{
			PhysicalTime: 3,
		},
	}, SnapshotIsolation)
	iter = table.NewIter(tx3)
	n = 0
	for ok := iter.First(); ok; ok = iter.Next() {
		n++
	}
	assert.Equal(t, 1, n)

}
