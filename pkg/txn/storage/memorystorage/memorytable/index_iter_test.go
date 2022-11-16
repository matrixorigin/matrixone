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

package memorytable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexIter(t *testing.T) {
	table := NewTable[Int, int, TestRow]()
	tx := NewTransaction(Time{})
	defer func() {
		err := tx.Commit(Time{})
		assert.Nil(t, err)
	}()

	row := TestRow{
		key:   1,
		value: 1,
	}
	err := table.Insert(tx, row)
	assert.Nil(t, err)

	min := Tuple{
		Min,
	}
	max := Tuple{
		Max,
	}

	iter, err := table.NewIndexIter(tx, min, max)
	assert.Nil(t, err)
	defer iter.Close()
	values := make(map[int]bool)
	for ok := iter.First(); ok; ok = iter.Next() {
		entry, err := iter.Read()
		assert.Nil(t, err)
		value, err := table.Get(tx, entry.Key)
		assert.Nil(t, err)
		values[value] = true
		assert.Equal(t, Int(1), entry.Index[1])
	}
	assert.Equal(t, 1, len(values))
	assert.True(t, values[1])

	row.value = 2
	err = table.Update(tx, row)
	assert.Nil(t, err)
	iter, err = table.NewIndexIter(tx, min, max)
	assert.Nil(t, err)
	defer iter.Close()
	values = make(map[int]bool)
	for ok := iter.First(); ok; ok = iter.Next() {
		entry, err := iter.Read()
		assert.Nil(t, err)
		value, err := table.Get(tx, entry.Key)
		assert.Nil(t, err)
		values[value] = true
		assert.Equal(t, Int(2), entry.Index[1])
	}
	assert.Equal(t, 1, len(values))
	assert.True(t, values[2])

	err = table.Delete(tx, row.key)
	assert.Nil(t, err)
	iter, err = table.NewIndexIter(tx, min, max)
	assert.Nil(t, err)
	defer iter.Close()
	values = make(map[int]bool)
	for ok := iter.First(); ok; ok = iter.Next() {
		entry, err := iter.Read()
		assert.Nil(t, err)
		value, err := table.Get(tx, entry.Key)
		assert.Nil(t, err)
		values[value] = true
	}
	assert.Equal(t, 0, len(values))

}
