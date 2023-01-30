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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIter(t *testing.T) {
	table := NewTable[Int, int, TestRow]()
	tx := NewTransaction(Time{})

	num := 1024
	for _, i := range rand.Perm(num) {
		row := TestRow{
			key:   Int(i),
			value: i,
		}
		err := table.Insert(tx, row)
		assert.Nil(t, err)
	}

	iter, err := table.NewIter(tx)
	assert.Nil(t, err)
	n := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		n++
		key, value, err := iter.Read()
		assert.Nil(t, err)
		assert.Equal(t, int(key), value)
	}
	assert.Equal(t, num, n)
	assert.Nil(t, iter.Close())

	iter, err = table.NewIter(tx)
	assert.Nil(t, err)
	n = 0
	for ok := iter.Seek(Int(num / 2)); ok; ok = iter.Next() {
		n++
		key, value, err := iter.Read()
		assert.Nil(t, err)
		assert.Equal(t, int(key), value)
	}
	assert.Equal(t, num/2, n)
	assert.Nil(t, iter.Close())

	// seek invalid
	iter, err = table.NewIter(tx)
	assert.Nil(t, err)
	n = 0
	for ok := iter.Seek(Int(num * 10)); ok; ok = iter.Next() {
		n++
		iter.Read()
	}
	if n != 0 {
		t.Fatalf("got %v", n)
	}

}
