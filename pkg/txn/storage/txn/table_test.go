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
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestRow struct {
	Key   Int
	Value int
}

func (t TestRow) PrimaryKey() Int {
	return t.Key
}

func TestTable(t *testing.T) {
	table := NewTable[Int, TestRow]()

	tx := NewTransaction("1", Timestamp{})

	row := TestRow{Key: 42, Value: 1}

	// insert
	err := table.Insert(tx, row)
	assert.Nil(t, err)

	// get
	r, err := table.Get(tx, Int(42))
	assert.Nil(t, err)
	assert.Equal(t, row, r)

	// update
	row.Value = 2
	err = table.Update(tx, row)
	assert.Nil(t, err)

	r, err = table.Get(tx, Int(42))
	assert.Nil(t, err)
	assert.Equal(t, row, r)

	// delete
	err = table.Delete(tx, Int(42))
	assert.Nil(t, err)

}
