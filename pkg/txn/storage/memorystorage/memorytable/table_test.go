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
	"database/sql"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

type TestRow struct {
	key   Int
	value int
}

var _ Row[Int, int] = TestRow{}

func (r TestRow) Key() Int {
	return r.key
}

func (r TestRow) Value() int {
	return r.value
}

var testindex_Value = Text("value")

func (r TestRow) Indexes() []Tuple {
	return []Tuple{
		{testindex_Value, Int(r.value)},
	}
}

func (r TestRow) UniqueIndexes() []Tuple {
	return nil
}

func TestTable(t *testing.T) {

	t.Run("insert and get", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})
		defer func() {
			err := tx.Commit(Time{})
			assert.Nil(t, err)
		}()
		row := TestRow{
			key:   42,
			value: 1,
		}
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		v, err := table.Get(tx, row.key)
		assert.Nil(t, err)
		assert.Equal(t, 1, v)
		_, err = table.Get(tx, Int(99))
		assert.ErrorIs(t, sql.ErrNoRows, err)
	})

	t.Run("insert duplicate", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})
		defer func() {
			err := tx.Commit(Time{})
			assert.Nil(t, err)
		}()
		row := TestRow{
			key:   42,
			value: 1,
		}
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = table.Insert(tx, row)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicate))
	})

	t.Run("insert and update", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})
		defer func() {
			err := tx.Commit(Time{})
			assert.Nil(t, err)
		}()
		row := TestRow{
			key:   42,
			value: 1,
		}
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		newRow := TestRow{
			key:   42,
			value: 2,
		}
		err = table.Update(tx, newRow)
		assert.Nil(t, err)
		v, err := table.Get(tx, row.key)
		assert.Nil(t, err)
		assert.Equal(t, 2, v)
	})

	t.Run("update now rows", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})
		defer func() {
			err := tx.Commit(Time{})
			assert.Nil(t, err)
		}()
		row := TestRow{
			key:   42,
			value: 1,
		}
		err := table.Update(tx, row)
		assert.ErrorIs(t, sql.ErrNoRows, err)
	})

	t.Run("insert and delete", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})
		defer func() {
			err := tx.Commit(Time{})
			assert.Nil(t, err)
		}()
		row := TestRow{
			key:   42,
			value: 1,
		}
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = table.Delete(tx, row.key)
		assert.Nil(t, err)
		_, err = table.Get(tx, row.key)
		assert.ErrorIs(t, sql.ErrNoRows, err)
	})

	t.Run("delete now rows", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})
		defer func() {
			err := tx.Commit(Time{})
			assert.Nil(t, err)
		}()
		err := table.Delete(tx, 42)
		assert.ErrorIs(t, sql.ErrNoRows, err)
	})

	t.Run("upsert", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})
		defer func() {
			err := tx.Commit(Time{})
			assert.Nil(t, err)
		}()
		row := TestRow{
			key:   42,
			value: 1,
		}
		err := table.Upsert(tx, row)
		assert.Nil(t, err)
		v, err := table.Get(tx, row.key)
		assert.Nil(t, err)
		assert.Equal(t, 1, v)
		newRow := TestRow{
			key:   42,
			value: 2,
		}
		err = table.Upsert(tx, newRow)
		assert.Nil(t, err)
		v, err = table.Get(tx, row.key)
		assert.Nil(t, err)
		assert.Equal(t, 2, v)
	})

}

func TestTableHistory(t *testing.T) {
	table := NewTable[Int, int, TestRow]()

	row := TestRow{
		key:   42,
		value: 1,
	}
	for i := 0; i < 10; i++ {
		row.value = i
		tx := NewTransaction(ts(int64(i)))
		err := table.Upsert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(tx.BeginTime)
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		tx := NewTransaction(ts(int64(i + 1)))
		v, err := table.Get(tx, row.key)
		assert.Nil(t, err)
		assert.Equal(t, i, v)
	}

	table.EraseHistory(ts(5))
	tx := NewTransaction(ts(5))
	_, err := table.getTransactionTable(tx)
	assert.Nil(t, err)
	tx = NewTransaction(ts(4))
	_, err = table.getTransactionTable(tx)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "begin time too old")

}
