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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func TestTransaction(t *testing.T) {

	t.Run("seq", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		tx := NewTransaction(Time{})

		// insert
		row := TestRow{
			key:   1,
			value: 1,
		}
		err := table.Insert(tx, row)
		assert.Nil(t, err)

		// read before commit
		tx2 := NewTransaction(Time{})
		_, err = table.Get(tx2, Int(1))
		assert.ErrorIs(t, sql.ErrNoRows, err)

		// commit
		assert.Nil(t, tx.Commit(Time{}))

		// read after commit
		tx3 := NewTransaction(Time{})
		value, err := table.Get(tx3, Int(1))
		assert.Nil(t, err)
		assert.Equal(t, 1, value)
	})

	isDuplicateOrWriteWriteConflict := func(err error) {
		if err == nil {
			return
		}
		if !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) &&
			!moerr.IsMoErrCode(err, moerr.ErrDuplicate) {
			panic(err)
		}
	}

	t.Run("concurrent insert", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   1,
			value: 1,
		}
		c := 1024
		var n int64

		wg := new(sync.WaitGroup)
		wg.Add(c)
		for i := 0; i < c; i++ {
			go func() {
				defer wg.Done()
				tx := NewTransaction(Time{})
				err := table.Insert(tx, row)
				err2 := tx.Commit(Time{})
				isDuplicateOrWriteWriteConflict(err)
				isDuplicateOrWriteWriteConflict(err2)
				if err == nil && err2 == nil {
					atomic.AddInt64(&n, 1)
				} else {
					err = tx.Abort()
					assert.Nil(t, err)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int64(1), n)
	})

	t.Run("concurrent update", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   1,
			value: 1,
		}
		tx := NewTransaction(Time{})
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		c := 1024
		var n int64
		var txs []*Transaction
		for i := 0; i < c; i++ {
			// setup tx table states
			tx := NewTransaction(Time{})
			txs = append(txs, tx)
			_, err := table.Get(tx, row.key)
			assert.Nil(t, err)
		}

		wg := new(sync.WaitGroup)
		wg.Add(c)
		for _, tx := range txs {
			tx := tx
			go func() {
				defer wg.Done()
				err := table.Update(tx, row)
				err2 := tx.Commit(Time{})
				isDuplicateOrWriteWriteConflict(err)
				isDuplicateOrWriteWriteConflict(err2)
				if err == nil && err2 == nil {
					atomic.AddInt64(&n, 1)
				} else {
					err = tx.Abort()
					assert.Nil(t, err)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int64(1), n)
	})

	t.Run("concurrent delete", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   1,
			value: 1,
		}
		tx := NewTransaction(Time{})
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		c := 1024
		var n int64
		var txs []*Transaction
		for i := 0; i < c; i++ {
			// setup tx table states
			tx := NewTransaction(Time{})
			txs = append(txs, tx)
			_, err := table.Get(tx, row.key)
			assert.Nil(t, err)
		}

		wg := new(sync.WaitGroup)
		wg.Add(c)
		for _, tx := range txs {
			tx := tx
			go func() {
				defer wg.Done()
				err := table.Delete(tx, row.key)
				err2 := tx.Commit(Time{})
				isDuplicateOrWriteWriteConflict(err)
				isDuplicateOrWriteWriteConflict(err2)
				if err == nil && err2 == nil {
					atomic.AddInt64(&n, 1)
				} else {
					err = tx.Abort()
					assert.Nil(t, err)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int64(1), n)
	})

	t.Run("concurrent update and delete", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   1,
			value: 1,
		}
		tx := NewTransaction(Time{})
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		c := 1024
		var n int64
		var txs []*Transaction
		for i := 0; i < c; i++ {
			// setup tx table states
			tx := NewTransaction(Time{})
			txs = append(txs, tx)
			_, err := table.Get(tx, row.key)
			assert.Nil(t, err)
		}

		wg := new(sync.WaitGroup)
		wg.Add(c)
		for i, tx := range txs {
			i := i
			tx := tx
			go func() {
				defer wg.Done()
				var err error
				if i%2 == 0 {
					err = table.Delete(tx, row.key)
				} else {
					err = table.Update(tx, row)
				}
				err2 := tx.Commit(Time{})
				isDuplicateOrWriteWriteConflict(err)
				isDuplicateOrWriteWriteConflict(err2)
				if err == nil && err2 == nil {
					atomic.AddInt64(&n, 1)
				} else {
					err = tx.Abort()
					assert.Nil(t, err)
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int64(1), n)
	})

}

func TestWriteWriteConflict(t *testing.T) {

	t.Run("update deleted", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   42,
			value: 1,
		}
		tx := NewTransaction(Time{})
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		tx = NewTransaction(Time{})
		table.getTransactionTable(tx)
		tx2 := NewTransaction(Time{})
		table.getTransactionTable(tx2)

		err = table.Delete(tx, row.key)
		assert.Nil(t, err)
		err = tx.Commit(ts(1))
		assert.Nil(t, err)

		err = table.Update(tx2, row)
		assert.Nil(t, err)
		err = tx2.Commit(Time{})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	})

	t.Run("update updated", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   42,
			value: 1,
		}
		tx := NewTransaction(Time{})
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		tx = NewTransaction(Time{})
		table.getTransactionTable(tx)
		tx2 := NewTransaction(Time{})
		table.getTransactionTable(tx2)

		err = table.Update(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(ts(1))
		assert.Nil(t, err)

		err = table.Update(tx2, row)
		assert.Nil(t, err)
		err = tx2.Commit(Time{})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	})

	t.Run("delete deleted", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   42,
			value: 1,
		}
		tx := NewTransaction(Time{})
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		tx = NewTransaction(Time{})
		table.getTransactionTable(tx)
		tx2 := NewTransaction(Time{})
		table.getTransactionTable(tx2)

		err = table.Delete(tx, row.key)
		assert.Nil(t, err)
		err = tx.Commit(ts(1))
		assert.Nil(t, err)

		err = table.Delete(tx2, row.key)
		assert.Nil(t, err)
		err = tx2.Commit(Time{})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	})

	t.Run("delete updated", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   42,
			value: 1,
		}
		tx := NewTransaction(Time{})
		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		tx = NewTransaction(Time{})
		table.getTransactionTable(tx)
		tx2 := NewTransaction(Time{})
		table.getTransactionTable(tx2)

		err = table.Update(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		err = table.Delete(tx2, row.key)
		assert.Nil(t, err)
		err = tx2.Commit(Time{})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	})

	t.Run("insert inserted", func(t *testing.T) {
		table := NewTable[Int, int, TestRow]()
		row := TestRow{
			key:   42,
			value: 1,
		}

		tx := NewTransaction(Time{})
		table.getTransactionTable(tx)
		tx2 := NewTransaction(Time{})
		table.getTransactionTable(tx2)

		err := table.Insert(tx, row)
		assert.Nil(t, err)
		err = tx.Commit(Time{})
		assert.Nil(t, err)

		err = table.Insert(tx2, row)
		assert.Nil(t, err)
		err = tx2.Commit(Time{})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	})

}
