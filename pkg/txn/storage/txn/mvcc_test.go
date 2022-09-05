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
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func testMVCC(
	t *testing.T,
	isolationPolicy IsolationPolicy,
) {

	// new
	m := new(MVCC[int])
	m.dump(io.Discard)

	// time
	now := Time{
		Timestamp: timestamp.Timestamp{
			PhysicalTime: 1,
			LogicalTime:  0,
		},
	}

	// tx
	tx1 := NewTransaction("1", now, isolationPolicy)
	tx2 := NewTransaction("2", now, isolationPolicy)

	// insert
	err := m.Insert(tx1, now, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(m.Values))
	assert.Equal(t, tx1, m.Values[0].BornTx)
	assert.Equal(t, now, m.Values[0].BornTime)
	assert.Nil(t, m.Values[0].LockTx)
	assert.True(t, m.Values[0].LockTime.IsZero())

	err = m.Insert(tx2, now, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(m.Values))
	assert.Equal(t, tx2, m.Values[1].BornTx)
	assert.Equal(t, now, m.Values[1].BornTime)
	assert.Nil(t, m.Values[1].LockTx)
	assert.True(t, m.Values[1].LockTime.IsZero())

	// not readable now
	res, err := m.Read(tx1, now)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Nil(t, res)

	res, err = m.Read(tx2, now)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Nil(t, res)

	now = now.Next()

	// read
	res, err = m.Read(tx1, now)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 1, *res)

	res, err = m.Read(tx2, now)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 2, *res)

	// delete
	err = m.Delete(tx1, now)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(m.Values))
	assert.Equal(t, tx1, m.Values[0].LockTx)
	assert.Equal(t, now, m.Values[0].LockTime)

	// not readable now by current tx
	res, err = m.Read(tx1, now)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Nil(t, res)

	// tx2 still readable
	res, err = m.Read(tx2, now)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 2, *res)

	err = m.Delete(tx2, now)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(m.Values))
	assert.Equal(t, tx2, m.Values[1].LockTx)
	assert.Equal(t, now, m.Values[1].LockTime)

	res, err = m.Read(tx2, now)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Nil(t, res)

	now = now.Next()

	// insert again
	err = m.Insert(tx1, now, 3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(m.Values))
	assert.Equal(t, tx1, m.Values[2].BornTx)
	assert.Equal(t, now, m.Values[2].BornTime)
	assert.Nil(t, m.Values[2].LockTx)
	assert.True(t, m.Values[2].LockTime.IsZero())

	err = m.Insert(tx2, now, 4)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(m.Values))
	assert.Equal(t, tx2, m.Values[3].BornTx)
	assert.Equal(t, now, m.Values[3].BornTime)
	assert.Nil(t, m.Values[3].LockTx)
	assert.True(t, m.Values[3].LockTime.IsZero())

	now = now.Next()

	// update
	err = m.Update(tx1, now, 5)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(m.Values))
	assert.Equal(t, tx1, m.Values[2].LockTx)
	assert.Equal(t, now, m.Values[2].LockTime)
	assert.Equal(t, tx1, m.Values[4].BornTx)
	assert.Equal(t, now, m.Values[4].BornTime)
	assert.Nil(t, m.Values[4].LockTx)
	assert.True(t, m.Values[4].LockTime.IsZero())

	// commit tx1
	tx1.State.Store(Committed)

	now = now.Next()

	// test read policy
	switch isolationPolicy.Read {
	case ReadCommitted:
		res, err = m.Read(tx2, now)
		assert.Nil(t, err)
		assert.Equal(t, 5, *res)
	case ReadSnapshot:
		res, err = m.Read(tx2, now)
		assert.Nil(t, err)
		assert.Equal(t, 4, *res)
	case ReadNoStale:
		res, err = m.Read(tx2, now)
		assert.NotNil(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, 5, *res)
		conflict, ok := err.(*ErrReadConflict)
		assert.True(t, ok)
		assert.Equal(t, tx2, conflict.ReadingTx)
		assert.Equal(t, tx1, conflict.Stale)
	default:
		panic(fmt.Errorf("not handle: %v", isolationPolicy.Read))
	}

	// write stale conflict
	err = m.Insert(tx2, now, 1)
	assert.NotNil(t, err)
	writeConflict, ok := err.(*ErrWriteConflict)
	assert.True(t, ok)
	assert.Equal(t, tx2, writeConflict.WritingTx)
	assert.Equal(t, tx1, writeConflict.Stale)

	err = m.Delete(tx2, now)
	assert.NotNil(t, err)
	writeConflict, ok = err.(*ErrWriteConflict)
	assert.True(t, ok)
	assert.Equal(t, tx2, writeConflict.WritingTx)
	assert.Equal(t, tx1, writeConflict.Stale)

	err = m.Update(tx2, now, 1)
	assert.NotNil(t, err)
	writeConflict, ok = err.(*ErrWriteConflict)
	assert.True(t, ok)
	assert.Equal(t, tx2, writeConflict.WritingTx)
	assert.Equal(t, tx1, writeConflict.Stale)

	// new transactions
	tx3 := NewTransaction("3", now, isolationPolicy)
	tx4 := NewTransaction("4", now, isolationPolicy)

	// write locked conflict
	err = m.Delete(tx3, now)
	assert.Nil(t, err)

	err = m.Delete(tx4, now)
	assert.NotNil(t, err)
	writeConflict, ok = err.(*ErrWriteConflict)
	assert.True(t, ok)
	assert.Equal(t, tx4, writeConflict.WritingTx)
	assert.Equal(t, tx3, writeConflict.Locked)

	err = m.Insert(tx4, now, 1)
	assert.NotNil(t, err)
	writeConflict, ok = err.(*ErrWriteConflict)
	assert.True(t, ok)
	assert.Equal(t, tx4, writeConflict.WritingTx)
	assert.Equal(t, tx3, writeConflict.Locked)

	err = m.Update(tx4, now, 1)
	assert.NotNil(t, err)
	writeConflict, ok = err.(*ErrWriteConflict)
	assert.True(t, ok)
	assert.Equal(t, tx4, writeConflict.WritingTx)
	assert.Equal(t, tx3, writeConflict.Locked)

}

func TestMVCC(t *testing.T) {
	t.Run("read committed", func(t *testing.T) {
		testMVCC(t, IsolationPolicy{
			Read: ReadCommitted,
		})
	})

	t.Run("snapshot isolation", func(t *testing.T) {
		testMVCC(t, SnapshotIsolation)
	})

	t.Run("serializable", func(t *testing.T) {
		testMVCC(t, Serializable)
	})
}
