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
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func testPhysicalRow(
	t *testing.T,
	isolationPolicy IsolationPolicy,
) {

	// new
	m := new(PhysicalRow[Int, int])
	m.dump(io.Discard)

	// time
	now := Time{
		Timestamp: timestamp.Timestamp{
			PhysicalTime: 1,
			LogicalTime:  0,
		},
	}
	tick := func() {
		now.Timestamp = now.Timestamp.Next()
	}

	// tx
	tx1 := NewTransaction("1", now, isolationPolicy)
	tx2 := NewTransaction("2", now, isolationPolicy)

	// insert
	n := 1
	m, _, err := m.Insert(now, tx1, n)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(m.Versions))
	assert.Equal(t, tx1, m.Versions[0].BornTx)
	assert.Equal(t, now, m.Versions[0].BornTime)
	assert.Nil(t, m.Versions[0].LockTx)
	assert.True(t, m.Versions[0].LockTime.IsZero())

	n2 := 2
	m, _, err = m.Insert(now, tx2, n2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(m.Versions))
	assert.Equal(t, tx2, m.Versions[1].BornTx)
	assert.Equal(t, now, m.Versions[1].BornTime)
	assert.Nil(t, m.Versions[1].LockTx)
	assert.True(t, m.Versions[1].LockTime.IsZero())

	// not readable now
	res, err := m.Read(now, tx1)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Empty(t, res)

	res, err = m.Read(now, tx2)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Empty(t, res)

	tick()

	// read
	res, err = m.Read(now, tx1)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 1, res)

	res, err = m.Read(now, tx2)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 2, res)

	// delete
	m, _, err = m.Delete(now, tx1)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(m.Versions))
	assert.Equal(t, tx1, m.Versions[0].LockTx)
	assert.Equal(t, now, m.Versions[0].LockTime)

	// not readable now by current tx
	res, err = m.Read(now, tx1)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Empty(t, res)

	// tx2 still readable
	res, err = m.Read(now, tx2)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 2, res)

	m, _, err = m.Delete(now, tx2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(m.Versions))
	assert.Equal(t, tx2, m.Versions[1].LockTx)
	assert.Equal(t, now, m.Versions[1].LockTime)

	res, err = m.Read(now, tx2)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Empty(t, res)

	tick()

	// insert again
	n3 := 3
	m, _, err = m.Insert(now, tx1, n3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(m.Versions))
	assert.Equal(t, tx1, m.Versions[2].BornTx)
	assert.Equal(t, now, m.Versions[2].BornTime)
	assert.Nil(t, m.Versions[2].LockTx)
	assert.True(t, m.Versions[2].LockTime.IsZero())

	n4 := 4
	m, _, err = m.Insert(now, tx2, n4)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(m.Versions))
	assert.Equal(t, tx2, m.Versions[3].BornTx)
	assert.Equal(t, now, m.Versions[3].BornTime)
	assert.Nil(t, m.Versions[3].LockTx)
	assert.True(t, m.Versions[3].LockTime.IsZero())

	tick()

	// update
	n5 := 5
	m, _, err = m.Update(now, tx1, n5)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(m.Versions))
	assert.Equal(t, tx1, m.Versions[2].LockTx)
	assert.Equal(t, now, m.Versions[2].LockTime)
	assert.Equal(t, tx1, m.Versions[4].BornTx)
	assert.Equal(t, now, m.Versions[4].BornTime)
	assert.Nil(t, m.Versions[4].LockTx)
	assert.True(t, m.Versions[4].LockTime.IsZero())

	tick()

	// commit tx1
	err = tx1.Commit(now)
	assert.Nil(t, err)

	tick()

	// test read policy
	switch isolationPolicy.Read {
	case ReadCommitted:
		res, err = m.Read(now, tx2)
		assert.Nil(t, err)
		assert.Equal(t, 5, res)
	case ReadSnapshot:
		res, err = m.Read(now, tx2)
		assert.Nil(t, err)
		assert.Equal(t, 4, res)
	case ReadNoStale:
		res, err = m.Read(now, tx2)
		assert.NotNil(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, 5, res)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnReadConflict))
	default:
		panic(fmt.Sprintf("not handle: %v", isolationPolicy.Read))
	}

	// write stale conflict
	i := 1
	_, _, err = m.Insert(now, tx2, i)
	assert.NotNil(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict))

	_, _, err = m.Delete(now, tx2)
	assert.NotNil(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict))

	i2 := 1
	_, _, err = m.Update(now, tx2, i2)
	assert.NotNil(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict))

	// new transactions
	tx3 := NewTransaction("3", now, isolationPolicy)
	tx4 := NewTransaction("4", now, isolationPolicy)

	// write locked conflict
	m, _, err = m.Delete(now, tx3)
	assert.Nil(t, err)

	_, _, err = m.Delete(now, tx4)
	assert.NotNil(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict))

	i3 := 1
	_, _, err = m.Insert(now, tx4, i3)
	assert.NotNil(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict))

	i4 := 1
	_, _, err = m.Update(now, tx4, i4)
	assert.NotNil(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict))
}

func TestPhysicalRow(t *testing.T) {
	t.Run("read committed", func(t *testing.T) {
		testPhysicalRow(t, IsolationPolicy{
			Read: ReadCommitted,
		})
	})

	t.Run("snapshot isolation", func(t *testing.T) {
		testPhysicalRow(t, SnapshotIsolation)
	})

	t.Run("serializable", func(t *testing.T) {
		testPhysicalRow(t, Serializable)
	})
}
