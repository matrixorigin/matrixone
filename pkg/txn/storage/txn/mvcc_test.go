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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMVCC(t *testing.T) {
	// new
	m := new(MVCC[int])
	m.dump(io.Discard)

	// tx
	tx1 := NewTransaction("1", Timestamp{})
	tx2 := NewTransaction("2", Timestamp{})

	// time
	now := Timestamp{
		PhysicalTime: 1,
		LogicalTime:  0,
	}

	// insert
	err := m.Insert(tx1, now, 1)
	assert.Nil(t, err)

	err = m.Insert(tx2, now, 2)
	assert.Nil(t, err)

	now = now.Next()

	// read
	res := m.Read(tx1, now)
	assert.NotNil(t, res)
	assert.Equal(t, 1, *res)

	res = m.Read(tx2, now)
	assert.NotNil(t, res)
	assert.Equal(t, 2, *res)

	now = now.Next()

	// delete
	err = m.Delete(tx1, now)
	assert.Nil(t, err)
	res = m.Read(tx1, now)
	assert.Nil(t, res)

	res = m.Read(tx2, now)
	assert.NotNil(t, res)
	assert.Equal(t, 2, *res)

	err = m.Delete(tx2, now)
	assert.Nil(t, err)
	res = m.Read(tx2, now)
	assert.Nil(t, res)

	res = m.Read(tx2, now)
	assert.Nil(t, res)

	res = m.Read(tx1, now)
	assert.Nil(t, res)

	now = now.Next()

	// update
	err = m.Insert(tx1, now, 1)
	assert.Nil(t, err)

	err = m.Insert(tx2, now, 2)
	assert.Nil(t, err)

	now = now.Next()

	err = m.Update(tx1, now, 3)
	assert.Nil(t, err)

	now = now.Next()

	res = m.Read(tx1, now)
	assert.NotNil(t, res)
	assert.Equal(t, 3, *res)
	res = m.Read(tx2, now)
	assert.NotNil(t, res)
	assert.Equal(t, 2, *res)

	err = m.Update(tx2, now, 4)
	assert.Nil(t, err)

	now = now.Next()

	res = m.Read(tx1, now)
	assert.NotNil(t, res)
	assert.Equal(t, 3, *res)
	res = m.Read(tx2, now)
	assert.NotNil(t, res)
	assert.Equal(t, 4, *res)

	now = now.Next()

	// commit and read
	tx1.State = Committed

	tx3 := &Transaction{
		ID: "3",
	}
	tx4 := &Transaction{
		ID: "4",
	}

	// read committed
	res = m.Read(tx3, now)
	assert.NotNil(t, res)
	assert.Equal(t, 3, *res)

	now = now.Next()

	// concurrent delete
	err = m.Delete(tx4, now)
	assert.Nil(t, err)
	res = m.Read(tx4, now)
	assert.Nil(t, res)

	now = now.Next()

	res = m.Read(tx3, now)
	assert.NotNil(t, res)
	assert.Equal(t, 3, *res)

	// delete conflict
	err = m.Delete(tx3, now)
	var conflict *ErrWriteConflict
	assert.ErrorAs(t, err, &conflict)

	now = now.Next()

	// update conflict
	err = m.Update(tx3, now, 5)
	assert.ErrorAs(t, err, &conflict)

}
