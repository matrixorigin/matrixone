// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	q := newWaiterQueue()
	w := acquireWaiter([]byte("w"))
	q.put(w)
	assert.Equal(t, 1, len(q.waiters))
	assert.Equal(t, 0, q.offset)
}

func TestLen(t *testing.T) {
	q := newWaiterQueue()
	q.put(acquireWaiter([]byte("w")))
	q.put(acquireWaiter([]byte("w1")))
	q.put(acquireWaiter([]byte("w2")))
	assert.Equal(t, 3, q.len())

	q.mustGet()
	assert.Equal(t, 2, q.len())
	assert.Nil(t, q.waiters[0])

	q.mustGet()
	assert.Equal(t, 1, q.len())
	assert.Nil(t, q.waiters[1])

	q.mustGet()
	assert.Equal(t, 0, q.len())
	assert.Nil(t, q.waiters[2])

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	q.mustGet()
}

func TestReset(t *testing.T) {
	q := newWaiterQueue()
	q.put(acquireWaiter([]byte("w")))
	q.put(acquireWaiter([]byte("w1")))
	q.put(acquireWaiter([]byte("w2")))
	q.mustGet()

	q.reset()
	assert.Equal(t, 0, q.offset)
	assert.Empty(t, q.waiters)
}

func TestIterTxns(t *testing.T) {
	q := newWaiterQueue()
	q.put(acquireWaiter([]byte("w")))
	q.put(acquireWaiter([]byte("w1")))
	q.put(acquireWaiter([]byte("w2")))

	var values [][]byte
	v := 0
	q.iter(func(b []byte) bool {
		values = append(values, b)
		v++
		return v < 2
	})
	assert.Equal(t, [][]byte{[]byte("w"), []byte("w1")}, values)
}
