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

package lockservice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCheckWithDeadlock(t *testing.T) {
	txn1 := []byte("t1")
	txn2 := []byte("t2")
	txn3 := []byte("t3")
	txn4 := []byte("t4")

	m := map[string][][]byte{
		string(txn1): {txn2},
		string(txn2): {txn3},
		string(txn3): {txn1},
	}
	abortC := make(chan []byte, 1)
	defer close(abortC)

	d := newDeadlockDetector(func(txn []byte, w *waiters) bool {
		for _, id := range m[string(txn)] {
			if !w.add(id) {
				return false
			}
		}
		return true
	}, func(txn []byte) {
		abortC <- txn
	})
	defer d.close()

	assert.NoError(t, d.check(txn1))
	assert.Equal(t, txn1, <-abortC)
	d.txnClosed(txn1)

	assert.NoError(t, d.check(txn2))
	assert.Equal(t, txn2, <-abortC)
	d.txnClosed(txn2)

	assert.NoError(t, d.check(txn3))
	assert.Equal(t, txn3, <-abortC)
	d.txnClosed(txn3)

	assert.NoError(t, d.check(txn4))
	select {
	case <-abortC:
		assert.Fail(t, "can not found dead lock")
	case <-time.After(time.Millisecond * 100):
	}
}
