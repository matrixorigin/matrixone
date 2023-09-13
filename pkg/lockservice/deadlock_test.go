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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestCheckWithDeadlock(t *testing.T) {
	txn1 := []byte("t1")
	txn2 := []byte("t2")
	txn3 := []byte("t3")
	txn4 := []byte("t4")

	m := map[string][]pb.WaitTxn{
		string(txn1): {{TxnID: txn2}},
		string(txn2): {{TxnID: txn3}},
		string(txn3): {{TxnID: txn1}},
	}
	abortC := make(chan []byte, 1)
	defer close(abortC)

	d := newDeadlockDetector(
		"s1",
		func(txn pb.WaitTxn, w *waiters) (bool, error) {
			for _, v := range m[string(txn.TxnID)] {
				if !w.add(v) {
					return false, nil
				}
			}
			return true, nil
		}, func(txn pb.WaitTxn, err error) {
			abortC <- txn.TxnID
		})
	defer d.close()

	assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn1}))
	assert.Equal(t, txn1, <-abortC)
	d.txnClosed(txn1)

	assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn2}))
	assert.Equal(t, txn2, <-abortC)
	d.txnClosed(txn2)

	assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn3}))
	assert.Equal(t, txn3, <-abortC)
	d.txnClosed(txn3)

	assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn4}))
	select {
	case <-abortC:
		assert.Fail(t, "can not found dead lock")
	case <-time.After(time.Millisecond * 100):
	}
}

func TestCheckWithDeadlockWith2Txn(t *testing.T) {
	txn1 := []byte("t1")
	txn2 := []byte("t2")
	txn3 := []byte("t3")

	depends := map[string][]pb.WaitTxn{
		string(txn1): {{TxnID: txn2}},
		string(txn2): {{TxnID: txn1}},
	}
	abortC := make(chan []byte, 1)
	defer close(abortC)

	d := newDeadlockDetector(
		"s1",
		func(txn pb.WaitTxn, w *waiters) (bool, error) {
			for _, v := range depends[string(txn.TxnID)] {
				if !w.add(v) {
					return false, nil
				}
			}
			return true, nil
		}, func(txn pb.WaitTxn, err error) {
			abortC <- txn.TxnID
		})
	defer d.close()

	assert.NoError(t, d.check(txn2, pb.WaitTxn{TxnID: txn1}))
	assert.Equal(t, txn1, <-abortC)
	d.txnClosed(txn1)

	assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn2}))
	assert.Equal(t, txn2, <-abortC)
	d.txnClosed(txn2)

	assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn3}))
	select {
	case <-abortC:
		assert.Fail(t, "can not found dead lock")
	case <-time.After(time.Millisecond * 100):
	}
}
