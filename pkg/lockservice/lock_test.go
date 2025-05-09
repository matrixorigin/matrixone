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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestNewRowLock(t *testing.T) {
	txnID := []byte("1")
	opts := LockOptions{}
	opts.Mode = pb.LockMode_Exclusive
	l := newRowLock(getLogger(""), &lockContext{txn: &activeTxn{txnID: txnID}, opts: opts})
	assert.True(t, l.isLockRow())
	assert.Equal(t, pb.LockMode_Exclusive, l.GetLockMode())
}

func TestNewRangeLock(t *testing.T) {
	txnID := []byte("1")
	opts := LockOptions{}
	opts.Mode = pb.LockMode_Shared
	sl, el := newRangeLock(getLogger(""), &lockContext{txn: &activeTxn{txnID: txnID}, opts: opts})

	assert.Equal(t, pb.LockMode_Shared, sl.GetLockMode())
	assert.True(t, el.isLockRangeEnd())
	assert.Equal(t, pb.LockMode_Shared, el.GetLockMode())
}

func BenchmarkHoldersContains(b *testing.B) {
	// Preparation phase: create holders and add 2000 txns
	h := newHolders()
	for i := 0; i < 2000; i++ {
		txn := pb.WaitTxn{
			TxnID: []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)},
		}
		h.add(txn)
	}

	// Reset timer to exclude preparation time
	b.ResetTimer()

	// Test searching for a non-existent txn
	txnID := []byte{0xFF, 0xFF, 0xFF, 0xFF} // A non-existent txn

	for i := 0; i < b.N; i++ {
		h.contains(txnID)
	}
}

func TestHoldersGetTxnSlice(t *testing.T) {
	// Test empty holders
	h := newHolders()
	slice := h.getTxnSlice()
	assert.Equal(t, 0, len(slice))

	// Test with multiple holders
	txn1 := pb.WaitTxn{TxnID: []byte("1")}
	txn2 := pb.WaitTxn{TxnID: []byte("2")}
	txn3 := pb.WaitTxn{TxnID: []byte("3")}

	h.add(txn1)
	h.add(txn2)
	h.add(txn3)

	slice = h.getTxnSlice()
	assert.Equal(t, 3, len(slice))

	// Create a map to check if all txns are present
	txnMap := make(map[string]bool)
	for _, txn := range slice {
		txnMap[string(txn.TxnID)] = true
	}

	assert.True(t, txnMap["1"])
	assert.True(t, txnMap["2"])
	assert.True(t, txnMap["3"])
}
