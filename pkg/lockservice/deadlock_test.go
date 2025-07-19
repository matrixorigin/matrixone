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
	"encoding/hex"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestCheckWithDeadlock(t *testing.T) {
	reuse.RunReuseTests(func() {
		txn1 := []byte("t1")
		txn2 := []byte("t2")
		txn3 := []byte("t3")
		txn4 := []byte("t4")

		// txn1 - txn2 - txn3 - txn1
		m := map[string][]pb.WaitTxn{
			string(txn1): {{TxnID: txn2}},
			string(txn2): {{TxnID: txn3}},
			string(txn3): {{TxnID: txn1}},
		}
		abortC := make(chan []byte, 1)
		defer close(abortC)

		d := newDeadlockDetector(
			runtime.DefaultRuntime().Logger(),
			func(txn pb.WaitTxn, w *waiters) (bool, error) {
				for _, v := range m[string(txn.TxnID)] {
					if !w.add(v, "") {
						return false, nil
					}
				}
				return true, nil
			}, func(txn pb.WaitTxn, err error) {
				abortC <- txn.TxnID
			})
		defer d.close()

		// txn1 - txn2 - txn3 - txn1
		assert.NoError(t, d.check(txn4, pb.WaitTxn{TxnID: txn1}))
		assert.Equal(t, txn1, <-abortC)
		d.txnClosed(txn1)

		// txn2 - txn3 - txn1 - txn2
		assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn2}))
		assert.Equal(t, txn2, <-abortC)
		d.txnClosed(txn2)

		// txn3 - txn1 - txn2 - txn3
		assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn3}))
		assert.Equal(t, txn3, <-abortC)
		d.txnClosed(txn3)

		assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn4}))
		select {
		case <-abortC:
			assert.Fail(t, "can not found dead lock")
		case <-time.After(time.Millisecond * 100):
		}
	})
}

func TestCheckWithDeadlockWith2Txn(t *testing.T) {
	reuse.RunReuseTests(func() {
		txn1 := []byte("t1")
		txn2 := []byte("t2")
		txn3 := []byte("t3")

		// txn1 - txn2 - txn1
		depends := map[string][]pb.WaitTxn{
			string(txn1): {{TxnID: txn2}},
			string(txn2): {{TxnID: txn1}},
		}
		abortC := make(chan []byte, 1)
		defer close(abortC)

		d := newDeadlockDetector(
			runtime.DefaultRuntime().Logger(),
			func(txn pb.WaitTxn, w *waiters) (bool, error) {
				for _, v := range depends[string(txn.TxnID)] {
					if !w.add(v, "") {
						return false, nil
					}
				}
				return true, nil
			}, func(txn pb.WaitTxn, err error) {
				abortC <- txn.TxnID
			})
		defer d.close()

		// txn2 - txn1 - txn2
		assert.NoError(t, d.check(txn2, pb.WaitTxn{TxnID: txn1}))
		assert.Equal(t, txn2, <-abortC)
		d.txnClosed(txn2)

		assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn2}))
		assert.Equal(t, txn2, <-abortC)
		d.txnClosed(txn2)

		assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn3}))
		select {
		case <-abortC:
			assert.Fail(t, "can not found dead lock")
		case <-time.After(time.Millisecond * 100):
		}
	})
}

func TestPrintPathFromRoot(t *testing.T) {
	// Test case 1: nil node
	assert.Equal(t, "<nil>", printPathFromRoot(nil))

	// Test case 2: single node
	txnID1 := []byte("txn1")
	node1 := newLockNode(pb.WaitTxn{TxnID: txnID1})
	expected1 := hex.EncodeToString(txnID1)
	assert.Equal(t, expected1, printPathFromRoot(node1))

	// Test case 3: two nodes (parent and child)
	txnID2 := []byte("txn2")
	node2 := node1.addChild(pb.WaitTxn{TxnID: txnID2})
	expected2 := hex.EncodeToString(txnID1) + " <= " + hex.EncodeToString(txnID2)
	assert.Equal(t, expected2, printPathFromRoot(node2))

	// Test case 4: three nodes (grandparent, parent, and child)
	txnID3 := []byte("txn3")
	node3 := node2.addChild(pb.WaitTxn{TxnID: txnID3})
	expected3 := hex.EncodeToString(txnID1) + " <= " + hex.EncodeToString(txnID2) + " <= " + hex.EncodeToString(txnID3)
	assert.Equal(t, expected3, printPathFromRoot(node3))

	// Test case 5: complex path with multiple branches
	// Create a tree like:
	//     root
	//    /    \
	//   A      B
	//  / \    /
	// C   D  E
	root := newLockNode(pb.WaitTxn{TxnID: []byte("root")})
	nodeA := root.addChild(pb.WaitTxn{TxnID: []byte("A")})
	nodeB := root.addChild(pb.WaitTxn{TxnID: []byte("B")})
	nodeC := nodeA.addChild(pb.WaitTxn{TxnID: []byte("C")})
	nodeD := nodeA.addChild(pb.WaitTxn{TxnID: []byte("D")})
	nodeE := nodeB.addChild(pb.WaitTxn{TxnID: []byte("E")})

	// Test path from root to node C
	expectedC := "726f6f74 <= 41 <= 43" // hex encoded "root <= A <= C"
	assert.Equal(t, expectedC, printPathFromRoot(nodeC))

	// Test path from root to node D
	expectedD := "726f6f74 <= 41 <= 44" // hex encoded "root <= A <= D"
	assert.Equal(t, expectedD, printPathFromRoot(nodeD))

	// Test path from root to node E
	expectedE := "726f6f74 <= 42 <= 45" // hex encoded "root <= B <= E"
	assert.Equal(t, expectedE, printPathFromRoot(nodeE))
}

func TestCheckWithComplexDeadlock(t *testing.T) {
	reuse.RunReuseTests(func() {
		// Create a more complex transaction dependency chain
		// T11 - T1 - T2 - T3 - T4 - T5 - T6 - T7 - T8 - T9 - T10 - T1 (deadlock)
		txn1 := []byte("t1")
		txn2 := []byte("t2")
		txn3 := []byte("t3")
		txn4 := []byte("t4")
		txn5 := []byte("t5")
		txn6 := []byte("t6")
		txn7 := []byte("t7")
		txn8 := []byte("t8")
		txn9 := []byte("t9")
		txn10 := []byte("t10")
		txn11 := []byte("t11")

		// Define the dependency map
		depends := map[string][]pb.WaitTxn{
			string(txn1):  {{TxnID: txn2}},
			string(txn2):  {{TxnID: txn3}},
			string(txn3):  {{TxnID: txn4}},
			string(txn4):  {{TxnID: txn5}},
			string(txn5):  {{TxnID: txn6}},
			string(txn6):  {{TxnID: txn7}},
			string(txn7):  {{TxnID: txn8}},
			string(txn8):  {{TxnID: txn9}},
			string(txn9):  {{TxnID: txn10}},
			string(txn10): {{TxnID: txn1}},
			string(txn11): {{TxnID: txn1}},
		}

		// Channel to receive aborted transactions
		abortC := make(chan []byte, 1)
		defer close(abortC)

		// Create the deadlock detector
		d := newDeadlockDetector(
			runtime.DefaultRuntime().Logger(),
			func(txn pb.WaitTxn, w *waiters) (bool, error) {
				for _, v := range depends[string(txn.TxnID)] {
					if !w.add(v, "") {
						return false, nil
					}
				}
				return true, nil
			}, func(txn pb.WaitTxn, err error) {
				abortC <- txn.TxnID
			})
		defer d.close()

		// Test case 1: Start with txn1, should detect deadlock and abort txn1
		assert.NoError(t, d.check([]byte("txn0"), pb.WaitTxn{TxnID: txn1}))
		assert.Equal(t, txn1, <-abortC)
		d.txnClosed(txn1)

		// Test case 2: Start with txn5, should detect deadlock and abort txn5
		assert.NoError(t, d.check([]byte("txn0"), pb.WaitTxn{TxnID: txn5}))
		assert.Equal(t, txn5, <-abortC)
		d.txnClosed(txn5)

		// Test case 3: Start with txn10, should detect deadlock and abort txn10
		assert.NoError(t, d.check([]byte("txn0"), pb.WaitTxn{TxnID: txn10}))
		assert.Equal(t, txn10, <-abortC)
		d.txnClosed(txn10)

		// Test case 3: Start with txn11, should detect deadlock and abort txn11
		assert.NoError(t, d.check([]byte("txn0"), pb.WaitTxn{TxnID: txn11}))
		assert.Equal(t, txn1, <-abortC)
		d.txnClosed(txn1)

		// Test case 5: Break the cycle by removing txn10's dependency on txn1
		depends[string(txn10)] = []pb.WaitTxn{} // Remove the dependency that creates the cycle

		// Now starting with txn1 should not detect a deadlock
		assert.NoError(t, d.check(nil, pb.WaitTxn{TxnID: txn1}))
		select {
		case <-abortC:
			assert.Fail(t, "should not detect deadlock after breaking the cycle")
		case <-time.After(time.Millisecond * 100):
			// Expected: no deadlock detected
		}
	})
}

func TestCheckDeadlock(t *testing.T) {
	reuse.RunReuseTests(func() {
		txn1 := []byte("t1")
		txn2 := []byte("t2")
		txn3 := []byte("t3")
		txn4 := []byte("t4")
		txn5 := []byte("t5")
		txn6 := []byte("t6")
		txn7 := []byte("t7")
		txn8 := []byte("t8")
		txn9 := []byte("t9")
		txn10 := []byte("t10")

		// Define the dependency map
		// T1 - T2 - T3 - T4 - T5 - T6 - T7 - T8 - T9 - T10 - T2 (deadlock)
		depends := map[string][]pb.WaitTxn{
			string(txn1):  {{TxnID: txn2}},
			string(txn2):  {{TxnID: txn3}},
			string(txn3):  {{TxnID: txn4}},
			string(txn4):  {{TxnID: txn5}},
			string(txn5):  {{TxnID: txn6}},
			string(txn6):  {{TxnID: txn7}},
			string(txn7):  {{TxnID: txn8}},
			string(txn8):  {{TxnID: txn9}},
			string(txn9):  {{TxnID: txn10}},
			string(txn10): {{TxnID: txn2}},
		}

		// Channel to receive aborted transactions
		abortC := make(chan []byte, 1)
		defer close(abortC)

		// Create the deadlock detector
		d := newDeadlockDetector(
			runtime.DefaultRuntime().Logger(),
			func(txn pb.WaitTxn, w *waiters) (bool, error) {
				for _, v := range depends[string(txn.TxnID)] {
					if !w.add(v, "") {
						return false, nil
					}
				}
				return true, nil
			}, func(txn pb.WaitTxn, err error) {
				abortC <- txn.TxnID
			})
		defer d.close()

		// Test case 1: Start with txn1, should detect deadlock and abort txn1
		assert.NoError(t, d.check([]byte("txn0"), pb.WaitTxn{TxnID: txn1}))
		assert.Equal(t, txn2, <-abortC)
		d.txnClosed(txn2)
	})
}
