// Copyright 2026 Matrix Origin
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

import "sync/atomic"

type waiterEnqueuedHook func(tableID uint64, waiterTxnID []byte, holderTxnIDs [][]byte)

var waiterEnqueuedHookForTest atomic.Pointer[waiterEnqueuedHook]

// SetWaiterEnqueuedHookForTest observes the point at which a lock request has
// been linked to its conflicting holders and enqueued as a blocking waiter.
// The callback runs while the local lock table mutex is held and must not block.
func SetWaiterEnqueuedHookForTest(hook func(tableID uint64, waiterTxnID []byte, holderTxnIDs [][]byte)) func() {
	previous := waiterEnqueuedHookForTest.Load()
	if hook == nil {
		waiterEnqueuedHookForTest.Store(nil)
	} else {
		value := waiterEnqueuedHook(hook)
		waiterEnqueuedHookForTest.Store(&value)
	}
	return func() { waiterEnqueuedHookForTest.Store(previous) }
}

func notifyWaiterEnqueuedForTest(tableID uint64, waiterTxnID []byte, holderTxnIDs [][]byte) {
	hook := waiterEnqueuedHookForTest.Load()
	if hook == nil {
		return
	}
	(*hook)(tableID, waiterTxnID, holderTxnIDs)
}
