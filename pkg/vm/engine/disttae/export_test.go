// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import "github.com/matrixorigin/matrixone/pkg/common/mpool"

// SwapCommitPayloadPoolForTest makes later commits take their payload
// buffers from mp, with an empty recycle list; restore puts the original
// pool and list back.
func SwapCommitPayloadPoolForTest(mp *mpool.MPool) (restore func()) {
	oldPool := commitPayloadPool
	commitPayloads.mu.Lock()
	oldFree := commitPayloads.free
	commitPayloads.free = nil
	commitPayloads.mu.Unlock()
	commitPayloadPool = mp
	return func() {
		FreeIdleCommitPayloadsForTest()
		commitPayloadPool = oldPool
		commitPayloads.mu.Lock()
		commitPayloads.free = oldFree
		commitPayloads.mu.Unlock()
	}
}

// IdleCommitPayloadsForTest returns the number of recycled payload buffers.
func IdleCommitPayloadsForTest() int {
	commitPayloads.mu.Lock()
	defer commitPayloads.mu.Unlock()
	return len(commitPayloads.free)
}

// FreeIdleCommitPayloadsForTest frees the recycled payload buffers.
func FreeIdleCommitPayloadsForTest() {
	commitPayloads.mu.Lock()
	defer commitPayloads.mu.Unlock()
	for _, buf := range commitPayloads.free {
		buf.Free()
	}
	commitPayloads.free = nil
}

// HoldsCommitPayloadForTest reports whether txn holds a payload buffer.
func HoldsCommitPayloadForTest(txn *Transaction) bool {
	return txn.commitPayload.Load() != nil
}
