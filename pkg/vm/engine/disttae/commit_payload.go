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

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// commitPayloadPool accounts the commit payload buffers.
var commitPayloadPool = mpool.MustNew("cn_commit_payload")

const (
	// maxRetainedCommitPayload bounds a recycled buffer; a larger one is
	// freed when its commit ends.
	maxRetainedCommitPayload = 8 << 20
	// maxFreeCommitPayloads bounds the recycled buffers kept for later
	// commits.
	maxFreeCommitPayloads = 32
)

// commitPayloads recycles the off-heap buffers that commit payloads are
// marshaled into. A sync.Pool cannot hold them: it would drop a buffer
// without freeing it.
var commitPayloads payloadBuffers

type payloadBuffers struct {
	mu   sync.Mutex
	free []*mpool.MessageBuffer
}

func (p *payloadBuffers) get() *mpool.MessageBuffer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.free); n > 0 {
		buf := p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
		return buf
	}
	buf := mpool.NewMessageBuffer(commitPayloadPool, maxRetainedCommitPayload)
	return &buf
}

func (p *payloadBuffers) put(buf *mpool.MessageBuffer) {
	buf.Reset()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.free) < maxFreeCommitPayloads {
		p.free = append(p.free, buf)
		return
	}
	buf.Free()
}

// holdCommitPayload makes buf the transaction's commit payload buffer. The
// commit requests point into it, so it is released only by the terminal
// hooks — FinalizeCommit, FinalizeCommitWithUnknownResult and Rollback —
// which the txn operator runs after the commit RPC has returned. By then
// the transport is done with the requests: the sender's Get and stream Send
// return only after the request was written or dropped. A transaction is
// committed at most once, but a buffer left by an earlier attempt is
// released here.
func (txn *Transaction) holdCommitPayload(buf *mpool.MessageBuffer) {
	if old := txn.commitPayload.Swap(buf); old != nil {
		commitPayloads.put(old)
	}
}

// releaseCommitPayload returns the commit payload buffer, if any, for reuse.
// The txn operator never runs a terminal hook while the commit RPC is in
// flight: Commit and Rollback share one terminal owner.
func (txn *Transaction) releaseCommitPayload() {
	if buf := txn.commitPayload.Swap(nil); buf != nil {
		commitPayloads.put(buf)
	}
}
