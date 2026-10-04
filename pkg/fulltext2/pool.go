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

package fulltext2

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// Per-query cursor block buffers are BlockSize-sized and short-lived: buildWandIters
// makes one pair per term and newPhraseCursor one pair per slot, then they die when
// the search returns. Under concurrent query load that churn outruns the GC, so the
// buffers are recycled through sync.Pool instead of freshly allocated per query. Each
// buffer is Get by exactly one (single-goroutine) search and Put back on return; the
// returned Result/docTf values copy out pk+score and never alias the buffers, so there
// is no use-after-Put. Reused buffers may hold stale entries past the current block's
// blen, but cursors only ever read within [0,blen), so staleness is invisible.

// wandBuf is a wandIter's decoded-block buffer pair (docIDs + capped tfs).
type wandBuf struct {
	docs []int64 // cap BlockSize
	tfs  []uint8 // cap BlockSize
}

var wandBufPool = sync.Pool{
	New: func() any {
		return &wandBuf{docs: make([]int64, BlockSize), tfs: make([]uint8, BlockSize)}
	},
}

func getWandBuf() *wandBuf { return wandBufPool.Get().(*wandBuf) }

// releaseWandIters returns every cursor's block buffer to the pool. Safe to defer right
// after buildWandIters (a nil/empty slice is a no-op) and idempotent per cursor (buf is
// cleared so a double release cannot double-Put the same buffer).
func releaseWandIters(iters []*wandIter) {
	for _, it := range iters {
		if it == nil || it.buf == nil {
			continue
		}
		wandBufPool.Put(it.buf)
		it.buf = nil
		it.bDocs = nil
		it.bTfs = nil
	}
}

// phraseBuf is a phraseCursor's decoded-block buffers: docIDs + per-doc positions.
type phraseBuf struct {
	docs []int64
	pos  [][]int32
}

var phraseBufPool = sync.Pool{
	New: func() any {
		return &phraseBuf{
			docs: make([]int64, BlockSize),
			pos:  make([][]int32, BlockSize),
		}
	},
}

func getPhraseBuf() *phraseBuf { return phraseBufPool.Get().(*phraseBuf) }

// releasePhraseCursors returns every cursor's block buffers to the pool; skips nil
// entries so it is safe to defer over a partially-built cursors slice (matchPhraseCursor
// bails out mid-build when a slot's term is absent).
func releasePhraseCursors(cursors []*phraseCursor) {
	for _, c := range cursors {
		if c == nil || c.buf == nil {
			continue
		}
		phraseBufPool.Put(c.buf)
		c.buf = nil
		c.bDocs = nil
		c.bPos = nil
	}
}

// The no-LIMIT streaming path emits one vectorindex.ColumnBuffer per streamBatch rows.
// Its Data backing is recycled here so a million-row result does not allocate a fresh
// buffer per batch. GC-managed: a buffer never Put back (an aborted/drained query) is
// just reclaimed — a missed reuse, not a leak. The struct lives in vectorindex (the Emit
// contract references it); the POOLING POLICY lives here with its only user, fulltext2.
var columnBufferPool = sync.Pool{New: func() any { return &vectorindex.ColumnBuffer{} }}

// GetColumnBuffer returns a reset ColumnBuffer of pk type t. The streaming producer Gets
// one per batch; the consumer PutColumnBuffers it back once it has copied Data out.
func GetColumnBuffer(t types.T) *vectorindex.ColumnBuffer {
	k := columnBufferPool.Get().(*vectorindex.ColumnBuffer)
	k.Type = t
	k.Reset()
	return k
}

// PutColumnBuffer returns a ColumnBuffer to the pool. Call only after the batch is fully
// consumed (its Data copied out); the buffer must not be referenced afterward. Exported
// because the consumer (pkg/sql/colexec/table_function) returns the batch it received.
func PutColumnBuffer(k *vectorindex.ColumnBuffer) {
	if k != nil {
		columnBufferPool.Put(k)
	}
}
