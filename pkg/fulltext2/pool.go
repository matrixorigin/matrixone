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

import "sync"

// Per-query cursor block buffers are BlockSize-sized and short-lived: buildWandIters
// makes one pair per term and newPhraseCursor one triple per slot, then they die when
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

// phraseBuf is a phraseCursor's decoded-block buffers: docIDs + per-doc positions + a
// tf scratch (phrase ignores tf but fillBlock still writes it).
type phraseBuf struct {
	docs []int64
	pos  [][]int32
	tfs  []uint8
}

var phraseBufPool = sync.Pool{
	New: func() any {
		return &phraseBuf{
			docs: make([]int64, BlockSize),
			pos:  make([][]int32, BlockSize),
			tfs:  make([]uint8, BlockSize),
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
		c.tfbuf = nil
	}
}
