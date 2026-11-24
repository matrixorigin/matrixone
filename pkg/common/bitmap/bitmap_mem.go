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

package bitmap

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

// Word storage and ownership.
//
// A bitmap's words come from its pool, off-heap, and go back to it in Free.
// A bitmap without a pool is read-only: it can reference words it does not
// own (UnmarshalNoCopy, InitView) but never allocates or writes them.
//
// Owned words always start at the allocation's first byte, so data[:1]
// identifies the block for mpool.Free. Code must never reslice an owned
// data from the front.

// ErrReadOnly is the panic value of a write to a read-only bitmap.
var ErrReadOnly = moerr.NewInternalErrorNoCtx("bitmap is read-only: it has no mpool")

func errNoPool() error {
	return moerr.NewInternalErrorNoCtx("bitmap is read-only: it has no mpool to allocate from")
}

// MPool returns the pool that owns n's words, or nil for a read-only bitmap.
func (n *Bitmap) MPool() *mpool.MPool {
	if n == nil {
		return nil
	}
	return n.mp
}

// IsReadOnly reports whether n has no pool, and so cannot be written.
func (n *Bitmap) IsReadOnly() bool {
	return n == nil || n.mp == nil
}

// Init gives an empty bitmap the pool it allocates from. n must hold no
// words; a bitmap that already has mp keeps it.
func (n *Bitmap) Init(mp *mpool.MPool) {
	if n.mp == mp {
		return
	}
	if n.data != nil {
		panic(moerr.NewInternalErrorNoCtx("bitmap Init: the bitmap still holds words"))
	}
	n.mp = mp
}

// prepareInit readies n for an Init* call that names mp. A read-only bitmap
// drops the words it references and takes mp; an owned bitmap must already
// belong to mp, and keeps its words for reuse.
func (n *Bitmap) prepareInit(mp *mpool.MPool) error {
	if mp == nil {
		return errNoPool()
	}
	if n.mp == nil {
		n.data = nil
		n.shift = 0
		n.mp = mp
		return nil
	}
	if n.mp != mp {
		return moerr.NewInternalErrorNoCtx("bitmap: mpool does not match the bitmap's pool")
	}
	return nil
}

// mustWrite panics when n is read-only. Every method that changes bits
// calls it before the first write.
func (n *Bitmap) mustWrite() {
	if n.mp == nil {
		panic(ErrReadOnly)
	}
}

// Free returns n's words to its pool and leaves n empty, keeping the pool.
// A read-only bitmap only drops its reference. Free is idempotent.
func (n *Bitmap) Free() {
	if n == nil {
		return
	}
	if n.mp != nil && cap(n.data) > 0 {
		n.mp.Free(wordBytes(n.data[:1]))
	}
	n.data = nil
	n.numBits = 0
	n.popCount = 0
	n.shift = 0
}

func wordBytes(words []uint64) []byte {
	if cap(words) == 0 {
		return nil
	}
	return util.UnsafeSliceCast[byte](words)
}

func bytesWords(b []byte) []uint64 {
	if cap(b) < 8 {
		return nil
	}
	return util.UnsafeSliceCast[uint64](b)
}

// allocWords returns words zeroed words with capacity for at least capWords
// from n's pool. It does not touch n.data.
func (n *Bitmap) allocWords(words, capWords int) ([]uint64, error) {
	if capWords < words {
		capWords = words
	}
	if capWords == 0 {
		return nil, nil
	}
	if n.mp == nil {
		return nil, errNoPool()
	}
	b, err := n.mp.Alloc(capWords*8, true)
	if err != nil {
		return nil, err
	}
	w := bytesWords(b)
	clear(w[:cap(w)])
	return w[:words], nil
}

// replaceWords installs words as n's storage, freeing the old block.
func (n *Bitmap) replaceWords(words []uint64) {
	if n.mp != nil && cap(n.data) > 0 {
		n.mp.Free(wordBytes(n.data[:1]))
	}
	n.data = words
}
