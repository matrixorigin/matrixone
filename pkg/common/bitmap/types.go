// +gobra

// Copyright 2021 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/common/mpool"

type Iterator interface {
	HasNext() bool
	Next() uint64
	PeekNext() uint64
}

type Bitmap struct {
	// popCount is the number of set bits.
	popCount int64
	// numBits is the logical length in bits. Bits at or beyond numBits are
	// never read; words missing from data read as zero.
	numBits int64
	data    []uint64
	// mp owns data. A bitmap with a nil mp is read-only: its words, if any,
	// belong to someone else (a decoded buffer, the FileService cache, or the
	// bitmap a view was taken from), and every write panics.
	mp *mpool.MPool
	// shift is the bit of data[0] that holds row 0. It is nonzero only for a
	// read-only view of another bitmap (InitView); row r is bit r+shift of
	// data. A view's words may hold set bits before shift and at or beyond
	// shift+numBits, so readers mask at Len.
	shift uint64
}

type BitmapIterator struct {
	// i is the position of the next set bit, or bm.numBits when exhausted.
	i  uint64
	bm *Bitmap
}
