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

type Iterator interface {
	HasNext() bool
	Next() uint64
	PeekNext() uint64
}

type Bitmap struct {
	// popCount is the number of set bits. Version 1 stored an int32 emptyFlag here.
	popCount int64
	// numBits is the logical length in bits. Bits at or beyond numBits are
	// never read; words missing from data read as zero.
	numBits int64
	data    []uint64
}

type BitmapIterator struct {
	// i is the position of the next set bit, or bm.numBits when exhausted.
	i  uint64
	bm *Bitmap
}
