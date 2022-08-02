// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
)

func ApplyUpdates(vec Vector, mask *roaring.Bitmap, vals map[uint32]any) {
	it := mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		vec.Update(int(row), vals[row])
	}
}

func FillBufferWithBytes(bs *Bytes, buffer *bytes.Buffer) *Bytes {
	buffer.Reset()
	offBuf := bs.OffsetBuf()
	lenBuf := bs.LengthBuf()
	dataBuf := bs.Data
	size := len(offBuf) + len(lenBuf) + len(dataBuf)
	if buffer.Cap() < size {
		buffer.Grow(size)
	}
	nbs := NewBytes()
	buf := buffer.Bytes()[:size]
	copy(buf, dataBuf)
	nbs.Data = buf[:len(dataBuf)]
	if len(offBuf) == 0 {
		return nbs
	}
	copy(buf[len(dataBuf):], offBuf)
	copy(buf[len(dataBuf)+len(offBuf):], lenBuf)
	nbs.SetOffsetBuf(buf[len(dataBuf) : len(dataBuf)+len(offBuf)])
	nbs.SetLengthBuf(buf[len(dataBuf)+len(offBuf) : size])
	return nbs
}

func CloneWithBuffer(src Vector, buffer *bytes.Buffer, allocator ...MemAllocator) (cloned Vector) {
	opts := new(Options)
	if len(allocator) > 0 {
		opts.Allocator = DefaultAllocator
	} else {
		opts.Allocator = src.GetAllocator()
	}
	cloned = MakeVector(src.GetType(), src.Nullable(), opts)
	bs := src.Bytes()
	var nulls *roaring64.Bitmap
	if src.HasNull() {
		nulls = src.NullMask().Clone()
	}
	nbs := FillBufferWithBytes(bs, buffer)
	cloned.ResetWithData(nbs, nulls)
	return
}
