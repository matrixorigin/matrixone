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
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
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
	size := bs.Size()
	if buffer.Cap() < size {
		buffer.Grow(size)
	}
	nbs := NewBytes()
	nbs.FixedTypeSize = bs.FixedTypeSize
	nbs.IsFixedType = bs.IsFixedType
	buf := buffer.Bytes()[:size]
	copy(buf, bs.StorageBuf())
	copy(buf[bs.StorageSize():], bs.HeaderBuf())

	nbs.SetStorageBuf(buf[:bs.StorageSize()])
	nbs.SetHeaderBuf(buf[bs.StorageSize():bs.Size()])
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

func CopyToMoVector(vec Vector) *movec.Vector {
	return VectorsToMO(vec)
}

// XXX VectorsToMo and CopyToMoVector.   The old impl. will move
// vec.Data to movec.Data and keeps on sharing.   This is way too
// fragile and error prone.
//
// Not just copy it.   Until profiler says I need to work harder.
func VectorsToMO(vec Vector) *movec.Vector {
	mov := movec.NewOriginal(vec.GetType())
	data := vec.Data()
	typ := vec.GetType()
	mov.Typ = typ
	if vec.HasNull() {
		mov.Nsp.Np = bitmap.New(vec.Length())
		mov.Nsp.Np.AddMany(vec.NullMask().ToArray())
		//mov.Nsp.Np = vec.NullMask()
	}

	if vec.GetType().IsVarlen() {
		bs := vec.Bytes()
		nbs := len(bs.Offset)
		bsv := make([][]byte, nbs)
		for i := 0; i < nbs; i++ {
			bsv[i] = bs.Data[bs.Offset[i] : bs.Offset[i]+bs.Length[i]]
		}
		movec.AppendBytes(mov, bsv, nil)
	} else if vec.GetType().IsTuple() {
		cnt := types.DecodeInt32(data)
		if cnt != 0 {
			if err := types.Decode(data, &mov.Col); err != nil {
				panic(any(err))
			}
		}
	} else {
		movec.AppendFixedRaw(mov, data)
	}

	return mov
}

func CopyToMoVectors(vecs []Vector) []*movec.Vector {
	movecs := make([]*movec.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVector(vecs[i])
	}
	return movecs
}
