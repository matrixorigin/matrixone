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

func FillBufferWithBytes(bs *Bytes, buffer *bytes.Buffer) {
	buffer.Reset()
	offBuf := bs.OffsetBuf()
	lenBuf := bs.LengthBuf()
	dataBuf := bs.Data
	size := len(offBuf) + len(lenBuf) + len(dataBuf)
	if buffer.Cap() < size {
		buffer.Grow(size)
	}
	buf := buffer.Bytes()[:size]
	copy(buf, dataBuf)
	if len(offBuf) == 0 {
		return
	}
	copy(buf[len(dataBuf):], offBuf)
	copy(buf[len(dataBuf)+len(offBuf):], lenBuf)
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
	FillBufferWithBytes(bs, buffer)
	cloned.ResetWithData(bs, nulls)
	return
}
