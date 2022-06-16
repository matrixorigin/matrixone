package common

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
)

func BM32Window(bm *roaring.Bitmap, start, end int) *roaring.Bitmap {
	new := roaring.NewBitmap()
	if bm == nil || bm.GetCardinality() == 0 {
		return new
	}
	iterator := bm.Iterator()
	for iterator.HasNext() {
		n := iterator.Next()
		if uint32(start) <= n {
			if n >= uint32(end) {
				break
			}
			new.Add(n - uint32(start))
		}
	}
	return new
}

func BM64Window(bm *roaring64.Bitmap, start, end int) *roaring64.Bitmap {
	new := roaring64.NewBitmap()
	if bm == nil || bm.GetCardinality() == 0 {
		return new
	}
	iterator := bm.Iterator()
	for iterator.HasNext() {
		n := iterator.Next()
		if uint64(start) <= n {
			if n >= uint64(end) {
				break
			}
			new.Add(n - uint64(start))
		}
	}
	return new
}
