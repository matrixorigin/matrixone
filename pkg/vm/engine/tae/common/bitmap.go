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

package common

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

func BM32Window(bm *roaring.Bitmap, start, end int) *roaring.Bitmap {
	new := roaring.NewBitmap()
	if bm == nil || bm.IsEmpty() {
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
	if bm == nil || bm.IsEmpty() {
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

func BitmapEqual(v1, v2 *nulls.Bitmap) bool {
	if v1 == nil || v2 == nil {
		return v1 == v2
	}
	if v1.GetCardinality() != v2.GetCardinality() {
		return false
	}
	vals1 := v1.ToArray()
	vals2 := v2.ToArray()
	for i := range vals1 {
		if vals1[i] != vals2[i] {
			return false
		}
	}
	return true
}

func RoaringToMOBitmap(bm *roaring.Bitmap) *nulls.Bitmap {
	if bm == nil {
		return nil
	}
	nbm := nulls.NewWithSize(int(bm.Maximum()) + 1)
	iterator := bm.Iterator()
	for iterator.HasNext() {
		nbm.Add(uint64(iterator.Next()))
	}
	return nbm
}

func MOOrRoaringBitmap(bm *nulls.Bitmap, rbm *roaring.Bitmap) {
	if bm == nil || rbm == nil {
		return
	}
	iterator := rbm.Iterator()
	for iterator.HasNext() {
		bm.Add(uint64(iterator.Next()))
	}
}
