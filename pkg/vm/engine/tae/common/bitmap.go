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
