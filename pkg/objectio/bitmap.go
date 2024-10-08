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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	BitmapBitsInPool = 8192
)

var BitmapPool = fileservice.NewPool(
	128,
	func() *bitmap.Bitmap {
		var bm bitmap.Bitmap
		bm.InitWithSize(BitmapBitsInPool)
		return &bm
	},
	func(bm *bitmap.Bitmap) {
		bm.Clear()
	},
	nil,
)

var NullReusableBitmap ReusableBitmap

type ReusableBitmap struct {
	bm  *bitmap.Bitmap
	put func()
	idx int
}

func (r *ReusableBitmap) Idx() int {
	return r.idx
}

func (r *ReusableBitmap) Release() {
	if r.bm != nil {
		r.bm = nil
	}
	if r.put != nil {
		r.put()
		r.put = nil
	}
	r.idx = 0
}

func (r *ReusableBitmap) OrBitmap(o *bitmap.Bitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.PreExtend(int(o.Len()))
	r.bm.Or(o)
}

func (r *ReusableBitmap) Reusable() bool {
	return r.put != nil
}

func (r *ReusableBitmap) Or(o ReusableBitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.PreExtend(int(o.bm.Len()))
	r.bm.Or(o.bm)
}

func (r *ReusableBitmap) Add(i uint64) {
	if r == nil || r.bm == nil {
		logutil.Fatal("invalid bitmap")
	}
	r.PreExtend(int(i) + 1)
	r.bm.Add(i)
}

func (r *ReusableBitmap) PreExtend(nbits int) {
	if r.bm.Len() >= int64(nbits) {
		return
	}
	if r.put != nil {
		logutil.Warn(
			"ReusableBitmap-COW",
			zap.Int("nbits", nbits),
		)
		var nbm bitmap.Bitmap
		nbm.TryExpandWithSize(nbits)
		nbm.Or(r.bm)
		r.Release()
		r.bm = &nbm
		return
	}

	r.bm.TryExpandWithSize(nbits)
}

func (r *ReusableBitmap) ToI64Array() []int64 {
	if r.IsEmpty() {
		return nil
	}
	return r.bm.ToI64Array()
}

func (r *ReusableBitmap) ToArray() []uint64 {
	if r.IsEmpty() {
		return nil
	}
	return r.bm.ToArray()
}

func (r *ReusableBitmap) IsEmpty() bool {
	return r.bm == nil || r.bm.IsEmpty()
}

func (r *ReusableBitmap) Clear() {
	if r.bm != nil {
		r.bm.Clear()
	}
}

func (r *ReusableBitmap) Count() int {
	if r.bm == nil {
		return 0
	}
	return r.bm.Count()
}

func (r *ReusableBitmap) Contains(i uint64) bool {
	if r.bm == nil {
		return false
	}
	return r.bm.Contains(i)
}

func (r *ReusableBitmap) IsValid() bool {
	return r != nil && r.bm != nil
}

func GetReusableBitmap() ReusableBitmap {
	var bm *bitmap.Bitmap
	put := BitmapPool.Get(&bm)
	return ReusableBitmap{
		bm:  bm,
		put: put.Put,
		idx: put.Idx(),
	}
}

func GetReusableBitmapNoReuse() ReusableBitmap {
	return ReusableBitmap{
		bm: &bitmap.Bitmap{},
	}
}
