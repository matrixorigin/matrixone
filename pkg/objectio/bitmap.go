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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	BitmapBitsInPool = 8192
	BitmapPoolSize   = 64
)

func BitmapPoolReport() string {
	return fmt.Sprintf(
		"Runtime-State-BitmapPool-Report: [%d/%d]",
		BitmapPool.InUseCount(),
		BitmapPoolSize,
	)
}

var BitmapPool = fileservice.NewPool(
	BitmapPoolSize,
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

var NullBitmap Bitmap

type Bitmap struct {
	bm  *bitmap.Bitmap
	put func()
	idx int
}

func (r *Bitmap) Idx() int {
	return r.idx
}

func (r *Bitmap) Release() {
	if r.bm != nil {
		r.bm = nil
	}
	if r.put != nil {
		r.put()
		r.put = nil
	}
	r.idx = 0
}

func (r *Bitmap) OrBitmap(o *bitmap.Bitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.PreExtend(int(o.Len()))
	r.bm.Or(o)
}

func (r *Bitmap) Reusable() bool {
	return r.put != nil
}

func (r *Bitmap) Bitmap() *bitmap.Bitmap {
	return r.bm
}

func (r *Bitmap) Or(o Bitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.PreExtend(int(o.bm.Len()))
	r.bm.Or(o.bm)
}

func (r *Bitmap) Add(i uint64) {
	if r == nil || r.bm == nil {
		logutil.Fatal("invalid bitmap")
	}
	r.PreExtend(int(i) + 1)
	r.bm.Add(i)
}

func (r *Bitmap) PreExtend(nbits int) {
	if r.bm.Len() >= int64(nbits) {
		return
	}
	if r.put != nil {
		logutil.Warn(
			"Bitmap-COW",
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

func (r *Bitmap) ToI64Array() []int64 {
	if r.IsEmpty() {
		return nil
	}
	return r.bm.ToI64Array()
}

func (r *Bitmap) ToArray() []uint64 {
	if r.IsEmpty() {
		return nil
	}
	return r.bm.ToArray()
}

func (r *Bitmap) IsEmpty() bool {
	return r.bm == nil || r.bm.IsEmpty()
}

func (r *Bitmap) Clear() {
	if r.bm != nil {
		r.bm.Clear()
	}
}

func (r *Bitmap) Count() int {
	if r.bm == nil {
		return 0
	}
	return r.bm.Count()
}

func (r *Bitmap) Contains(i uint64) bool {
	if r.bm == nil {
		return false
	}
	return r.bm.Contains(i)
}

func (r *Bitmap) IsValid() bool {
	return r != nil && r.bm != nil
}

func GetReusableBitmap() Bitmap {
	var bm *bitmap.Bitmap
	put := BitmapPool.Get(&bm)
	return Bitmap{
		bm:  bm,
		put: put.Put,
		idx: put.Idx(),
	}
}

func GetNoReuseBitmap() Bitmap {
	return Bitmap{
		bm: &bitmap.Bitmap{},
	}
}
