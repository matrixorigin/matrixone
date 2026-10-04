// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashbuild

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

func makeDedupSlice[T any](
	hb *HashmapBuilder,
	n int,
	mp *mpool.MPool,
	site mpool.AllocationSite,
) ([]T, error) {
	if n < 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if hb.mapAllocationAccount == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return mpool.MakeSliceAccounted[T](
		n,
		mp,
		hb.mapAllocationAccount,
		mpool.AllocationOwnerHashBuild,
		site,
	)
}

func freeDedupSlice[T any](hb *HashmapBuilder, values []T, mp *mpool.MPool) {
	if cap(values) > 0 {
		mpool.FreeSlice(mp, values)
	}
}

func (hb *HashmapBuilder) newDedupBitmap(
	rows int,
	mp *mpool.MPool,
	site mpool.AllocationSite,
) (*bitmap.Bitmap, error) {
	if rows < 0 || rows > math.MaxInt-63 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	bm := &bitmap.Bitmap{}
	if hb.mapAllocationAccount == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	words := (rows + 63) / 64
	storage, err := mpool.MakeSliceAccounted[uint64](
		words,
		mp,
		hb.mapAllocationAccount,
		mpool.AllocationOwnerHashBuild,
		site,
	)
	if err != nil {
		return nil, err
	}
	bm.InstallExternalStorage(storage)
	bm.InitWithSize(int64(rows))
	return bm, nil
}

func releaseDedupBitmap(bm *bitmap.Bitmap, mp *mpool.MPool) {
	if bm == nil || !bm.HasExternalStorage() {
		return
	}
	storage := bm.ReleaseExternalStorage()
	if cap(storage) > 0 {
		mpool.FreeSlice(mp, storage)
	}
}

func (hb *HashmapBuilder) freeIgnoreRows(mp *mpool.MPool) {
	releaseDedupBitmap(hb.IgnoreRows, mp)
	hb.IgnoreRows = nil
}

func (hb *HashmapBuilder) freeDelRows(mp *mpool.MPool) {
	releaseDedupBitmap(hb.DelRows, mp)
	hb.DelRows = nil
}
