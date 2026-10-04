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

package colexec

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// NewAccountedBitmap creates a bitmap whose complete backing capacity belongs
// to the statement allocation account. The caller owns the returned bitmap
// until FreeAccountedBitmap or an explicit ownership transfer.
func NewAccountedBitmap(
	rows int64,
	mp *mpool.MPool,
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	site mpool.AllocationSite,
) (*bitmap.Bitmap, error) {
	if rows < 0 || rows > math.MaxInt64-63 || mp == nil || account == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	words := (rows + 63) / 64
	if words > int64(math.MaxInt) {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	storage, err := mpool.MakeSliceAccounted[uint64](
		int(words),
		mp,
		account,
		owner,
		site,
	)
	if err != nil {
		return nil, err
	}
	value := new(bitmap.Bitmap)
	value.InstallExternalStorage(storage)
	value.InitWithSize(rows)
	return value, nil
}

func FreeAccountedBitmap(value *bitmap.Bitmap, mp *mpool.MPool) {
	if value == nil || !value.HasExternalStorage() {
		return
	}
	storage := value.ReleaseExternalStorage()
	if cap(storage) > 0 {
		mpool.FreeSlice(mp, storage)
	}
}
