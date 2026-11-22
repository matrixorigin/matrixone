// Copyright 2026 Matrix Origin
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

package vector

import (
	"errors"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func allocationAccountInvalid(message string) error {
	return errors.Join(
		mpool.ErrAllocationAccountInvalid,
		moerr.NewInternalErrorNoCtx(message),
	)
}

func (v *Vector) hasBackingStorage() bool {
	return cap(v.data) != 0 ||
		cap(v.area) != 0 ||
		v.nsp.GetBitmap().Size() != 0 ||
		v.gsp.GetBitmap().Size() != 0
}

// hasOwnedBackingStorage reports storage that UnmarshalBinary cannot replace
// without losing an MPool-owned allocation. Data and area marked cantFree are
// borrowed aliases; bitmap backing is always Go-owned and remains GC-visible
// after replacement.
func (v *Vector) hasOwnedBackingStorage() bool {
	return cap(v.data) != 0 && !v.cantFreeData ||
		cap(v.area) != 0 && !v.cantFreeArea
}

// Bitmaps always use Go-owned memory. When a vector grows, it reserves room in
// the bitmaps it uses for all of its rows; a bitmap also grows itself on write.

func validBitmapRows(rows int) error {
	if rows < 0 || rows > math.MaxInt-63 {
		return moerr.NewInvalidInputNoCtxf("invalid bitmap row count %d", rows)
	}
	return nil
}

func (v *Vector) ensureBitmapCapacity(rows int, mp *mpool.MPool) error {
	if err := v.ensureNullCapacity(rows, mp); err != nil {
		return err
	}
	return v.ensureGroupingCapacity(rows, mp)
}

func (v *Vector) ensureNullCapacity(rows int, _ *mpool.MPool) error {
	if err := validBitmapRows(rows); err != nil {
		return err
	}
	v.nsp.GetBitmap().Reserve(rows)
	return nil
}

func (v *Vector) ensureGroupingCapacity(rows int, _ *mpool.MPool) error {
	if err := validBitmapRows(rows); err != nil {
		return err
	}
	v.gsp.GetBitmap().Reserve(rows)
	return nil
}

func (v *Vector) allocData(mp *mpool.MPool, size int) ([]byte, error) {
	return v.allocOwned(mp, size, v.offHeap, true)
}

func (v *Vector) allocArea(mp *mpool.MPool, size int) ([]byte, error) {
	return v.allocOwned(mp, size, v.offHeap, false)
}

func (v *Vector) allocOwned(
	mp *mpool.MPool,
	size int,
	offHeap bool,
	_ bool,
) ([]byte, error) {
	if mp == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"vector allocation does not have a mpool",
		)
	}
	// An accounted MPool view charges the allocation; the vector itself
	// carries no account. A view charges only off-heap allocations, so an
	// allocation through a view is always off-heap.
	return mp.Alloc(size, offHeap || mp.IsView())
}

func (v *Vector) growData(mp *mpool.MPool, size int) ([]byte, error) {
	return v.growOwned(mp, v.data, size, true)
}

func (v *Vector) growArea(mp *mpool.MPool, size int) ([]byte, error) {
	return v.growOwned(mp, v.area, size, false)
}

func (v *Vector) growOwned(
	mp *mpool.MPool,
	old []byte,
	size int,
	data bool,
) ([]byte, error) {
	if v.HasBorrowedBacking() {
		if err := v.MaterializeOwned(mp); err != nil {
			return nil, err
		}
		if data {
			old = v.data
		} else {
			old = v.area
		}
	}
	if size <= cap(old) {
		return old[:size], nil
	}
	if mp == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"vector growth does not have a mpool",
		)
	}
	capacity, ok := mpool.GrowCapacity(int64(cap(old)), int64(size))
	if !ok {
		return nil, mpool.ErrAllocationAllocatorLimit
	}
	if cap(old) != 0 {
		return mp.Grow(old, size, v.offHeap || mp.IsView())
	}
	buf, err := v.allocOwned(mp, int(capacity), v.offHeap, data)
	if err != nil {
		return nil, err
	}
	return buf[:size], nil
}

func (v *Vector) growArea2(
	mp *mpool.MPool,
	src []byte,
	size int,
) ([]byte, error) {
	oldLen := len(v.area)
	if size < oldLen+len(src) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"mpool grow2 actually shrinks, %d+%d, %d",
			oldLen,
			len(src),
			size,
		)
	}
	grown, err := v.growArea(mp, size)
	if err != nil {
		return nil, err
	}
	copy(grown[oldLen:oldLen+len(src)], src)
	return grown, nil
}

func (v *Vector) readSizeBytes(
	r io.Reader,
	mp *mpool.MPool,
	data bool,
) (int32, []byte, error) {
	size, err := types.ReadInt32(r)
	if err != nil {
		return 0, nil, err
	}
	var old []byte
	if data {
		old = v.data
	} else {
		old = v.area
	}
	if size == 0 {
		if old != nil {
			old = old[:0]
		}
		if data {
			v.data = old
		} else {
			v.area = old
		}
		return 0, old, nil
	}
	if size < 0 {
		return size, nil, moerr.NewInvalidInputNoCtx(
			"negative vector buffer size",
		)
	}
	if err := validateStreamingReadSize(r, int64(size)); err != nil {
		return size, nil, err
	}
	var buf []byte
	if data {
		buf, err = v.growData(mp, int(size))
	} else {
		buf, err = v.growArea(mp, int(size))
	}
	if err != nil {
		return 0, nil, err
	}
	// Grow may already have freed the old allocation. Publish its replacement
	// before reading so a short reader still leaves one reachable cleanup owner.
	if data {
		v.data = buf
	} else {
		v.area = buf
	}
	if _, err = io.ReadFull(r, buf); err != nil {
		return size, buf, err
	}
	return size, buf, nil
}

func validateStreamingReadSize(r io.Reader, size int64) error {
	if size < 0 {
		return moerr.NewInvalidInputNoCtx("negative vector buffer size")
	}
	var remaining int64 = -1
	switch reader := r.(type) {
	case *io.LimitedReader:
		remaining = reader.N
	case interface{ Len() int }:
		remaining = int64(reader.Len())
	}
	if remaining >= 0 && size > remaining {
		return io.ErrUnexpectedEOF
	}
	return nil
}
