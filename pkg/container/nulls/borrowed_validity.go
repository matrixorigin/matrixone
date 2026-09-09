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

package nulls

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
)

// InstallBorrowedValidity installs an immutable Arrow validity bitmap. Arrow
// bit 1 means valid, so Contains exposes its inverse as an MO NULL marker.
// The lease is retained on success; the caller keeps its incoming reference.
func (nsp *Nulls) InstallBorrowedValidity(
	validity []byte,
	bitOffset int,
	length int,
	nullCount int,
	lease bufferlease.BufferLease,
) error {
	if nsp == nil || lease == nil || bitOffset < 0 || length < 0 ||
		nullCount <= 0 || nullCount > length || nsp.validityLease != nil ||
		nsp.np.Len() != 0 || bitOffset > int(^uint(0)>>1)-length ||
		(bitOffset+length+7)/8 > len(validity) {
		return moerr.NewInvalidInputNoCtx("invalid borrowed Arrow validity view")
	}
	if !lease.Retain() {
		return moerr.NewInternalErrorNoCtx("buffer lease is already released")
	}
	nsp.validity = validity
	nsp.validityOffset = bitOffset
	nsp.validityLength = length
	nsp.validityNulls = nullCount
	nsp.validityLease = lease
	return nil
}

// HasBorrowedValidity reports whether reads still use the Arrow bitmap.
func (nsp *Nulls) HasBorrowedValidity() bool {
	return nsp != nil && nsp.validityLease != nil
}

// BorrowedAccountedBytes returns the charge attached to the shared validity
// owner. Lease-group accounting may de-duplicate it across retained views.
func (nsp *Nulls) BorrowedAccountedBytes() int64 {
	if nsp == nil || nsp.validityLease == nil {
		return 0
	}
	return nsp.validityLease.AccountedBytes()
}

// InitBorrowedWindow creates a retained logical sub-view. It returns false
// when the requested range contains no NULL, in which case dst remains empty.
func (nsp *Nulls) InitBorrowedWindow(dst *Nulls, start, end int) (bool, error) {
	if nsp == nil || dst == nil || nsp.validityLease == nil ||
		start < 0 || end < start || end > nsp.validityLength {
		return false, moerr.NewInvalidInputNoCtx("invalid borrowed validity window")
	}
	nulls := 0
	for row := start; row < end; row++ {
		if nsp.validityContainsNull(uint64(row)) {
			nulls++
		}
	}
	dst.Reset()
	if nulls == 0 {
		return false, nil
	}
	err := dst.InstallBorrowedValidity(
		nsp.validity,
		nsp.validityOffset+start,
		end-start,
		nulls,
		nsp.validityLease,
	)
	return err == nil, err
}

func (nsp *Nulls) validityContainsNull(row uint64) bool {
	if nsp.validityLease == nil || row >= uint64(nsp.validityLength) {
		return false
	}
	bit := nsp.validityOffset + int(row)
	return nsp.validity[bit>>3]&(byte(1)<<uint(bit&7)) == 0
}

func (nsp *Nulls) releaseValidity() {
	if nsp == nil || nsp.validityLease == nil {
		return
	}
	lease := nsp.validityLease
	nsp.validity = nil
	nsp.validityOffset = 0
	nsp.validityLength = 0
	nsp.validityNulls = 0
	nsp.validityLease = nil
	lease.Release()
}

// materializeValidity is the COW boundary for legacy bitmap APIs and all
// mutation. The lease is released only after the owned bitmap is complete.
func (nsp *Nulls) materializeValidity() {
	if nsp == nil || nsp.validityLease == nil {
		return
	}
	length := nsp.validityLength
	// Vector-backed borrowed validity installs admitted external bitmap
	// storage before publication. Standalone Nulls users preserve the legacy
	// Go-owned fallback, but do not allocate a second row-index scratch slice.
	nsp.np.InitWithSize(int64(length))
	for row := 0; row < nsp.validityLength; row++ {
		if nsp.validityContainsNull(uint64(row)) {
			nsp.np.Add(uint64(row))
		}
	}
	nsp.releaseValidity()
}
