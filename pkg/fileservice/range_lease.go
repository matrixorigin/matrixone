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

package fileservice

import (
	"context"
	"errors"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// ErrObjectChanged is returned when a conditional object read can no longer
// address the identity fixed by planning. Callers must fail the whole
// statement; retrying without the same identity could combine object versions.
var ErrObjectChanged = errors.New("fileservice: object changed")

// ObjectIdentity is a format-neutral immutable object reference. VersionID is
// preferred when the backend exposes versioning; otherwise ETag is required
// for conditional reads. Size participates in every identity check.
type ObjectIdentity struct {
	VersionID    string
	ETag         string
	Size         int64
	LastModified time.Time
}

func (i ObjectIdentity) Validate() error {
	if i.Size < 0 || (i.VersionID == "" && i.ETag == "") {
		return moerr.NewInvalidInputNoCtx("object identity requires a non-negative size and version ID or ETag")
	}
	return nil
}

// ObjectIdentityFileService is the optional backend capability beneath a
// ConditionalLeasedRangeReader. OpenReadWithIdentity performs one conditional
// read; size -1 means through end of object.
type ObjectIdentityFileService interface {
	StatFileIdentity(ctx context.Context, path string) (ObjectIdentity, error)
	OpenReadWithIdentity(
		ctx context.Context,
		path string,
		offset, size int64,
		expected ObjectIdentity,
	) (io.ReadCloser, error)
}

// CapacityLease is the format-independent release side of a committed read
// reservation.
type CapacityLease interface {
	Release()
}

// CapacityReservation reserves an upper bound before a read can allocate or
// pin its authoritative backing.
type CapacityReservation interface {
	Commit(actualCapacity int64) (CapacityLease, error)
	Abort()
}

// RangeReadAdmission connects FileService range reads to an execution-owned
// capacity account without making FileService depend on SQL or file formats.
type RangeReadAdmission interface {
	Reserve(ctx context.Context, upperBound int64) (CapacityReservation, error)
}

type allocationAccountRangeAdmission struct {
	account       *mpool.AllocationAccount
	owner         mpool.AllocationOwner
	site          mpool.AllocationSite
	capacityClass mpool.AllocationCapacityClass
}

// NewAllocationAccountRangeAdmission binds non-MPool range capacity to the
// same statement account used by execution allocations.
func NewAllocationAccountRangeAdmission(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	site mpool.AllocationSite,
	capacityClass mpool.AllocationCapacityClass,
) (RangeReadAdmission, error) {
	if account == nil || site < mpool.AllocationSiteMin || site > mpool.AllocationSiteMax {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return &allocationAccountRangeAdmission{
		account: account, owner: owner, site: site, capacityClass: capacityClass,
	}, nil
}

func (a *allocationAccountRangeAdmission) Reserve(
	ctx context.Context,
	upperBound int64,
) (CapacityReservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a == nil || a.account == nil || upperBound <= 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	reservation, err := a.account.ReserveCapacityWithClass(
		uint64(upperBound), a.capacityClass, a.owner, a.site,
	)
	if err != nil {
		return nil, err
	}
	return allocationCapacityReservation{reservation: reservation}, nil
}

type allocationCapacityReservation struct {
	reservation *mpool.CapacityReservation
}

func (r allocationCapacityReservation) Commit(actualCapacity int64) (CapacityLease, error) {
	if r.reservation == nil || actualCapacity < 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return r.reservation.Commit(uint64(actualCapacity))
}

func (r allocationCapacityReservation) Abort() {
	if r.reservation != nil {
		r.reservation.Abort()
	}
}

// RangeLease keeps the authoritative read result and its committed capacity
// reservation alive under one idempotent release owner.
type RangeLease interface {
	Bytes() []byte
	Capacity() int64
	Release()
}

// LeasedRangeReader is the generic range capability used by columnar readers.
type LeasedRangeReader interface {
	ReadRangeLease(
		ctx context.Context,
		path string,
		offset, size int64,
		admission RangeReadAdmission,
	) (RangeLease, error)
}

// ConditionalLeasedRangeReader prevents one logical scan from combining
// ranges belonging to different versions of a mutable object.
type ConditionalLeasedRangeReader interface {
	LeasedRangeReader
	StatIdentity(ctx context.Context, path string) (ObjectIdentity, error)
	ReadRangeLeaseWithIdentity(
		ctx context.Context,
		path string,
		offset, size int64,
		expected ObjectIdentity,
		admission RangeReadAdmission,
	) (RangeLease, error)
}

type fileServiceRangeReader struct {
	fs FileService
}

// NewLeasedRangeReader adapts every FileService through its ordinary Read
// contract. Backend-specific implementations may replace this adapter while
// preserving the same ownership and admission semantics.
func NewLeasedRangeReader(fs FileService) LeasedRangeReader {
	base := &fileServiceRangeReader{fs: fs}
	if identityFS, ok := fs.(ObjectIdentityFileService); ok {
		return &conditionalFileServiceRangeReader{fileServiceRangeReader: base, identityFS: identityFS}
	}
	return base
}

type conditionalFileServiceRangeReader struct {
	*fileServiceRangeReader
	identityFS ObjectIdentityFileService
}

func (r *conditionalFileServiceRangeReader) StatIdentity(
	ctx context.Context,
	path string,
) (ObjectIdentity, error) {
	if r == nil || r.identityFS == nil {
		return ObjectIdentity{}, moerr.NewNotSupported(ctx, "object identity")
	}
	identity, err := r.identityFS.StatFileIdentity(ctx, path)
	if err != nil {
		return ObjectIdentity{}, err
	}
	if err := identity.Validate(); err != nil {
		return ObjectIdentity{}, err
	}
	return identity, nil
}

func (r *conditionalFileServiceRangeReader) ReadRangeLeaseWithIdentity(
	ctx context.Context,
	path string,
	offset, size int64,
	expected ObjectIdentity,
	admission RangeReadAdmission,
) (RangeLease, error) {
	if r == nil || r.identityFS == nil {
		return nil, moerr.NewNotSupported(ctx, "conditional object range read")
	}
	if err := expected.Validate(); err != nil {
		return nil, err
	}
	if offset < 0 || size <= 0 || offset > expected.Size || size > expected.Size-offset {
		return nil, moerr.NewInvalidInput(ctx, "conditional range is outside the fixed object identity")
	}
	return readRangeLease(ctx, path, offset, size, admission, func(destination []byte) error {
		reader, err := r.identityFS.OpenReadWithIdentity(ctx, path, offset, size, expected)
		if err != nil {
			return err
		}
		defer reader.Close()
		if _, err = io.ReadFull(reader, destination); err != nil {
			return err
		}
		var probe [1]byte
		if n, readErr := reader.Read(probe[:]); n != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) {
			return moerr.NewUnexpectedEOFNoCtx(path)
		}
		return nil
	})
}

type ioVectorRangeLease struct {
	data          []byte
	capacity      int64
	vector        *IOVector
	capacityLease CapacityLease
	released      atomic.Bool
}

func (l *ioVectorRangeLease) Bytes() []byte {
	if l == nil || l.released.Load() {
		return nil
	}
	return l.data
}

func (l *ioVectorRangeLease) Capacity() int64 {
	if l == nil {
		return 0
	}
	return l.capacity
}

func (l *ioVectorRangeLease) Release() {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	// Release backing before its capacity charge. Observing zero usage then
	// implies that no range backing remains reachable through this owner.
	if l.vector != nil {
		l.vector.Release()
		l.vector = nil
	}
	l.data = nil
	if l.capacityLease != nil {
		l.capacityLease.Release()
		l.capacityLease = nil
	}
}

func (r *fileServiceRangeReader) ReadRangeLease(
	ctx context.Context,
	path string,
	offset, size int64,
	admission RangeReadAdmission,
) (_ RangeLease, retErr error) {
	if r == nil || r.fs == nil || admission == nil || path == "" ||
		offset < 0 || size <= 0 || offset > math.MaxInt64-size ||
		uint64(size) > uint64(^uint(0)>>1) {
		return nil, moerr.NewInvalidInput(ctx, "invalid leased range read")
	}

	return readRangeLease(ctx, path, offset, size, admission, func(destination []byte) error {
		vector := &IOVector{
			FilePath: path,
			Policy:   SkipAllCache,
			Entries: []IOEntry{{
				Offset: offset,
				Size:   size,
				Data:   destination,
			}},
		}
		if err := r.fs.Read(ctx, vector); err != nil {
			vector.ReleaseReadResultOnError()
			return err
		}
		entry := &vector.Entries[0]
		if entry.CachedData != nil || entry.releaseData != nil || len(entry.Data) != len(destination) ||
			(cap(entry.Data) > 0 && &entry.Data[0] != &destination[0]) {
			vector.Release()
			return moerr.NewInvalidInput(ctx, "leased range backend replaced exact destination")
		}
		// Drop the IOVector owner without dropping destination. The generic
		// adapter supplied the backing and no backend release hook owns it.
		entry.Data = nil
		vector.Release()
		return nil
	})
}

func readRangeLease(
	ctx context.Context,
	path string,
	offset, size int64,
	admission RangeReadAdmission,
	read func(destination []byte) error,
) (RangeLease, error) {
	if admission == nil || read == nil || path == "" || offset < 0 || size <= 0 ||
		offset > math.MaxInt64-size || uint64(size) > uint64(^uint(0)>>1) {
		return nil, moerr.NewInvalidInput(ctx, "invalid leased range read")
	}
	reservation, err := admission.Reserve(ctx, size)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		// This also runs while unwinding an allocation/backend panic. Until a
		// capacity lease is published, the reservation remains our sole owner.
		if !committed {
			reservation.Abort()
		}
	}()
	destination := make([]byte, int(size))
	if err = read(destination); err != nil {
		return nil, err
	}
	capacity := int64(cap(destination))
	capacityLease, err := reservation.Commit(capacity)
	if err != nil {
		return nil, err
	}
	committed = true
	return &ioVectorRangeLease{
		data: destination, capacity: capacity, capacityLease: capacityLease,
	}, nil
}
