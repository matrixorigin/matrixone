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
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type conditionalMemoryFS struct {
	*MemoryFS
	identity ObjectIdentity
}

func (f *conditionalMemoryFS) StatFileIdentity(ctx context.Context, path string) (ObjectIdentity, error) {
	if err := ctx.Err(); err != nil {
		return ObjectIdentity{}, err
	}
	return f.identity, nil
}

func (f *conditionalMemoryFS) OpenReadWithIdentity(
	ctx context.Context,
	path string,
	offset, size int64,
	expected ObjectIdentity,
) (io.ReadCloser, error) {
	if expected != f.identity {
		return nil, ErrObjectChanged
	}
	vector := &IOVector{FilePath: path, Policy: SkipAllCache, Entries: []IOEntry{{Offset: offset, Size: size}}}
	if err := f.Read(ctx, vector); err != nil {
		return nil, err
	}
	data := append([]byte(nil), vector.Entries[0].Data...)
	vector.Release()
	return io.NopCloser(bytes.NewReader(data)), nil
}

type testRangeAdmission struct {
	reject       error
	reserved     atomic.Int64
	committed    atomic.Int64
	released     atomic.Int64
	aborted      atomic.Int64
	commitReject error
}

func (a *testRangeAdmission) Reserve(_ context.Context, upper int64) (CapacityReservation, error) {
	if a.reject != nil {
		return nil, a.reject
	}
	a.reserved.Add(upper)
	return &testRangeReservation{admission: a, upper: upper}, nil
}

type testRangeReservation struct {
	admission *testRangeAdmission
	upper     int64
	done      atomic.Bool
}

func (r *testRangeReservation) Commit(actual int64) (CapacityLease, error) {
	if r.admission.commitReject != nil {
		return nil, r.admission.commitReject
	}
	if actual > r.upper || !r.done.CompareAndSwap(false, true) {
		return nil, errors.New("invalid commit")
	}
	r.admission.committed.Add(actual)
	return &testRangeCapacityLease{admission: r.admission, capacity: actual}, nil
}

func (r *testRangeReservation) Abort() {
	if r.done.CompareAndSwap(false, true) {
		r.admission.aborted.Add(r.upper)
	}
}

type testRangeCapacityLease struct {
	admission *testRangeAdmission
	capacity  int64
	released  atomic.Bool
}

func (l *testRangeCapacityLease) Release() {
	if l.released.CompareAndSwap(false, true) {
		l.admission.released.Add(l.capacity)
	}
}

func TestReadRangeLeaseLifecycle(t *testing.T) {
	ctx := context.Background()
	fs, err := NewMemoryFS("range", DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: "range:file",
		Entries:  []IOEntry{{Offset: 0, Size: 6, Data: []byte("abcdef")}},
	}))
	admission := new(testRangeAdmission)
	lease, err := NewLeasedRangeReader(fs).ReadRangeLease(ctx, "range:file", 2, 3, admission)
	require.NoError(t, err)
	require.Equal(t, []byte("cde"), lease.Bytes())
	require.Equal(t, int64(3), lease.Capacity())
	require.Equal(t, int64(3), admission.reserved.Load())
	require.Equal(t, int64(3), admission.committed.Load())

	lease.Release()
	lease.Release()
	require.Nil(t, lease.Bytes())
	require.Equal(t, int64(3), admission.released.Load())
}

func TestReadRangeLeaseAdmissionAndCommitFailure(t *testing.T) {
	ctx := context.Background()
	fs, err := NewMemoryFS("range-errors", DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: "range-errors:file",
		Entries:  []IOEntry{{Offset: 0, Size: 3, Data: []byte("abc")}},
	}))

	reject := errors.New("capacity rejected")
	_, err = NewLeasedRangeReader(fs).ReadRangeLease(ctx, "range-errors:file", 0, 3, &testRangeAdmission{reject: reject})
	require.ErrorIs(t, err, reject)

	admission := &testRangeAdmission{commitReject: reject}
	_, err = NewLeasedRangeReader(fs).ReadRangeLease(ctx, "range-errors:file", 0, 3, admission)
	require.ErrorIs(t, err, reject)
	require.Equal(t, int64(3), admission.aborted.Load())
}

func TestReadRangeLeaseBackendPanicAbortsReservation(t *testing.T) {
	admission := new(testRangeAdmission)
	require.PanicsWithValue(t, "injected range read panic", func() {
		_, _ = readRangeLease(
			context.Background(), "panic:file", 0, 8, admission,
			func([]byte) error { panic("injected range read panic") },
		)
	})
	require.Equal(t, int64(8), admission.reserved.Load())
	require.Equal(t, int64(8), admission.aborted.Load())
	require.Zero(t, admission.committed.Load())
}

func TestReadRangeLeaseRejectsInvalidAndCanceledReads(t *testing.T) {
	fs, err := NewMemoryFS("range-invalid", DisabledCacheConfig, nil)
	require.NoError(t, err)
	reader := NewLeasedRangeReader(fs)
	admission := new(testRangeAdmission)
	_, err = reader.ReadRangeLease(context.Background(), "range-invalid:file", 0, 0, admission)
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = reader.ReadRangeLease(ctx, "range-invalid:file", 0, 1, admission)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), admission.aborted.Load())
}

func TestConditionalRangeLeaseFixesObjectIdentity(t *testing.T) {
	ctx := context.Background()
	memoryFS, err := NewMemoryFS("conditional", DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, memoryFS.Write(ctx, IOVector{
		FilePath: "conditional:file",
		Entries:  []IOEntry{{Offset: 0, Size: 6, Data: []byte("abcdef")}},
	}))
	identity := ObjectIdentity{
		ETag: "etag-v1", Size: 6, LastModified: time.Unix(10, 0).UTC(),
	}
	fs := &conditionalMemoryFS{MemoryFS: memoryFS, identity: identity}
	reader, ok := NewLeasedRangeReader(fs).(ConditionalLeasedRangeReader)
	require.True(t, ok)
	gotIdentity, err := reader.StatIdentity(ctx, "conditional:file")
	require.NoError(t, err)
	require.Equal(t, identity, gotIdentity)

	admission := new(testRangeAdmission)
	lease, err := reader.ReadRangeLeaseWithIdentity(ctx, "conditional:file", 1, 3, identity, admission)
	require.NoError(t, err)
	require.Equal(t, []byte("bcd"), lease.Bytes())
	require.Equal(t, int64(3), admission.committed.Load())
	lease.Release()
	require.Equal(t, int64(3), admission.released.Load())

	fs.identity.ETag = "etag-v2"
	_, err = reader.ReadRangeLeaseWithIdentity(ctx, "conditional:file", 1, 3, identity, admission)
	require.ErrorIs(t, err, ErrObjectChanged)
	require.Equal(t, int64(3), admission.aborted.Load())
}

func TestConditionalRangeRejectsIncompleteIdentityAndOutOfBounds(t *testing.T) {
	ctx := context.Background()
	memoryFS, err := NewMemoryFS("conditional-invalid", DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &conditionalMemoryFS{MemoryFS: memoryFS, identity: ObjectIdentity{ETag: "etag", Size: 2}}
	reader := NewLeasedRangeReader(fs).(ConditionalLeasedRangeReader)
	admission := new(testRangeAdmission)

	_, err = reader.ReadRangeLeaseWithIdentity(ctx, "conditional-invalid:file", 0, 1, ObjectIdentity{Size: 2}, admission)
	require.ErrorContains(t, err, "version ID or ETag")
	_, err = reader.ReadRangeLeaseWithIdentity(ctx, "conditional-invalid:file", 1, 2, fs.identity, admission)
	require.ErrorContains(t, err, "outside")
}
