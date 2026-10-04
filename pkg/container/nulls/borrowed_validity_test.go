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
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/stretchr/testify/require"
)

func TestBorrowedValidityReadCloneAndRelease(t *testing.T) {
	// Logical rows begin at bit two: valid, NULL, valid, NULL, valid.
	validity := []byte{0b01010101}
	var releases atomic.Int32
	lease, err := bufferlease.NewRefCounted(validity, int64(cap(validity)), func() {
		releases.Add(1)
	})
	require.NoError(t, err)

	var source Nulls
	require.NoError(t, source.InstallBorrowedValidity(validity, 2, 5, 2, lease))
	lease.Release()
	require.True(t, source.HasBorrowedValidity())
	require.True(t, source.Any())
	require.Equal(t, 2, source.Count())
	require.False(t, source.Contains(0))
	require.True(t, source.Contains(1))
	require.True(t, source.Contains(3))
	require.Equal(t, []uint64{1, 3}, source.ToArray())
	require.True(t, source.HasBorrowedValidity(), "read-only APIs must not materialize")

	clone := source.Clone()
	source.Reset()
	require.Equal(t, int32(0), releases.Load())
	require.True(t, clone.Contains(1))
	clone.Reset()
	require.Equal(t, int32(1), releases.Load())
}

func TestBorrowedValidityMutationCOW(t *testing.T) {
	validity := []byte{0b00000101} // rows: valid, NULL, valid
	var releases atomic.Int32
	lease, err := bufferlease.NewRefCounted(validity, 8, func() { releases.Add(1) })
	require.NoError(t, err)
	var nsp Nulls
	require.NoError(t, nsp.InstallBorrowedValidity(validity, 0, 3, 1, lease))
	lease.Release()

	nsp.Set(0)
	require.False(t, nsp.HasBorrowedValidity())
	require.Equal(t, int32(1), releases.Load())
	require.True(t, nsp.Contains(0))
	require.True(t, nsp.Contains(1))
	nsp.Unset(1)
	require.Equal(t, []uint64{0}, nsp.ToArray())
}

func TestBorrowedValidityRetainedWindow(t *testing.T) {
	validity := []byte{0b00101011} // row 2 and row 4 are NULL
	var releases atomic.Int32
	lease, err := bufferlease.NewRefCounted(validity, 1, func() { releases.Add(1) })
	require.NoError(t, err)
	var source, window Nulls
	require.NoError(t, source.InstallBorrowedValidity(validity, 0, 6, 2, lease))
	lease.Release()

	hasNull, err := source.InitBorrowedWindow(&window, 1, 5)
	require.NoError(t, err)
	require.True(t, hasNull)
	source.Reset()
	require.Equal(t, int32(0), releases.Load())
	require.Equal(t, []uint64{1, 3}, window.ToArray())
	window.Reset()
	require.Equal(t, int32(1), releases.Load())
}

func TestBorrowedValidityRejectsInvalidBounds(t *testing.T) {
	lease, err := bufferlease.NewRefCounted([]byte{0xff}, 1, nil)
	require.NoError(t, err)
	defer lease.Release()
	for _, tc := range []struct {
		offset int
		length int
		nulls  int
	}{
		{-1, 1, 1},
		{0, -1, 1},
		{0, 1, 0},
		{0, 1, 2},
		{8, 1, 1},
	} {
		var nsp Nulls
		require.Error(t, nsp.InstallBorrowedValidity([]byte{0xff}, tc.offset, tc.length, tc.nulls, lease))
	}
}

func TestBorrowedValidityMarshalIsAllocationFreeAndKeepsLease(t *testing.T) {
	validity := []byte{0b01010101}
	var releases atomic.Int32
	lease, err := bufferlease.NewRefCounted(validity, 1, func() { releases.Add(1) })
	require.NoError(t, err)
	var source Nulls
	require.NoError(t, source.InstallBorrowedValidity(validity, 2, 5, 2, lease))
	lease.Release()

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalTo(&encoded))
	require.Equal(t, source.MarshalSize(), encoded.Len())
	require.True(t, source.HasBorrowedValidity())
	require.Equal(t, int32(0), releases.Load())
	var decoded Nulls
	require.NoError(t, decoded.Read(encoded.Bytes()))
	require.Equal(t, []uint64{1, 3}, decoded.ToArray())
	source.Reset()
	require.Equal(t, int32(1), releases.Load())
}
