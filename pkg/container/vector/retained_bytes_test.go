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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestRetainedBytesKeepsOffsetPrefix(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int64.ToType())
	defer v.Free(mp)
	require.NoError(t, AppendFixedList(v, make([]int64, 1024), nil, mp))
	original := v.RetainedBytes()
	w, err := v.Window(1000, 1024)
	require.NoError(t, err)
	defer w.Free(mp)
	for i := 0; i < 10; i++ {
		next, err := w.Window(1, w.Length())
		require.NoError(t, err)
		w.Free(mp)
		w = next
	}
	defer w.Free(mp)
	require.GreaterOrEqual(t, w.RetainedBytes(), original)
	require.Equal(t, original, w.RetainedBytes(), "nested views must not accumulate inherited capacity repeatedly")
	w.Free(mp)
	require.Zero(t, w.RetainedBytes())
}

func TestRetainedBytesLeasedSourceLifetime(t *testing.T) {
	data := make([]byte, 8192)
	lease, err := NewRefCountedBufferLease(data, 16384, func() {})
	require.NoError(t, err)
	defer lease.Release()
	source, err := NewBorrowedFixedVector(types.T_int64.ToType(), 1, data[8184:], lease)
	require.NoError(t, err)
	view, err := source.Window(0, 1)
	require.NoError(t, err)
	defer view.Free(nil)
	source.Free(nil)
	require.GreaterOrEqual(t, view.RetainedBytes(), uint64(16384))
	require.Equal(t, int64(0), MustFixedColWithTypeCheck[int64](view)[0])
}

func TestRetainedBytesOwnedValidityCapacity(t *testing.T) {
	v := NewVec(types.T_int64.ToType())
	defer v.Free(nil)
	v.GetNulls().InitWithSize(65536)
	v.GetNulls().Add(0)
	v.SetLength(1)
	require.GreaterOrEqual(t, v.RetainedBytes(), uint64(8192))
	v.GetNulls().Reset()
	require.Zero(t, v.RetainedBytes(), "reset releases ordinary bitmap storage")
}
