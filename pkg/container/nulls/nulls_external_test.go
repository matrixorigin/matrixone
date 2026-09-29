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

package nulls

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrTruncatesLongSourceForExternalResult(t *testing.T) {
	dst := NewWithSize(4)
	dst.Add(2)
	storage := make([]uint64, 1)
	dst.GetBitmap().InstallExternalStorage(storage)

	src := NewWithSize(128)
	src.Add(2, 100)

	Or(dst, src, dst)
	require.True(t, dst.Contains(2))
	require.False(t, dst.Contains(100))
	require.EqualValues(t, 4, dst.GetBitmap().Len())
}

func TestOrTruncatesLongSourceForExternalResultWithTwoInputs(t *testing.T) {
	dst := NewWithSize(8)
	storage := make([]uint64, 1)
	dst.GetBitmap().InstallExternalStorage(storage)

	left := NewWithSize(128)
	left.Add(1, 64)
	right := NewWithSize(128)
	right.Add(7, 8, 127)

	Or(left, right, dst)
	require.True(t, dst.Contains(1))
	require.True(t, dst.Contains(7))
	require.False(t, dst.Contains(64))
	require.False(t, dst.Contains(8))
	require.False(t, dst.Contains(127))
	require.EqualValues(t, 8, dst.GetBitmap().Len())
}

func TestOrExternalResultBoundaryCases(t *testing.T) {
	t.Run("empty sources clear within the destination bound", func(t *testing.T) {
		dst := NewWithSize(4)
		dst.Add(1)
		dst.GetBitmap().InstallExternalStorage(make([]uint64, 1))

		left := NewWithSize(8)
		right := NewWithSize(8)
		Or(left, right, dst)

		require.False(t, dst.Contains(1))
		require.EqualValues(t, 4, dst.GetBitmap().Len())

		src := NewWithSize(8)
		src.Add(2, 7)
		Or(dst, src, dst)
		require.True(t, dst.Contains(2))
		require.False(t, dst.Contains(7))
		require.EqualValues(t, 4, dst.GetBitmap().Len())
	})

	t.Run("zero length destination ignores source rows", func(t *testing.T) {
		dst := NewWithSize(0)
		dst.GetBitmap().InstallExternalStorage(make([]uint64, 1))
		src := NewWithSize(1)
		src.Add(0)

		Or(src, NewWithSize(0), dst)

		require.EqualValues(t, 0, dst.GetBitmap().Len())
	})

	t.Run("shorter source and aliased destination keep length", func(t *testing.T) {
		dst := NewWithSize(8)
		dst.Add(6)
		dst.GetBitmap().InstallExternalStorage(make([]uint64, 1))
		src := NewWithSize(4)
		src.Add(2)

		Or(src, dst, dst)

		require.True(t, dst.Contains(2))
		require.True(t, dst.Contains(6))
		require.EqualValues(t, 8, dst.GetBitmap().Len())
	})

	t.Run("reset destination requires its owner to restore the bound", func(t *testing.T) {
		dst := NewWithSize(8)
		dst.Add(6)
		dst.GetBitmap().InstallExternalStorage(make([]uint64, 1))
		dst.Reset()

		src := NewWithSize(3)
		src.Add(1)
		Or(dst, src, dst)

		require.False(t, dst.Contains(1))
		require.EqualValues(t, 0, dst.GetBitmap().Len())
	})

	t.Run("visible length bounds a longer source within spare capacity", func(t *testing.T) {
		dst := NewWithSize(4)
		dst.GetBitmap().InstallExternalStorage(make([]uint64, 1))

		src := NewWithSize(8)
		src.Add(2, 7)
		Or(dst, src, dst)

		require.True(t, dst.Contains(2))
		require.False(t, dst.Contains(7))
		require.EqualValues(t, 4, dst.GetBitmap().Len())
	})

	for _, tc := range []struct {
		name  string
		union func(dst, src *Nulls)
	}{
		{name: "method Or", union: func(dst, src *Nulls) { dst.Or(src) }},
		{name: "Set", union: func(dst, src *Nulls) { Set(dst, src) }},
		{name: "OrBitmap", union: func(dst, src *Nulls) { dst.OrBitmap(src.GetBitmap()) }},
		{name: "Merge", union: func(dst, src *Nulls) { dst.Merge(src) }},
	} {
		t.Run(tc.name+" respects the external destination bound", func(t *testing.T) {
			dst := NewWithSize(4)
			dst.GetBitmap().InstallExternalStorage(make([]uint64, 1))
			src := NewWithSize(128)
			src.Add(2, 100)

			require.NotPanics(t, func() {
				tc.union(dst, src)
			})
			require.True(t, dst.Contains(2))
			require.False(t, dst.Contains(100))
			require.EqualValues(t, 4, dst.GetBitmap().Len())
		})
	}
}

func BenchmarkOrLongSourceIntoExternalResult(b *testing.B) {
	const visibleRows = 8 << 10
	const sourceRows = 64 << 10

	dst := NewWithSize(visibleRows)
	dst.GetBitmap().InstallExternalStorage(make([]uint64, visibleRows/64))
	src := NewWithSize(sourceRows)
	src.AddRange(0, sourceRows)

	b.ReportAllocs()
	b.SetBytes(visibleRows / 8)
	b.ResetTimer()
	for range b.N {
		dst.Clear()
		Or(dst, src, dst)
	}
}
