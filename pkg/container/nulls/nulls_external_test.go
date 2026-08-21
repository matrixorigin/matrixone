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
	t.Run("empty sources reset the destination", func(t *testing.T) {
		dst := NewWithSize(4)
		dst.Add(1)
		dst.GetBitmap().InstallExternalStorage(make([]uint64, 1))

		left := NewWithSize(128)
		right := NewWithSize(128)
		Or(left, right, dst)

		require.False(t, dst.Contains(1))
		require.EqualValues(t, 0, dst.GetBitmap().Len())
	})

	t.Run("zero length destination ignores source rows", func(t *testing.T) {
		dst := NewWithSize(0)
		dst.GetBitmap().InstallExternalStorage(make([]uint64, 0))
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
}
