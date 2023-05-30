package plan

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRangeShuffle(t *testing.T) {
	require.Equal(t, GetRangeShuffleIndexUnsigned(0, 1000000, 299999, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexUnsigned(0, 1000000, 888, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexUnsigned(0, 1000000, 100000000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 299999, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 888, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 100000000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, -2, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 999000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 99999, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 100000, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 100001, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 199999, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 200000, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 999999, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 899999, 10), uint64(8))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 900000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 1000000, 10), uint64(9))
}
