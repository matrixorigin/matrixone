package mergesort

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMergeStats(t *testing.T) {
	cases := []struct {
		stats    mergeStats
		expected bool
	}{
		{
			stats:    mergeStats{},
			expected: false,
		},
		{
			stats:    mergeStats{objBlkCnt: int(options.DefaultBlocksPerObject)},
			expected: true,
		},
		{
			stats:    mergeStats{objBlkCnt: int(options.DefaultBlocksPerObject - 1)},
			expected: false,
		},
		{
			stats:    mergeStats{targetObjSize: 5, rowSize: 1, objRowCnt: 4, mergedRowCnt: 4, totalRowCnt: 10},
			expected: false,
		},
		{
			stats:    mergeStats{targetObjSize: 5, rowSize: 1, objRowCnt: 5, mergedRowCnt: 5, totalRowCnt: 10},
			expected: false,
		},
		{
			stats:    mergeStats{targetObjSize: 10, rowSize: 1, objRowCnt: 6, mergedRowCnt: 6, totalRowCnt: 10},
			expected: false,
		},
		{
			stats:    mergeStats{targetObjSize: 5, rowSize: 1, objRowCnt: 6, mergedRowCnt: 6, totalRowCnt: 12},
			expected: true,
		},
	}

	for _, c := range cases {
		require.Equal(t, c.expected, c.stats.needNewObject())
	}
}
