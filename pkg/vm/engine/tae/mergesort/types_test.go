// Copyright 2024 Matrix Origin
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
