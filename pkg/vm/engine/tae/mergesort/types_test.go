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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestMergeStats(t *testing.T) {
	cases := []struct {
		stats    mergeStats
		expected bool
		note     string
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
			stats:    mergeStats{targetObjSize: 5, totalSize: 15, mergedSize: 3, writtenBytes: 3},
			expected: false,
			note:     "object not full",
		},
		{
			stats:    mergeStats{targetObjSize: 5, totalSize: 18, mergedSize: 15, writtenBytes: 6},
			expected: false,
			note:     "left data is less than targetObjectSize",
		},
		{
			stats:    mergeStats{targetObjSize: 5, totalSize: 15, mergedSize: 6, writtenBytes: 6},
			expected: true,
			note:     "left data is greater than targetObjectSize",
		},
	}

	for i, c := range cases {
		require.Equal(t, c.expected, c.stats.needNewObject(), "case %d", i)
	}
}
