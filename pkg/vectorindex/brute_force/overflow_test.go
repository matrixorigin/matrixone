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

package brute_force

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// A candidate distance that leaves the element domain must not leave the winner unassigned.
// minIdx starts at -1 and only moves on `dist < minDist`, which +Inf never satisfies, so every
// candidate overflowing used to return key -1 with no error -- and ProductL2 feeds that key
// straight into vector.UnionOne, which has no row -1.
func TestSearchRejectsOutOfDomainDistances(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	idx := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{{0}},
		Metric:    metric.Metric_L2Distance,
		Dimension: 1,
		Count:     1,
	}

	for _, limit := range []uint{1, 2} {
		keys := make([]int64, limit)
		dists := make([]float32, limit)
		err := idx.SearchFloat32(sqlproc, [][]float32{{2e19}},
			vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}, keys, dists)
		require.Error(t, err, "limit=%d", limit)
		for _, k := range keys {
			require.GreaterOrEqual(t, k, int64(0), "limit=%d: -1 is not a row index", limit)
		}
	}

	// Ordinary magnitudes still resolve to the nearest row.
	idx2 := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{{0}, {10}},
		Metric:    metric.Metric_L2Distance,
		Dimension: 1,
		Count:     2,
	}
	keys := make([]int64, 1)
	dists := make([]float32, 1)
	require.NoError(t, idx2.SearchFloat32(sqlproc, [][]float32{{9}},
		vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}, keys, dists))
	require.EqualValues(t, 1, keys[0])
	require.EqualValues(t, 1, dists[0])
}
