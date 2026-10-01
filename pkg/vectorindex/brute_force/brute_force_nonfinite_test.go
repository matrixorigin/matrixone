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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// LOCK: GoBruteForceIndex deliberately returns a non-finite distance verbatim,
// with no error, instead of rejecting it. Finiteness is enforced at the consumer
// score boundary (moarray / the SQL scalar distance builtins / the native index
// Search), never inside this shared index -- see the recorded decision in
// GoBruteForceIndex.SearchFloat32 and metric/metric_nonfinite_contract_test.go.
// This pins that contract so a future in-index finite check (the wrong layer) is
// caught here (#29496).
func TestBruteForceReturnsNonFiniteDistance29496(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	isInf := func(v float64) bool { return math.IsInf(v, 0) }

	// Inner product distance is -dot. An overflowing dot (2e19*2e19 > float32 max)
	// yields -Inf, which is the SMALLEST distance and therefore WINS. Both entry
	// points return that -Inf score, with a valid key and no error.
	ip := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{{2e19}},
		Metric:    metric.Metric_InnerProduct,
		Dimension: 1,
		Count:     1,
	}
	rt1 := vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}

	keys := make([]int64, 1)
	dists := make([]float32, 1)
	require.NoError(t, ip.SearchFloat32(sqlproc, [][]float32{{2e19}}, rt1, keys, dists))
	require.EqualValues(t, 0, keys[0], "the overflowing row still wins")
	require.Truef(t, isInf(float64(dists[0])), "SearchFloat32 must return the non-finite winner, got %v", dists[0])

	_, distances, err := ip.Search(sqlproc, [][]float32{{2e19}}, rt1)
	require.NoError(t, err)
	require.Len(t, distances, 1)
	require.Truef(t, isInf(distances[0]), "Search must return the non-finite winner verbatim, got %v", distances[0])

	// limit>1 heap: a LOSING row whose L2sq distance overflows to +Inf (all-positive,
	// cannot win the min) is still returned in its slot as +Inf, not rejected.
	bfBig := types.BF16FromFloat32(2e19)
	bfZero := types.BF16FromFloat32(0)
	l2 := &GoBruteForceIndex[types.BF16, float32]{
		Dataset:   [][]types.BF16{{bfZero, bfZero}, {bfBig, bfBig}},
		Metric:    metric.Metric_L2sqDistance,
		Dimension: 2,
		Count:     2,
	}
	kbf := make([]int64, 2)
	dbf := make([]float32, 2)
	require.NoError(t, l2.SearchFloat32(sqlproc, [][]types.BF16{{bfZero, bfZero}},
		vectorindex.RuntimeConfig{Limit: 2, NThreads: 1}, kbf, dbf))
	require.Truef(t, isInf(float64(dbf[0])) || isInf(float64(dbf[1])),
		"the overflowing loser's +Inf distance must be returned, got %v", dbf)
}
