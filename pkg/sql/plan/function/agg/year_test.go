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

package agg

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

const (
	testAggIDYearMin = int64(11001)
	testAggIDYearMax = int64(11002)
	testAggIDYearSum = int64(11003)
	testAggIDYearAvg = int64(11004)
)

var registerYearAggsOnce sync.Once

func registerYearAggs() {
	registerYearAggsOnce.Do(func() {
		RegisterMin2(testAggIDYearMin)
		RegisterMax2(testAggIDYearMax)
		RegisterSum2(testAggIDYearSum)
		RegisterAvg2(testAggIDYearAvg)
	})
}

func TestYearAggregations(t *testing.T) {
	registerYearAggs()

	mp := mpool.MustNewZero()
	typ := types.T_year.ToType()
	values := []types.MoYear{2024, 1999, 2010}

	vec := vector.NewVec(typ)
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	}
	defer vec.Free(mp)

	testCases := []struct {
		name    string
		aggID   int64
		checker func(t *testing.T, result *vector.Vector)
	}{
		{
			name:  "min",
			aggID: testAggIDYearMin,
			checker: func(t *testing.T, result *vector.Vector) {
				require.Equal(t, types.MoYear(1999), vector.MustFixedColNoTypeCheck[types.MoYear](result)[0])
			},
		},
		{
			name:  "max",
			aggID: testAggIDYearMax,
			checker: func(t *testing.T, result *vector.Vector) {
				require.Equal(t, types.MoYear(2024), vector.MustFixedColNoTypeCheck[types.MoYear](result)[0])
			},
		},
		{
			name:  "sum",
			aggID: testAggIDYearSum,
			checker: func(t *testing.T, result *vector.Vector) {
				require.Equal(t, int64(6033), vector.MustFixedColNoTypeCheck[int64](result)[0])
			},
		},
		{
			name:  "avg",
			aggID: testAggIDYearAvg,
			checker: func(t *testing.T, result *vector.Vector) {
				require.Equal(t, float64(2011), vector.MustFixedColNoTypeCheck[float64](result)[0])
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg, err := aggexec.MakeAgg(aggexec.NewSimpleAggMemoryManager(mp), tc.aggID, false, typ)
			require.NoError(t, err)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			tc.checker(t, results[0])

			agg.Free()
			for _, result := range results {
				result.Free(mp)
			}
		})
	}
}
