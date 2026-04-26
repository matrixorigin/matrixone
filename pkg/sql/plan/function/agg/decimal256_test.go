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
	testAggIDDecimal256Min = int64(11005)
	testAggIDDecimal256Max = int64(11006)
)

var registerDecimal256AggsOnce sync.Once

func mustParseDecimal256ForAgg(t *testing.T, value string, scale int32) types.Decimal256 {
	t.Helper()
	dec, err := types.ParseDecimal256(value, 65, scale)
	require.NoError(t, err)
	return dec
}

func registerDecimal256Aggs() {
	registerDecimal256AggsOnce.Do(func() {
		RegisterMin2(testAggIDDecimal256Min)
		RegisterMax2(testAggIDDecimal256Max)
	})
}

func TestDecimal256MinMaxAggregations(t *testing.T) {
	registerDecimal256Aggs()

	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 65, 4)

	testCases := []struct {
		name        string
		aggID       int64
		constValue  string
		fillValue   string
		mergeValue  string
		expectValue string
	}{
		{
			name:        "min",
			aggID:       testAggIDDecimal256Min,
			constValue:  "12.3412",
			fillValue:   "7.7777",
			mergeValue:  "-9.8765",
			expectValue: "-9.8765",
		},
		{
			name:        "max",
			aggID:       testAggIDDecimal256Max,
			constValue:  "-9.8765",
			fillValue:   "7.7777",
			mergeValue:  "12.3412",
			expectValue: "12.3412",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			constVec, err := vector.NewConstFixed(typ, mustParseDecimal256ForAgg(t, tc.constValue, 4), 3, mp)
			require.NoError(t, err)
			defer constVec.Free(mp)

			fillVec := vector.NewVec(typ)
			defer fillVec.Free(mp)
			require.NoError(t, vector.AppendFixed(fillVec, mustParseDecimal256ForAgg(t, tc.fillValue, 4), false, mp))

			mergeVec := vector.NewVec(typ)
			defer mergeVec.Free(mp)
			require.NoError(t, vector.AppendFixed(mergeVec, mustParseDecimal256ForAgg(t, tc.mergeValue, 4), false, mp))

			agg1, err := aggexec.MakeAgg(aggexec.NewSimpleAggMemoryManager(mp), tc.aggID, false, typ)
			require.NoError(t, err)
			defer agg1.Free()
			require.NoError(t, agg1.GroupGrow(1))
			require.NoError(t, agg1.BulkFill(0, []*vector.Vector{constVec}))
			require.NoError(t, agg1.Fill(0, 0, []*vector.Vector{fillVec}))

			agg2, err := aggexec.MakeAgg(aggexec.NewSimpleAggMemoryManager(mp), tc.aggID, false, typ)
			require.NoError(t, err)
			defer agg2.Free()
			require.NoError(t, agg2.GroupGrow(1))
			require.NoError(t, agg2.Fill(0, 0, []*vector.Vector{mergeVec}))

			require.NoError(t, agg1.Merge(agg2, 0, 0))

			results, err := agg1.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(
				t,
				mustParseDecimal256ForAgg(t, tc.expectValue, 4),
				vector.MustFixedColNoTypeCheck[types.Decimal256](results[0])[0],
			)

			for _, result := range results {
				result.Free(mp)
			}
		})
	}
}
