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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// test-local agg IDs for Decimal256 min/max; chosen to not collide with production IDs.
const (
	testAggIDDecimal256Min = int64(10001)
	testAggIDDecimal256Max = int64(10002)
)

func mustParseAggDecimal256(t *testing.T, value string, scale int32) types.Decimal256 {
	t.Helper()
	dec, err := types.ParseDecimal256(value, 65, scale)
	require.NoError(t, err)
	return dec
}

// decimal256MaxPositive is the largest positive Decimal256 value, used as the
// initial sentinel for min aggregation.
var decimal256MaxPositive = types.Decimal256{
	B0_63:    ^uint64(0),
	B64_127:  ^uint64(0),
	B128_191: ^uint64(0),
	B192_255: ^uint64(0) >> 1,
}

// decimal256MinNegative is the smallest (most negative) Decimal256 value, used
// as the initial sentinel for max aggregation.
var decimal256MinNegative = types.Decimal256{
	B0_63:    0,
	B64_127:  0,
	B128_191: 0,
	B192_255: uint64(1) << 63,
}

func registerDecimal256MinMax() {
	typ := types.T_decimal256.ToType()
	retFn := func(p []types.Type) types.Type { return p[0] }

	RegisterAggFromFixedRetFixed(
		MakeSingleColumnAggInformation(testAggIDDecimal256Min, typ, retFn, true),
		nil, nil,
		func(_ types.Type, _ ...types.Type) types.Decimal256 { return decimal256MaxPositive },
		func(_ AggGroupExecContext, _ AggCommonExecContext, v types.Decimal256, _ bool,
			g AggGetter[types.Decimal256], s AggSetter[types.Decimal256]) error {
			if v.Compare(g()) < 0 {
				s(v)
			}
			return nil
		},
		func(_ AggGroupExecContext, _ AggCommonExecContext, v types.Decimal256, _ int, _ bool,
			g AggGetter[types.Decimal256], s AggSetter[types.Decimal256]) error {
			if v.Compare(g()) < 0 {
				s(v)
			}
			return nil
		},
		func(_, _ AggGroupExecContext, _ AggCommonExecContext, _, _ bool,
			g1, g2 AggGetter[types.Decimal256], s AggSetter[types.Decimal256]) error {
			if r := g2(); r.Compare(g1()) < 0 {
				s(r)
			}
			return nil
		},
		nil,
	)

	RegisterAggFromFixedRetFixed(
		MakeSingleColumnAggInformation(testAggIDDecimal256Max, typ, retFn, true),
		nil, nil,
		func(_ types.Type, _ ...types.Type) types.Decimal256 { return decimal256MinNegative },
		func(_ AggGroupExecContext, _ AggCommonExecContext, v types.Decimal256, _ bool,
			g AggGetter[types.Decimal256], s AggSetter[types.Decimal256]) error {
			if v.Compare(g()) > 0 {
				s(v)
			}
			return nil
		},
		func(_ AggGroupExecContext, _ AggCommonExecContext, v types.Decimal256, _ int, _ bool,
			g AggGetter[types.Decimal256], s AggSetter[types.Decimal256]) error {
			if v.Compare(g()) > 0 {
				s(v)
			}
			return nil
		},
		func(_, _ AggGroupExecContext, _ AggCommonExecContext, _, _ bool,
			g1, g2 AggGetter[types.Decimal256], s AggSetter[types.Decimal256]) error {
			if r := g2(); r.Compare(g1()) > 0 {
				s(r)
			}
			return nil
		},
		nil,
	)
}

func TestDecimal256MinMax(t *testing.T) {
	registerDecimal256MinMax()

	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 65, 4)
	values := []types.Decimal256{
		mustParseAggDecimal256(t, "12.3412", 4),
		mustParseAggDecimal256(t, "-9.8765", 4),
		mustParseAggDecimal256(t, "7.7777", 4),
	}

	vec := vector.NewVec(typ)
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	}
	defer vec.Free(mp)

	testCases := []struct {
		name   string
		aggID  int64
		expect types.Decimal256
	}{
		{
			name:   "min",
			aggID:  testAggIDDecimal256Min,
			expect: mustParseAggDecimal256(t, "-9.8765", 4),
		},
		{
			name:   "max",
			aggID:  testAggIDDecimal256Max,
			expect: mustParseAggDecimal256(t, "12.3412", 4),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg, err := MakeAgg(NewSimpleAggMemoryManager(mp), tc.aggID, false, typ)
			require.NoError(t, err)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.expect, vector.MustFixedColNoTypeCheck[types.Decimal256](results[0])[0])

			agg.Free()
			for _, result := range results {
				result.Free(mp)
			}
		})
	}
}
