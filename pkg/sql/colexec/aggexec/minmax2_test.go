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

func mustParseAggDecimal256(t *testing.T, value string, scale int32) types.Decimal256 {
	t.Helper()
	dec, err := types.ParseDecimal256(value, 65, scale)
	require.NoError(t, err)
	return dec
}

func TestDecimal256MinMax(t *testing.T) {
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
			aggID:  AggIdOfMin,
			expect: mustParseAggDecimal256(t, "-9.8765", 4),
		},
		{
			name:   "max",
			aggID:  AggIdOfMax,
			expect: mustParseAggDecimal256(t, "12.3412", 4),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
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

func TestMinMaxPreservesWinningPrepareParamKind(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(input, []byte("9"), false, mp))
	input.SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	})
	defer func() {
		input.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	for _, tc := range []struct {
		name string
		id   int64
		want vector.PrepareParamKind
	}{
		{name: "min", id: AggIdOfMin, want: vector.PrepareParamInteger},
		{name: "max", id: AggIdOfMax, want: vector.PrepareParamNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg := makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, types.T_text.ToType())
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.want, results[0].GetPrepareParamKindAt(0))
			results[0].Free(mp)
			agg.Free()
		})
	}
}
