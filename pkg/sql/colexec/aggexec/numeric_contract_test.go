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

// The MongoDB aggregate-v1 contract deliberately keeps AVG(DOUBLE) single-scope:
// partial sums are not associative. DECIMAL is the exact, merge-order-stable
// alternative required before local split can be enabled for that ingestion.
func TestAvgNumericContractMergeOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	floatInput := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(floatInput, []float64{1e16, -1e16, 1}, nil, mp))
	defer floatInput.Free(mp)

	runFloat := func(rightAssociated bool) float64 {
		states := make([]AggFuncExec, 3)
		for i := range states {
			states[i] = makeAvgExec(t, mp, types.T_float64.ToType())
			require.NoError(t, states[i].GroupGrow(1))
			require.NoError(t, states[i].Fill(0, i, []*vector.Vector{floatInput}))
		}
		if rightAssociated {
			require.NoError(t, states[1].Merge(states[2], 0, 0))
			require.NoError(t, states[0].Merge(states[1], 0, 0))
		} else {
			require.NoError(t, states[0].Merge(states[1], 0, 0))
			require.NoError(t, states[0].Merge(states[2], 0, 0))
		}
		result, err := states[0].Flush()
		require.NoError(t, err)
		value := vector.GetFixedAtNoTypeCheck[float64](result[0], 0)
		result[0].Free(mp)
		for _, state := range states {
			state.Free()
		}
		return value
	}

	leftFloat := runFloat(false)
	rightFloat := runFloat(true)
	require.Equal(t, 1.0/3.0, leftFloat)
	require.Equal(t, 0.0, rightFloat)

	decimalType := types.New(types.T_decimal64, 18, 2)
	decimalInput := vector.NewVec(decimalType)
	for _, text := range []string{"9000000000000000.00", "-9000000000000000.00", "1.00"} {
		value, err := types.ParseDecimal64(text, decimalType.Width, decimalType.Scale)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(decimalInput, value, false, mp))
	}
	defer decimalInput.Free(mp)

	runDecimal := func(rightAssociated bool) (types.Decimal128, int32) {
		states := make([]AggFuncExec, 3)
		for i := range states {
			states[i] = makeAvgExec(t, mp, decimalType)
			require.NoError(t, states[i].GroupGrow(1))
			require.NoError(t, states[i].Fill(0, i, []*vector.Vector{decimalInput}))
		}
		if rightAssociated {
			require.NoError(t, states[1].Merge(states[2], 0, 0))
			require.NoError(t, states[0].Merge(states[1], 0, 0))
		} else {
			require.NoError(t, states[0].Merge(states[1], 0, 0))
			require.NoError(t, states[0].Merge(states[2], 0, 0))
		}
		result, err := states[0].Flush()
		require.NoError(t, err)
		value := vector.GetFixedAtNoTypeCheck[types.Decimal128](result[0], 0)
		scale := result[0].GetType().Scale
		result[0].Free(mp)
		for _, state := range states {
			state.Free()
		}
		return value, scale
	}

	leftDecimal, leftScale := runDecimal(false)
	rightDecimal, rightScale := runDecimal(true)
	require.Equal(t, leftDecimal, rightDecimal)
	require.Equal(t, leftScale, rightScale)
	require.Equal(t, int32(6), leftScale)
	require.Equal(t, "0.333333", leftDecimal.Format(leftScale))
	require.Zero(t, mp.CurrNB())
}
