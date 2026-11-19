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

package function

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestPreparedNonFiniteFloatCast covers #31720: a NaN/+Inf/-Inf DOUBLE passed
// through EXECUTE ... USING travels as "NaN"/"+Inf"/"-Inf" text. Casting it
// back to FLOAT/DOUBLE must restore the non-finite value (so percentile
// validation can reject it) instead of reading the text as 0. Ordinary SQL
// strings with the same characters keep the string conversion.
func TestPreparedNonFiniteFloatCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := newVectorByType(proc.Mp(), types.T_text.ToType(),
		[]string{"NaN", "+Inf", "-Inf", "0.25", "NaN", "+Inf"}, nil)
	defer input.Free(proc.Mp())
	input.SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamFloat, vector.PrepareParamFloat, vector.PrepareParamFloat,
		vector.PrepareParamFloat, vector.PrepareParamNone, vector.PrepareParamInteger,
	})
	for _, mode := range []SQLCompatibilityMode{SQLCompatibilityMySQL, SQLCompatibilityMatrixOne} {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp()).(*vector.FunctionResult[float64])
		require.NoError(t, result.PreExtendAndReset(6))
		err := strToFloat(context.Background(), mode, vector.GenerateFunctionStrParameter(input), result, 64, 4, nil)
		require.NoError(t, err)
		got := vector.MustFixedColNoTypeCheck[float64](result.GetResultVector())
		require.True(t, math.IsNaN(got[0]), "NaN parameter: %v", got[0])
		require.True(t, math.IsInf(got[1], 1), "+Inf parameter: %v", got[1])
		require.True(t, math.IsInf(got[2], -1), "-Inf parameter: %v", got[2])
		require.Equal(t, 0.25, got[3])
		result.Free()
	}

	nanFloat, ok := preparedNonFiniteFloat(vector.PrepareParamFloat, []byte("NaN"))
	require.True(t, ok)
	require.True(t, math.IsNaN(nanFloat))
	for _, c := range []struct {
		kind vector.PrepareParamKind
		text string
	}{
		{vector.PrepareParamNone, "NaN"},
		{vector.PrepareParamInteger, "+Inf"},
		{vector.PrepareParamDecimal, "-Inf"},
		{vector.PrepareParamFloat, "nan"},
		{vector.PrepareParamFloat, "Infinity"},
		{vector.PrepareParamFloat, "1.5"},
	} {
		_, ok := preparedNonFiniteFloat(c.kind, []byte(c.text))
		require.False(t, ok, "%v %q", c.kind, c.text)
	}
}
