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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestPreparedNonFiniteFloatCast covers #31720: a NaN/+Inf/-Inf DOUBLE passed
// through EXECUTE ... USING now reaches execution as a typed DOUBLE value.
// Casting it to DOUBLE must keep the non-finite value (so percentile
// validation can reject it) instead of turning it into 0.
func TestPreparedNonFiniteFloatCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := newVectorByType(proc.Mp(), types.T_float64.ToType(),
		[]float64{math.NaN(), math.Inf(1), math.Inf(-1), 0.25}, nil)
	defer input.Free(proc.Mp())

	t.Run("double", func(t *testing.T) {
		target := newVectorByType(proc.Mp(), types.T_float64.ToType(), []float64{}, nil)
		defer target.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(4))
		require.NoError(t, NewCast([]*vector.Vector{input, target}, result, proc, 4, nil))
		got := vector.MustFixedColNoTypeCheck[float64](result.GetResultVector())
		require.True(t, math.IsNaN(got[0]), "NaN parameter: %v", got[0])
		require.True(t, math.IsInf(got[1], 1), "+Inf parameter: %v", got[1])
		require.True(t, math.IsInf(got[2], -1), "-Inf parameter: %v", got[2])
		require.Equal(t, 0.25, got[3])
	})
}
