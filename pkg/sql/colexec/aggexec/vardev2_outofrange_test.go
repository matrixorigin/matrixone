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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Issue 31355: a VARIANCE family result whose exact value exceeds the finite
// DOUBLE range must fail with MySQL ER_DATA_OUT_OF_RANGE instead of returning
// the IEEE "+Infinity" token (which downstream breaks JSON serialization with
// the unrelated "invalid JSON vector payload" message). The finite STDDEV
// capability on the same input is preserved: STDDEV(1e200,-1e200)=1e200.
func TestVarianceOutOfRangeFinalize(t *testing.T) {
	param := types.T_float64.ToType()
	mp := newAggExecTestPool(t)
	pair := varianceInput(t, mp, param, []float64{1e200, -1e200}, nil)

	overflowCases := []struct {
		name     string
		id       int64
		distinct bool
	}{
		{"var-pop", AggIdOfVarPop, false},
		{"var-pop/distinct", AggIdOfVarPop, true},
		{"var-sample", AggIdOfVarSample, false},
	}
	for _, tc := range overflowCases {
		t.Run(tc.name, func(t *testing.T) {
			exec := varianceExec(t, mp, tc.id, tc.distinct, param)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{pair}))
			results, err := exec.Flush()
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected 1690, got %v", err)
			require.Nil(t, results)
		})
	}

	t.Run("merge path", func(t *testing.T) {
		left := varianceExec(t, mp, AggIdOfVarPop, false, param)
		right := varianceExec(t, mp, AggIdOfVarPop, false, param)
		leftVal := varianceInput(t, mp, param, []float64{1e200}, nil)
		rightVal := varianceInput(t, mp, param, []float64{-1e200}, nil)
		require.NoError(t, left.GroupGrow(1))
		require.NoError(t, right.GroupGrow(1))
		require.NoError(t, left.BulkFill(0, []*vector.Vector{leftVal}))
		require.NoError(t, right.BulkFill(0, []*vector.Vector{rightVal}))
		require.NoError(t, left.Merge(right, 0, 0))
		results, err := left.Flush()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected 1690, got %v", err)
		require.Nil(t, results)
	})

	t.Run("legacy wire layout", func(t *testing.T) {
		lmp := newAggExecTestPool(t)
		legacyInput := vector.NewVec(param)
		t.Cleanup(func() { legacyInput.Free(lmp) })
		require.NoError(t, vector.AppendFixedList(legacyInput, []float64{1e200, -1e200}, nil, lmp))
		exec := makeVarPopExec(lmp, AggIdOfVarPop, false, param, true)
		t.Cleanup(exec.Free)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{legacyInput}))
		results, err := exec.Flush()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected 1690, got %v", err)
		require.Nil(t, results)
	})

	// The existing finite capability is preserved on the same extreme input.
	stddevCases := []struct {
		name string
		id   int64
		want float64
	}{
		{"stddev-pop", AggIdOfStdDevPop, 1e200},
		{"stddev-sample", AggIdOfStdDevSample, math.Sqrt2 * 1e200},
	}
	for _, tc := range stddevCases {
		t.Run(tc.name, func(t *testing.T) {
			exec := varianceExec(t, mp, tc.id, false, param)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{pair}))
			result := varianceResult(t, mp, exec, param, []bool{false})
			got := vector.MustFixedColWithTypeCheck[float64](result)[0]
			require.False(t, math.IsInf(got, 0))
			require.False(t, math.IsNaN(got))
			require.InEpsilon(t, tc.want, got, 1e-15)
		})
	}
}

// Issue 31355: with a normal group followed by an overflowing group, the
// error must abort the whole Flush; the partially built group result vectors
// are released by the deferred cleanup in Flush/flushLegacy and must not
// escape to the caller.
func TestVarianceGroupErrorDoesNotEmitPartialResults(t *testing.T) {
	param := types.T_float64.ToType()
	mp := newAggExecTestPool(t)
	normal := varianceInput(t, mp, param, []float64{1, 2, 3, 4}, nil)
	extreme := varianceInput(t, mp, param, []float64{1e200, -1e200}, nil)

	names := map[bool]string{false: "modern", true: "legacy"}
	for _, legacy := range []bool{false, true} {
		t.Run(names[legacy], func(t *testing.T) {
			exec := makeVarPopExec(mp, AggIdOfVarPop, false, param, legacy)
			t.Cleanup(exec.Free)
			require.NoError(t, exec.GroupGrow(2))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{normal}))
			require.NoError(t, exec.BulkFill(1, []*vector.Vector{extreme}))
			results, err := exec.Flush()
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected 1690, got %v", err)
			require.Nil(t, results, "partially emitted group vectors must not escape on error")
		})
	}
}

// Issue 31355: in the legacy sum/sumsq formula, finite same-sign large
// inputs make sumsq overflow to +Inf while avg*avg*cnt/denominator is +Inf
// as well, so the difference is Inf-Inf = NaN (for example VAR_POP over
// 1e200, 1e200). The legacy finalize must take the same ER_DATA_OUT_OF_RANGE
// branch instead of delivering NaN; the modern scaled path keeps the correct
// finite answer (0) available.
func TestVarianceLegacyInfInfNanSameSign(t *testing.T) {
	param := types.T_float64.ToType()

	t.Run("legacy var-pop", func(t *testing.T) {
		lmp := newAggExecTestPool(t)
		exec := makeVarPopExec(lmp, AggIdOfVarPop, false, param, true)
		t.Cleanup(exec.Free)
		input := vector.NewVec(param)
		t.Cleanup(func() { input.Free(lmp) })
		require.NoError(t, vector.AppendFixedList(input, []float64{1e200, 1e200}, nil, lmp))
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
		results, err := exec.Flush()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected 1690, got %v", err)
		require.Nil(t, results)
	})

	t.Run("modern path keeps exact zero", func(t *testing.T) {
		mp := newAggExecTestPool(t)
		pair := varianceInput(t, mp, param, []float64{1e200, 1e200}, nil)
		exec := varianceExec(t, mp, AggIdOfVarPop, false, param)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{pair}))
		got := vector.MustFixedColWithTypeCheck[float64](varianceResult(t, mp, exec, param, []bool{false}))[0]
		require.False(t, math.IsInf(got, 0))
		require.False(t, math.IsNaN(got))
		require.Equal(t, 0.0, got)
	})
}
