// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestParamExpressionExecutorTypedTransport(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	state := proc.DetachPrepareParams()
	t.Cleanup(func() { proc.RestorePrepareParams(state) })

	// install sets parameter 0: its TEXT spelling and, optionally, its typed value.
	install := func(text string, typed *vector.Vector) {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(text), false, proc.Mp()))
		proc.SetOwnedPrepareParamsWithIsBin(params, nil)
		if typed != nil {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = typed
			bat.SetRowCount(1)
			proc.SetTypedPrepareParams(bat)
		}
	}
	t.Cleanup(func() { proc.SetPrepareParams(nil) })
	newExecutor := func(typ types.Type) ExpressionExecutor {
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Typ:  typ.PlanType(),
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		})
		require.NoError(t, err)
		return executor
	}
	rows := func(n int) []*batch.Batch {
		bat := batch.NewWithSize(0)
		bat.SetRowCount(n)
		return []*batch.Batch{bat}
	}

	t.Run("typed value of the bound type is used as is", func(t *testing.T) {
		// The TEXT spelling disagrees on purpose: the typed value must win.
		typed, err := vector.NewConstFixed(types.T_float64.ToType(), math.NaN(), 1, proc.Mp())
		require.NoError(t, err)
		install("not a number", typed)
		executor := newExecutor(types.T_float64.ToType())
		defer executor.Free()
		for range 2 {
			got, err := executor.Eval(proc, rows(3), nil)
			require.NoError(t, err)
			require.True(t, got.IsConst())
			require.Equal(t, 3, got.Length())
			require.True(t, math.IsNaN(vector.MustFixedColNoTypeCheck[float64](got)[0]))
		}
	})

	t.Run("typed value of another type is cast from its source type", func(t *testing.T) {
		typed, err := vector.NewConstFixed(types.T_float64.ToType(), 2.5, 1, proc.Mp())
		require.NoError(t, err)
		install("ignored", typed)
		executor := newExecutor(types.T_int64.ToType())
		defer executor.Free()
		got, err := executor.Eval(proc, rows(2), nil)
		require.NoError(t, err)
		// DOUBLE -> BIGINT rounds; the TEXT path would have failed to parse.
		require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](got, 0))
	})

	t.Run("no typed value reads the TEXT transport", func(t *testing.T) {
		install("42", nil)
		executor := newExecutor(types.T_int64.ToType())
		defer executor.Free()
		got, err := executor.Eval(proc, rows(1), nil)
		require.NoError(t, err)
		require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](got, 0))
	})

	t.Run("route follows each execution", func(t *testing.T) {
		executor := newExecutor(types.T_int64.ToType())
		defer executor.Free()

		typed, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, proc.Mp())
		require.NoError(t, err)
		install("ignored", typed)
		got, err := executor.Eval(proc, rows(1), nil)
		require.NoError(t, err)
		require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](got, 0))

		executor.ResetForNextQuery()
		install("8", nil)
		got, err = executor.Eval(proc, rows(1), nil)
		require.NoError(t, err)
		require.Equal(t, int64(8), vector.GetFixedAtNoTypeCheck[int64](got, 0))

		executor.ResetForNextQuery()
		typed, err = vector.NewConstFixed(types.T_float64.ToType(), 8.5, 1, proc.Mp())
		require.NoError(t, err)
		install("ignored", typed)
		got, err = executor.Eval(proc, rows(1), nil)
		require.NoError(t, err)
		require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](got, 0))
	})

	t.Run("detached result is owned by the caller", func(t *testing.T) {
		typed, err := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		require.NoError(t, err)
		install("ignored", typed)
		executor := newExecutor(types.T_int64.ToType())
		defer executor.Free()
		got, err := executor.EvalWithoutResultReusing(proc, rows(1), nil)
		require.NoError(t, err)
		require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](got, 0))
		got.Free(proc.Mp())
	})

	t.Run("NULL typed value", func(t *testing.T) {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, nil, true, proc.Mp()))
		proc.SetOwnedPrepareParamsWithIsBin(params, nil)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
		bat.SetRowCount(1)
		proc.SetTypedPrepareParams(bat)
		executor := newExecutor(types.T_int64.ToType())
		defer executor.Free()
		got, err := executor.Eval(proc, rows(2), nil)
		require.NoError(t, err)
		require.True(t, got.IsNull(0))
	})
}
