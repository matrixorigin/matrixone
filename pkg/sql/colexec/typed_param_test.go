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
	"strings"
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
		params := vector.NewVec(types.T_text.ToType(), proc.Mp())
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
		params := vector.NewVec(types.T_text.ToType(), proc.Mp())
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

func TestParamExpressionExecutorTypedIntegerConversion(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	state := proc.DetachPrepareParams()
	t.Cleanup(func() { proc.RestorePrepareParams(state) })
	t.Cleanup(func() { proc.SetPrepareParams(nil) })

	install := func(typed *vector.Vector) {
		params := vector.NewVec(types.T_text.ToType(), proc.Mp())
		require.NoError(t, vector.AppendBytes(params, []byte("ignored"), false, proc.Mp()))
		proc.SetOwnedPrepareParamsWithIsBin(params, nil)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = typed
		bat.SetRowCount(1)
		proc.SetTypedPrepareParams(bat)
	}
	constOf := func(typ types.T, v any) *vector.Vector {
		var vec *vector.Vector
		var err error
		switch x := v.(type) {
		case int64:
			vec, err = vector.NewConstFixed(typ.ToType(), x, 1, proc.Mp())
		case uint64:
			vec, err = vector.NewConstFixed(typ.ToType(), x, 1, proc.Mp())
		case int32:
			vec, err = vector.NewConstFixed(typ.ToType(), x, 1, proc.Mp())
		}
		require.NoError(t, err)
		return vec
	}
	newExecutor := func(typ types.T) *ParamExpressionExecutor {
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Typ:  typ.ToType().PlanType(),
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		})
		require.NoError(t, err)
		return executor.(*ParamExpressionExecutor)
	}
	rows := func(n int) []*batch.Batch {
		bat := batch.NewWithSize(0)
		bat.SetRowCount(n)
		return []*batch.Batch{bat}
	}

	t.Run("in-range integers convert without a cast", func(t *testing.T) {
		cases := []struct {
			from, to types.T
			value    any
			check    func(*vector.Vector)
		}{
			{types.T_int64, types.T_int32, int64(7), func(v *vector.Vector) {
				require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](v, 0))
			}},
			{types.T_int64, types.T_int32, int64(math.MinInt32), func(v *vector.Vector) {
				require.Equal(t, int32(math.MinInt32), vector.GetFixedAtNoTypeCheck[int32](v, 0))
			}},
			{types.T_int64, types.T_uint8, int64(255), func(v *vector.Vector) {
				require.Equal(t, uint8(255), vector.GetFixedAtNoTypeCheck[uint8](v, 0))
			}},
			{types.T_uint64, types.T_int64, uint64(math.MaxInt64), func(v *vector.Vector) {
				require.Equal(t, int64(math.MaxInt64), vector.GetFixedAtNoTypeCheck[int64](v, 0))
			}},
			{types.T_int32, types.T_uint64, int32(5), func(v *vector.Vector) {
				require.Equal(t, uint64(5), vector.GetFixedAtNoTypeCheck[uint64](v, 0))
			}},
		}
		for _, c := range cases {
			install(constOf(c.from, c.value))
			executor := newExecutor(c.to)
			for range 2 {
				got, err := executor.Eval(proc, rows(3), nil)
				require.NoError(t, err)
				require.True(t, got.IsConst())
				require.Equal(t, 3, got.Length())
				require.Equal(t, c.to, got.GetType().Oid)
				c.check(got)
			}
			require.Nil(t, executor.adapter)
			executor.Free()
		}
	})

	t.Run("each execution converts its own value", func(t *testing.T) {
		executor := newExecutor(types.T_int32)
		defer executor.Free()
		install(constOf(types.T_int64, int64(8)))
		got, err := executor.Eval(proc, rows(1), nil)
		require.NoError(t, err)
		require.Equal(t, int32(8), vector.GetFixedAtNoTypeCheck[int32](got, 0))
		executor.ResetForNextQuery()
		install(constOf(types.T_int64, int64(9)))
		got, err = executor.Eval(proc, rows(1), nil)
		require.NoError(t, err)
		require.Equal(t, int32(9), vector.GetFixedAtNoTypeCheck[int32](got, 0))
	})

	t.Run("out-of-range values keep the cast and its error", func(t *testing.T) {
		cases := []struct {
			from, to types.T
			value    any
		}{
			{types.T_int64, types.T_int32, int64(math.MaxInt32) + 1},
			{types.T_int64, types.T_int32, int64(math.MinInt32) - 1},
			{types.T_int64, types.T_uint64, int64(-1)},
			{types.T_uint64, types.T_int64, uint64(math.MaxInt64) + 1},
		}
		for _, c := range cases {
			install(constOf(c.from, c.value))
			executor := newExecutor(c.to)
			_, err := executor.Eval(proc, rows(1), nil)
			require.Error(t, err)
			require.NotNil(t, executor.adapter)
			// Rows that are not selected never see the error.
			executor.ResetForNextQuery()
			got, err := executor.Eval(proc, rows(2), boolSelection([]bool{false, false}))
			require.NoError(t, err)
			require.True(t, got.IsNull(0))
			executor.Free()
		}
	})
}

// Reading a typed parameter allocates nothing per executor: executors
// reference the process-held value, and a narrowed integer is converted once.
func TestParamExpressionExecutorTypedReadDoesNotAllocate(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	state := proc.DetachPrepareParams()
	t.Cleanup(func() { proc.RestorePrepareParams(state) })
	t.Cleanup(func() { proc.SetPrepareParams(nil) })

	params := vector.NewVec(types.T_text.ToType(), proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("ignored"), false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithIsBin(params, nil)
	typed, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 1, proc.Mp())
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = typed
	bat.SetRowCount(1)
	proc.SetTypedPrepareParams(bat)

	rows := batch.NewWithSize(0)
	rows.SetRowCount(4)
	read := func(typ types.T) {
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Typ:  typ.ToType().PlanType(),
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		})
		require.NoError(t, err)
		defer executor.Free()
		got, err := executor.Eval(proc, []*batch.Batch{rows}, nil)
		require.NoError(t, err)
		require.Equal(t, 4, got.Length())
		require.True(t, got.IsReadOnly(), "a parameter is referenced, not copied")
		require.Equal(t, typ, got.GetType().Oid)
	}
	read(types.T_int32) // first narrowed read materializes the conversion
	allocs := proc.Mp().Stats().NumAlloc.Load()
	for range 3 {
		read(types.T_int64)
		read(types.T_int32)
	}
	require.Equal(t, allocs, proc.Mp().Stats().NumAlloc.Load())
	// The shared value is untouched by the executors' row counts.
	require.Equal(t, 1, typed.Length())
}

// A caller that keeps a string parameter gets its value only, not the TEXT
// transport area shared by every parameter of the execution.
func TestParamExpressionExecutorOwnedStringCopiesOnlyItsValue(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	state := proc.DetachPrepareParams()
	t.Cleanup(func() { proc.RestorePrepareParams(state) })
	t.Cleanup(func() { proc.SetPrepareParams(nil) })

	large := strings.Repeat("x", 1<<20)
	medium := "an out-of-line parameter longer than the inline varlena"
	params := vector.NewVec(types.T_text.ToType(), proc.Mp())
	for _, v := range []string{"ok", large, medium} {
		require.NoError(t, vector.AppendBytes(params, []byte(v), false, proc.Mp()))
	}
	proc.SetOwnedPrepareParamsWithIsBin(params, nil)
	rows := batch.NewWithSize(0)
	rows.SetRowCount(3)

	for _, test := range []struct {
		pos  int32
		typ  types.T
		want string
	}{
		{0, types.T_varchar, "ok"},
		{2, types.T_text, medium},
	} {
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Typ:  test.typ.ToType().PlanType(),
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: test.pos}},
		})
		require.NoError(t, err)
		owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{rows}, nil)
		require.NoError(t, err)
		require.False(t, owned.IsReadOnly())
		require.True(t, owned.IsConst())
		require.Equal(t, 3, owned.Length())
		require.Equal(t, test.typ, owned.GetType().Oid)
		require.Equal(t, test.want, owned.GetStringAt(0))
		require.Less(t, len(owned.GetArea()), 1024, "the unrelated 1 MiB parameter must not be copied")
		require.Less(t, owned.Allocated(), 4096)
		owned.Free(proc.Mp())
		executor.Free()
	}
}

// A string parameter is a read-only reference to its row of the TEXT
// transport: fresh executors read it without allocating.
func TestParamExpressionExecutorStringReadDoesNotAllocate(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	state := proc.DetachPrepareParams()
	t.Cleanup(func() { proc.RestorePrepareParams(state) })
	t.Cleanup(func() { proc.SetPrepareParams(nil) })

	long := "a string parameter longer than the inline varlena"
	install := func(values ...string) {
		params := vector.NewVec(types.T_text.ToType(), proc.Mp())
		for _, v := range values {
			require.NoError(t, vector.AppendBytes(params, []byte(v), false, proc.Mp()))
		}
		proc.SetOwnedPrepareParamsWithIsBin(params, nil)
	}
	install("short", long)
	rows := batch.NewWithSize(0)
	rows.SetRowCount(3)
	newExecutor := func(pos int32, typ types.T) ExpressionExecutor {
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Typ:  typ.ToType().PlanType(),
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
		})
		require.NoError(t, err)
		return executor
	}
	read := func(pos int32, typ types.T, want string) {
		executor := newExecutor(pos, typ)
		defer executor.Free()
		got, err := executor.Eval(proc, []*batch.Batch{rows}, nil)
		require.NoError(t, err)
		require.True(t, got.IsConst())
		require.True(t, got.IsReadOnly())
		require.Equal(t, 3, got.Length())
		require.Equal(t, typ, got.GetType().Oid)
		require.Equal(t, want, got.GetStringAt(0))
	}
	allocs := proc.Mp().Stats().NumAlloc.Load()
	for range 3 {
		read(0, types.T_varchar, "short")
		read(1, types.T_varchar, long)
		read(1, types.T_text, long)
	}
	require.Equal(t, allocs, proc.Mp().Stats().NumAlloc.Load())

	// A caller that keeps the result gets its own copy.
	executor := newExecutor(1, types.T_varchar)
	defer executor.Free()
	owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{rows}, nil)
	require.NoError(t, err)
	require.False(t, owned.IsReadOnly())
	require.Equal(t, long, owned.GetStringAt(0))
	owned.Free(proc.Mp())

	// New TEXT parameters are read again, never through a stale reference.
	_, err = executor.Eval(proc, []*batch.Batch{rows}, nil)
	require.NoError(t, err)
	install("next", "values")
	got, err := executor.Eval(proc, []*batch.Batch{rows}, nil)
	require.NoError(t, err)
	require.Equal(t, "values", got.GetStringAt(0))
}
