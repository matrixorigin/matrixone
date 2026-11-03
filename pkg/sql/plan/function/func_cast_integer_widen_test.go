// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func checkIntegerWidening[T1, T2 constraints.Integer | constraints.Float](t *testing.T, source, target types.T, values []T1, want []T2, handled bool) {
	t.Helper()
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	input := newVectorByType(proc.Mp(), source.ToType(), values, nil)
	t.Cleanup(func() { input.Free(proc.Mp()) })
	before, err := input.MarshalBinary()
	require.NoError(t, err)
	result := vector.NewFunctionResultWrapper(target.ToType(), proc.Mp())
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(len(values)))
	from := vector.GenerateFunctionFixedTypeParameter[T1](input)
	to := vector.MustFunctionResult[T2](result)
	require.Equal(t, handled, widenIntegerBatch(from, to, len(values), nil))
	// The normal entry point also proves fallback behavior and wrapper metadata.
	require.NoError(t, result.PreExtendAndReset(len(values)))
	require.NoError(t, numericToNumeric(proc.Ctx, from, to, len(values), nil))
	require.Equal(t, want, vector.MustFixedColWithTypeCheck[T2](result.GetResultVector()))
	require.Equal(t, len(values), result.GetResultVector().Length())
	require.Equal(t, target.ToType(), *result.GetResultVector().GetType())
	require.True(t, result.GetResultVector().GetNulls().EmptyByFlag())
	after, err := input.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestIntegerWideningBatch(t *testing.T) {
	t.Run("int8", func(t *testing.T) {
		checkIntegerWidening(t, types.T_int8, types.T_int64, []int8{-128, 0, 127}, []int64{-128, 0, 127}, true)
	})
	t.Run("int16", func(t *testing.T) {
		checkIntegerWidening(t, types.T_int16, types.T_int64, []int16{-32768, 0, 32767}, []int64{-32768, 0, 32767}, true)
	})
	t.Run("int32", func(t *testing.T) {
		checkIntegerWidening(t, types.T_int32, types.T_int64, []int32{-2147483648, 0, 2147483647}, []int64{-2147483648, 0, 2147483647}, true)
	})
	t.Run("uint8", func(t *testing.T) {
		checkIntegerWidening(t, types.T_uint8, types.T_uint64, []uint8{0, 128, 255}, []uint64{0, 128, 255}, true)
	})
	t.Run("uint16", func(t *testing.T) {
		checkIntegerWidening(t, types.T_uint16, types.T_uint64, []uint16{0, 32768, 65535}, []uint64{0, 32768, 65535}, true)
	})
	t.Run("uint32", func(t *testing.T) {
		checkIntegerWidening(t, types.T_uint32, types.T_uint64, []uint32{0, 2147483648, 4294967295}, []uint64{0, 2147483648, 4294967295}, true)
	})
	t.Run("cross sign", func(t *testing.T) {
		checkIntegerWidening(t, types.T_uint32, types.T_int64, []uint32{4294967295}, []int64{4294967295}, false)
	})
	t.Run("64 bit", func(t *testing.T) {
		checkIntegerWidening(t, types.T_int64, types.T_int64, []int64{math.MinInt64, math.MaxInt64}, []int64{math.MinInt64, math.MaxInt64}, false)
	})
	t.Run("narrowing", func(t *testing.T) {
		checkIntegerWidening(t, types.T_int32, types.T_int16, []int32{-32768, 32767}, []int16{-32768, 32767}, false)
	})
	t.Run("float", func(t *testing.T) {
		checkIntegerWidening(t, types.T_float32, types.T_int64, []float32{-3, 7}, []int64{-3, 7}, false)
	})
}

func TestIntegerWideningSelectionAndReuse(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_int8.ToType(), []int8{-128, 0, 127}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
	}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-128, 0, 127}, nil), NewCast)
	t.Cleanup(fc.Free)
	source := fc.parameters[0]
	before, err := source.MarshalBinary()
	require.NoError(t, err)
	from := vector.GenerateFunctionFixedTypeParameter[int8](source)
	to := vector.MustFunctionResult[int64](fc.result)
	for _, tc := range []struct {
		name      string
		selection *FunctionSelectList
		handled   bool
	}{
		{"nil", nil, true},
		{"zero", &FunctionSelectList{}, true},
		{"all true", &FunctionSelectList{SelectList: []bool{true, true, true}}, true},
		{"stale bitmap", &FunctionSelectList{SelectList: []bool{false, false, false}}, true},
		{"partial", &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}, false},
		{"all bitmap", &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, false}}, false},
		{"all flag", &FunctionSelectList{AllNull: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, fc.result.PreExtendAndReset(3))
			dst := vector.MustFixedColWithTypeCheck[int64](fc.result.GetResultVector())
			copy(dst, []int64{91, 92, 93})
			require.Equal(t, tc.handled, widenIntegerBatch(from, to, 3, tc.selection))
			if !tc.handled {
				require.Equal(t, []int64{91, 92, 93}, dst, "rejection must not write")
			}
			fc.selectList = tc.selection
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
	// One result crosses fast/slow and size boundaries; NULLs cannot escape reset.
	for _, state := range []struct {
		length             int
		nullable, constant bool
	}{
		{3, false, false}, {1, true, false}, {3, false, false}, {3, false, true}, {0, false, false}, {1, false, false}, {3, false, false},
	} {
		source.GetNulls().Reset()
		if state.nullable {
			source.GetNulls().Add(0)
		}
		source.SetClass(vector.FLAT)
		if state.constant {
			source.SetClass(vector.CONSTANT)
		}
		require.NoError(t, fc.result.PreExtendAndReset(state.length))
		require.Equal(t, state.length > 0 && !state.nullable && !state.constant, widenIntegerBatch(from, to, state.length, nil))
		// Recreate the parameter wrapper after changing const/NULL classification.
		from = vector.GenerateFunctionFixedTypeParameter[int8](source)
		require.NoError(t, fc.result.PreExtendAndReset(state.length))
		require.NoError(t, numericToNumeric(proc.Ctx, from, to, state.length, nil))
		out := fc.result.GetResultVector()
		require.Equal(t, state.length, out.Length())
		for row := 0; row < state.length; row++ {
			require.Equal(t, state.nullable && row == 0, out.IsNull(uint64(row)))
			if !out.IsNull(uint64(row)) {
				want := []int64{-128, 0, 127}[row]
				if state.constant {
					want = -128
				}
				require.Equal(t, want, vector.MustFixedColWithTypeCheck[int64](out)[row])
			}
		}
	}
	source.SetClass(vector.FLAT)
	source.GetNulls().Reset()
	after, err := source.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, before, after)
	// The metadata-specific range check must precede widening, including recovery.
	to.TempSetType(types.New(types.T_int64, 2, 0))
	fc.fnLength = 3
	_, err = fc.DebugRun()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
	to.TempSetType(types.T_int64.ToType())
	fc.selectList = nil
	ok, info := fc.Run()
	require.True(t, ok, info)
	for _, fn := range []executeLogicOfOverload{NewExplicitCast, NewAssignCast} {
		fc.fn = fn
		ok, info = fc.Run()
		require.True(t, ok, info)
	}
}

func TestIntegerWideningConstNull(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	source := vector.NewConstNull(types.T_int8.ToType(), 3, proc.Mp())
	t.Cleanup(func() { source.Free(proc.Mp()) })
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(3))
	from := vector.GenerateFunctionFixedTypeParameter[int8](source)
	to := vector.MustFunctionResult[int64](result)
	require.False(t, widenIntegerBatch(from, to, 3, nil))
	require.NoError(t, numericToNumeric(proc.Ctx, from, to, 3, nil))
	for row := uint64(0); row < 3; row++ {
		require.True(t, result.GetResultVector().IsNull(row))
	}
}

func BenchmarkIntegerWideningCastPipeline(b *testing.B) {
	const rows = 8192
	for _, sourceOID := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_uint8, types.T_uint16, types.T_uint32} {
		for _, shape := range []string{"all rows", "nullable", "selection"} {
			b.Run(sourceOID.String()+"/"+shape, func(b *testing.B) {
				proc := newMemoryFunctionTestProcess(b)
				b.Cleanup(func() { proc.Free(); require.Zero(b, proc.Mp().CurrNB()) })
				targetOID := types.T_int64
				if sourceOID == types.T_uint8 || sourceOID == types.T_uint16 || sourceOID == types.T_uint32 {
					targetOID = types.T_uint64
				}
				// The fixture uses the real cast dispatcher and arithmetic kernel, with
				// reused result wrappers just as in expression evaluation.
				input := vector.NewVec(sourceOID.ToType())
				b.Cleanup(func() { input.Free(proc.Mp()) })
				for row := 0; row < rows; row++ {
					n := row%7 + 1
					null := shape == "nullable" && row == 0
					var err error
					switch sourceOID {
					case types.T_int8:
						err = vector.AppendFixed(input, int8(n), null, proc.Mp())
					case types.T_int16:
						err = vector.AppendFixed(input, int16(n), null, proc.Mp())
					case types.T_int32:
						err = vector.AppendFixed(input, int32(n), null, proc.Mp())
					case types.T_uint8:
						err = vector.AppendFixed(input, uint8(n), null, proc.Mp())
					case types.T_uint16:
						err = vector.AppendFixed(input, uint16(n), null, proc.Mp())
					case types.T_uint32:
						err = vector.AppendFixed(input, uint32(n), null, proc.Mp())
					}
					require.NoError(b, err)
				}
				target := vector.NewVec(targetOID.ToType())
				b.Cleanup(func() { target.Free(proc.Mp()) })
				cast := vector.NewFunctionResultWrapper(targetOID.ToType(), proc.Mp())
				b.Cleanup(cast.Free)
				result := vector.NewFunctionResultWrapper(targetOID.ToType(), proc.Mp())
				b.Cleanup(result.Free)
				selection := &FunctionSelectList{}
				if shape == "selection" {
					selection.AnyNull = true
					selection.SelectList = make([]bool, rows)
					for i := range selection.SelectList {
						selection.SelectList[i] = i%2 == 0
					}
				}
				parameters := []*vector.Vector{input, target}
				args := make([]*vector.Vector, 2)
				run := func() error {
					if err := cast.PreExtendAndReset(rows); err != nil {
						return err
					}
					if err := NewCast(parameters, cast, proc, rows, selection); err != nil {
						return err
					}
					args[0], args[1] = cast.GetResultVector(), cast.GetResultVector()
					if err := result.PreExtendAndReset(rows); err != nil {
						return err
					}
					return multiFn(args, result, proc, rows, selection)
				}
				before := proc.Mp().CurrNB()
				require.NoError(b, run())
				materialized := proc.Mp().CurrNB() - before
				out := result.GetResultVector()
				require.Equal(b, rows, out.Length())
				if targetOID == types.T_int64 {
					require.Equal(b, int64(9), vector.MustFixedColWithTypeCheck[int64](out)[2])
				} else {
					require.Equal(b, uint64(9), vector.MustFixedColWithTypeCheck[uint64](out)[2])
				}
				require.Equal(b, shape == "nullable", out.IsNull(0))
				b.ReportAllocs()
				b.SetBytes(rows * 8)
				b.ResetTimer()
				b.ReportMetric(float64(materialized), "materialized-B")
				for i := 0; i < b.N; i++ {
					if err := run(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
