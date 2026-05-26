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
	"bytes"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeNulls(flags []bool) *nulls.Nulls {
	if len(flags) == 0 {
		return nil
	}
	ns := nulls.NewWithSize(len(flags))
	for i, flag := range flags {
		if flag {
			ns.Set(uint64(i))
		}
	}
	return ns
}

func mustConstFixedVector[T any](t *testing.T, typ types.Type, val T, length int, proc *process.Process) *vector.Vector {
	t.Helper()
	vec, err := vector.NewConstFixed(typ, val, length, proc.Mp())
	require.NoError(t, err)
	return vec
}

func mustConstBytesVector(t *testing.T, typ types.Type, val string, length int, proc *process.Process) *vector.Vector {
	t.Helper()
	vec, err := vector.NewConstBytes(typ, []byte(val), length, proc.Mp())
	require.NoError(t, err)
	return vec
}

func TestOpUnaryFixedToFixedWithNullOnError(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("const error turns all rows null", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryFixedToFixedWithNullOnError[float64, float64](parameters, result, proc, length, func(v float64) (float64, error) {
				if v < 0 {
					return 0, errors.New("neg")
				}
				return v + 1, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("row error only nulls failing row", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{1, -1, 3}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{2, 0, 4}, []bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryFixedToFixedWithNullOnError[float64, float64](parameters, result, proc, length, func(v float64) (float64, error) {
				if v < 0 {
					return 0, errors.New("neg")
				}
				return v + 1, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("const null turns all rows null", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{0}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryFixedToFixedWithNullOnError[float64, float64](parameters, result, proc, length, func(v float64) (float64, error) {
				return v + 1, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("null and error rows both become null", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{1, 0, -1}, []bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{2, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryFixedToFixedWithNullOnError[float64, float64](parameters, result, proc, length, func(v float64) (float64, error) {
				if v < 0 {
					return 0, errors.New("neg")
				}
				return v + 1, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("select list skips error row", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		input := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(input, []float64{-1, 2}, nil, proc.Mp())
		defer input.Free(proc.Mp())

		err := opUnaryFixedToFixedWithNullOnError[float64, float64]([]*vector.Vector{input}, result, proc, 2, func(v float64) (float64, error) {
			if v < 0 {
				return 0, errors.New("neg")
			}
			return v + 1, nil
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
		require.NoError(t, err)
		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionFixedTypeParameter[float64](vec).GetValue(1)
		require.False(t, null)
		require.Equal(t, 3.0, v)
	})

	t.Run("const success low level repeats value", func(t *testing.T) {
		v := mustConstFixedVector(t, types.T_float64.ToType(), float64(2), 3, proc)
		defer v.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		err := opUnaryFixedToFixedWithNullOnError[float64, float64]([]*vector.Vector{v}, result, proc, 3, func(v float64) (float64, error) {
			return v + 1, nil
		}, nil)
		require.NoError(t, err)

		p := vector.GenerateFunctionFixedTypeParameter[float64](result.GetResultVector())
		for i := uint64(0); i < 3; i++ {
			got, null := p.GetValue(i)
			require.False(t, null)
			require.Equal(t, 3.0, got)
		}
	})

	t.Run("ignore all rows low level", func(t *testing.T) {
		v := mustConstFixedVector(t, types.T_float64.ToType(), float64(-1), 2, proc)
		defer v.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		err := opUnaryFixedToFixedWithNullOnError[float64, float64]([]*vector.Vector{v}, result, proc, 2, func(v float64) (float64, error) {
			return 0, errors.New("neg")
		}, &FunctionSelectList{AllNull: true})
		require.NoError(t, err)

		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})
}

func TestOpBinaryFixedFixedToFixedWithNullOnError(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("const lhs with row error", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{10}, nil),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{2, 0, 5}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5, 0, 2}, []bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) (float64, error) {
				if v2 == 0 {
					return 0, errors.New("div0")
				}
				return v1 / v2, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("both const error nulls all rows", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{10}, nil),
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) (float64, error) {
				if v2 == 0 {
					return 0, errors.New("div0")
				}
				return v1 / v2, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("const rhs with row error", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{10, 20, 30}, nil),
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0, 0}, []bool{true, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) (float64, error) {
				if v2 == 0 {
					return 0, errors.New("div0")
				}
				return v1 / v2, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("both const nulls all rows", func(t *testing.T) {
		v1 := vector.NewConstNull(types.T_float64.ToType(), 3, proc.Mp())
		defer v1.Free(proc.Mp())
		v2 := vector.NewConstNull(types.T_float64.ToType(), 3, proc.Mp())
		defer v2.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			return v1 + v2, nil
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
		require.True(t, vec.IsNull(2))
	})

	t.Run("const rhs with input null and success rows", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		v1 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v1, []float64{10, 20, 30}, nil, proc.Mp())
		v1.SetNulls(makeNulls([]bool{false, true, false}))
		defer v1.Free(proc.Mp())
		v2 := mustConstFixedVector(t, types.T_float64.ToType(), float64(2), 3, proc)
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)

		vec := result.GetResultVector()
		p := vector.GenerateFunctionFixedTypeParameter[float64](vec)
		v, null := p.GetValue(0)
		require.False(t, null)
		require.Equal(t, 5.0, v)
		require.True(t, vec.IsNull(1))
		v, null = p.GetValue(2)
		require.False(t, null)
		require.Equal(t, 15.0, v)
	})

	t.Run("select list ignore all rows", func(t *testing.T) {
		v1 := mustConstFixedVector(t, types.T_float64.ToType(), float64(10), 2, proc)
		defer v1.Free(proc.Mp())
		v2 := mustConstFixedVector(t, types.T_float64.ToType(), float64(0), 2, proc)
		defer v2.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 2, func(v1, v2 float64) (float64, error) {
			return 0, errors.New("div0")
		}, &FunctionSelectList{AllNull: true})
		require.NoError(t, err)

		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("select list skips error row", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		v1 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v1, []float64{10, 10}, nil, proc.Mp())
		defer v1.Free(proc.Mp())
		v2 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v2, []float64{0, 2}, nil, proc.Mp())
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 2, func(v1, v2 float64) (float64, error) {
			if v2 == 0 {
				return 0, errors.New("div0")
			}
			return v1 / v2, nil
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
		require.NoError(t, err)

		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		p := vector.GenerateFunctionFixedTypeParameter[float64](vec)
		got, null := p.GetValue(1)
		require.False(t, null)
		require.Equal(t, 5.0, got)
	})

	t.Run("basic case with input null and row error", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		v1 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v1, []float64{10, 20, 30}, nil, proc.Mp())
		v1.SetNulls(makeNulls([]bool{false, true, false}))
		defer v1.Free(proc.Mp())
		v2 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v2, []float64{2, 2, 0}, nil, proc.Mp())
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			if v2 == 0 {
				return 0, errors.New("div0")
			}
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)

		vec := result.GetResultVector()
		p := vector.GenerateFunctionFixedTypeParameter[float64](vec)
		got, null := p.GetValue(0)
		require.False(t, null)
		require.Equal(t, 5.0, got)
		require.True(t, vec.IsNull(1))
		require.True(t, vec.IsNull(2))
	})

	t.Run("both const success fills all rows", func(t *testing.T) {
		v1 := mustConstFixedVector(t, types.T_float64.ToType(), float64(10), 3, proc)
		defer v1.Free(proc.Mp())
		v2 := mustConstFixedVector(t, types.T_float64.ToType(), float64(2), 3, proc)
		defer v2.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)
		p := vector.GenerateFunctionFixedTypeParameter[float64](result.GetResultVector())
		for i := uint64(0); i < 3; i++ {
			got, null := p.GetValue(i)
			require.False(t, null)
			require.Equal(t, 5.0, got)
		}
	})

	t.Run("const lhs null nulls all rows", func(t *testing.T) {
		v1 := vector.NewConstNull(types.T_float64.ToType(), 2, proc.Mp())
		defer v1.Free(proc.Mp())
		v2 := mustConstFixedVector(t, types.T_float64.ToType(), float64(2), 2, proc)
		defer v2.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 2, func(v1, v2 float64) (float64, error) {
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("const rhs null nulls all rows", func(t *testing.T) {
		v1 := mustConstFixedVector(t, types.T_float64.ToType(), float64(10), 2, proc)
		defer v1.Free(proc.Mp())
		v2 := vector.NewConstNull(types.T_float64.ToType(), 2, proc.Mp())
		defer v2.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 2, func(v1, v2 float64) (float64, error) {
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("const lhs with rhs any null", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		v1 := mustConstFixedVector(t, types.T_float64.ToType(), float64(10), 3, proc)
		defer v1.Free(proc.Mp())
		v2 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v2, []float64{2, 4, 5}, nil, proc.Mp())
		v2.SetNulls(makeNulls([]bool{false, true, false}))
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		p := vector.GenerateFunctionFixedTypeParameter[float64](vec)
		got, null := p.GetValue(0)
		require.False(t, null)
		require.Equal(t, 5.0, got)
		require.True(t, vec.IsNull(1))
		got, null = p.GetValue(2)
		require.False(t, null)
		require.Equal(t, 2.0, got)
	})

	t.Run("const lhs with rhs no null and error row", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		v1 := mustConstFixedVector(t, types.T_float64.ToType(), float64(10), 3, proc)
		defer v1.Free(proc.Mp())
		v2 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v2, []float64{2, 0, 5}, nil, proc.Mp())
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			if v2 == 0 {
				return 0, errors.New("div0")
			}
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		p := vector.GenerateFunctionFixedTypeParameter[float64](vec)
		got, null := p.GetValue(0)
		require.False(t, null)
		require.Equal(t, 5.0, got)
		require.True(t, vec.IsNull(1))
		got, null = p.GetValue(2)
		require.False(t, null)
		require.Equal(t, 2.0, got)
	})

	t.Run("const rhs with lhs any null", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		v1 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v1, []float64{10, 20, 30}, nil, proc.Mp())
		v1.SetNulls(makeNulls([]bool{false, true, false}))
		defer v1.Free(proc.Mp())
		v2 := mustConstFixedVector(t, types.T_float64.ToType(), float64(2), 3, proc)
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		p := vector.GenerateFunctionFixedTypeParameter[float64](vec)
		got, null := p.GetValue(0)
		require.False(t, null)
		require.Equal(t, 5.0, got)
		require.True(t, vec.IsNull(1))
		got, null = p.GetValue(2)
		require.False(t, null)
		require.Equal(t, 15.0, got)
	})

	t.Run("const rhs with lhs no null and error row", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(3))

		v1 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v1, []float64{10, 20, 30}, nil, proc.Mp())
		defer v1.Free(proc.Mp())
		v2 := mustConstFixedVector(t, types.T_float64.ToType(), float64(0), 3, proc)
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 3, func(v1, v2 float64) (float64, error) {
			if v2 == 0 {
				return 0, errors.New("div0")
			}
			return v1 / v2, nil
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true}})
		require.NoError(t, err)
		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
		require.True(t, vec.IsNull(2))
	})

	t.Run("basic case all success", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		v1 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v1, []float64{10, 30}, nil, proc.Mp())
		defer v1.Free(proc.Mp())
		v2 := vector.NewVec(types.T_float64.ToType())
		vector.AppendFixedList(v2, []float64{2, 5}, nil, proc.Mp())
		defer v2.Free(proc.Mp())

		err := opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64]([]*vector.Vector{v1, v2}, result, proc, 2, func(v1, v2 float64) (float64, error) {
			return v1 / v2, nil
		}, nil)
		require.NoError(t, err)
		p := vector.GenerateFunctionFixedTypeParameter[float64](result.GetResultVector())
		got, null := p.GetValue(0)
		require.False(t, null)
		require.Equal(t, 5.0, got)
		got, null = p.GetValue(1)
		require.False(t, null)
		require.Equal(t, 6.0, got)
	})
}

func TestOpUnaryBytesToBytesWithNullOnError(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("error row becomes null", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ok", "bad", "go"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"OK", "", "GO"}, []bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length, func(v []byte) ([]byte, error) {
				if string(v) == "bad" {
					return nil, errors.New("bad")
				}
				return bytes.ToUpper(v), nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("ignore all rows", func(t *testing.T) {
		vec := runJsonFunctionWithSelectListForBaseTemplate(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad", "still bad"}, nil),
			},
			types.T_varchar.ToType(),
			func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length, func(v []byte) ([]byte, error) {
					return nil, errors.New("bad")
				}, selectList)
			},
			&FunctionSelectList{AllNull: true},
		)
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("const success repeats value", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"ok"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"OK"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length, func(v []byte) ([]byte, error) {
				return bytes.ToUpper(v), nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("const null low level turns all rows null", func(t *testing.T) {
		v := vector.NewConstNull(types.T_varchar.ToType(), 2, proc.Mp())
		defer v.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		err := opUnaryBytesToBytesWithNullOnError([]*vector.Vector{v}, result, proc, 2, func(v []byte) ([]byte, error) {
			return bytes.ToUpper(v), nil
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("const error low level turns all rows null", func(t *testing.T) {
		v, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("bad"), 2, proc.Mp())
		require.NoError(t, err)
		defer v.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		err = opUnaryBytesToBytesWithNullOnError([]*vector.Vector{v}, result, proc, 2, func(v []byte) ([]byte, error) {
			return nil, errors.New("bad")
		}, nil)
		require.NoError(t, err)
		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("null and error rows both become null", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ok", "", "bad"}, []bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"OK", "", ""}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length, func(v []byte) ([]byte, error) {
				if string(v) == "bad" {
					return nil, errors.New("bad")
				}
				return bytes.ToUpper(v), nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("select list skips error row", func(t *testing.T) {
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))

		input := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(input, []string{"bad", "ok"}, nil, proc.Mp())
		defer input.Free(proc.Mp())

		err := opUnaryBytesToBytesWithNullOnError([]*vector.Vector{input}, result, proc, 2, func(v []byte) ([]byte, error) {
			if string(v) == "bad" {
				return nil, errors.New("bad")
			}
			return bytes.ToUpper(v), nil
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
		require.NoError(t, err)

		vec := result.GetResultVector()
		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, "OK", string(v))
	})
}

func runJsonFunctionWithSelectListForBaseTemplate(t *testing.T, proc *process.Process, inputs []FunctionTestInput, retType types.Type, fn fEvalFn, selectList *FunctionSelectList) *vector.Vector {
	t.Helper()
	fcTC := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(retType, false, nil, nil), fn)
	require.NoError(t, fcTC.result.PreExtendAndReset(fcTC.fnLength))
	require.NoError(t, fcTC.fn(fcTC.parameters, fcTC.result, fcTC.proc, fcTC.fnLength, selectList))
	return fcTC.result.GetResultVector()
}
