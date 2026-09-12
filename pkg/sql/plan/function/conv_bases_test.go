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
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestConvRowDependentBases(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"ff", "1010", "-10", "z", "10", "10", "10"}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{16, 2, 10, 36, 1, math.MinInt64, 10}, []bool{false, false, false, false, false, false, true}),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{10, 16, -16, 10, 10, 10, 10}, nil),
	}
	fc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil), Conv)
	defer fc.result.Free()
	for _, v := range fc.parameters {
		defer v.Free(proc.Mp())
	}
	require.NoError(t, fc.result.PreExtendAndReset(7))
	require.NoError(t, Conv(fc.parameters, fc.result, proc, 7, nil))
	got := fc.result.GetResultVector()
	for i, want := range []string{"255", "A", "-A", "35"} {
		require.Equal(t, want, string(got.GetBytesAt(i)))
		require.False(t, got.IsNull(uint64(i)))
	}
	for i := 4; i < 7; i++ {
		require.True(t, got.IsNull(uint64(i)))
	}
	// Reuse must not leak values across masked/NULL rows.
	require.NoError(t, fc.result.PreExtendAndReset(7))
	require.NoError(t, Conv(fc.parameters, fc.result, proc, 7, &FunctionSelectList{AllNull: true}))
	for i := 0; i < 7; i++ {
		require.True(t, got.IsNull(uint64(i)))
	}
}

func TestConvDynamicBasesPreserveTypedInputs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input FunctionTestInput
		want  []string
	}{
		{"string", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"10"}, nil), []string{"2", "A", "16"}},
		{"signed", NewFunctionTestConstInput(types.T_int64.ToType(), []int64{10}, nil), []string{"2", "A", "16"}},
		{"unsigned", NewFunctionTestConstInput(types.T_uint64.ToType(), []uint64{10}, nil), []string{"2", "A", "16"}},
		{"bit", NewFunctionTestConstInput(types.T_bit.ToType(), []uint64{10}, nil), []string{"10", "A", "10"}},
		{"bool", NewFunctionTestConstInput(types.T_bool.ToType(), []bool{true}, nil), []string{"1", "1", "1"}},
		{"float", NewFunctionTestConstInput(types.T_float64.ToType(), []float64{10}, nil), []string{"2", "A", "16"}},
		{"decimal", NewFunctionTestConstInput(types.New(types.T_decimal128, 20, 0), []types.Decimal128{{B0_63: 10}}, nil), []string{"2", "A", "16"}},
		{"year", NewFunctionTestConstInput(types.T_year.ToType(), []types.MoYear{2024}, nil), []string{"0", "7E8", "8228"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			fc := NewFunctionTestCase(proc, []FunctionTestInput{tc.input,
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{2, 10, 16}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{10, 16, 10}, nil)},
				NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil), Conv)
			defer fc.result.Free()
			for _, v := range fc.parameters {
				defer v.Free(proc.Mp())
			}
			require.NoError(t, fc.result.PreExtendAndReset(3))
			require.NoError(t, Conv(fc.parameters, fc.result, proc, 3, nil))
			for i, want := range tc.want {
				require.Equal(t, want, string(fc.result.GetResultVector().GetBytesAt(i)))
			}
		})
	}
}

func TestConvBaseBindingAndUnsignedBoundaries(t *testing.T) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_uint8, types.T_uint16, types.T_uint32, types.T_any, types.T_text} {
		got, err := GetFunctionByName(context.Background(), "conv", []types.Type{types.T_bit.ToType(), oid.ToType(), oid.ToType()})
		require.NoError(t, err)
		cast, ok := got.ShouldDoImplicitTypeCast()
		require.True(t, ok)
		require.Equal(t, types.T_bit, cast[0].Oid)
		require.Equal(t, types.T_int64, cast[1].Oid)
		require.Equal(t, types.T_int64, cast[2].Oid)
	}
	proc := testutil.NewProcess(t)
	defer proc.Free()
	v := vector.NewVec(types.T_uint64.ToType())
	defer v.Free(proc.Mp())
	require.NoError(t, vector.AppendFixedList(v, []uint64{2, 36, 37, math.MaxUint64, math.MaxUint64 - 9}, nil, proc.Mp()))
	base, err := newConvBase(v)
	require.NoError(t, err)
	for i, want := range []bool{true, true, false, false, false} {
		_, ok := base.at(uint64(i))
		require.Equal(t, want, ok)
	}
}

func BenchmarkConvConstantBases(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	const rows = 1024
	values := make([]string, rows)
	for i := range values {
		values[i] = "12345"
	}
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), values, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{10}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{16}, nil),
	}, NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil), Conv)
	defer fc.result.Free()
	for _, v := range fc.parameters {
		defer v.Free(proc.Mp())
	}
	require.NoError(b, fc.result.PreExtendAndReset(rows))
	require.NoError(b, Conv(fc.parameters, fc.result, proc, rows, nil))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fc.result.PreExtendAndReset(rows); err != nil {
			b.Fatal(err)
		}
		if err := Conv(fc.parameters, fc.result, proc, rows, nil); err != nil {
			b.Fatal(err)
		}
	}
}
