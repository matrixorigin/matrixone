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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFieldExactTypeResolution(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inputs []types.Type
		idx    int
		target types.T
	}{
		{"decimal64", []types.Type{types.New(types.T_decimal64, 4, 2), types.New(types.T_decimal64, 3, 1)}, 11, types.T_decimal128},
		{"wide scale span", []types.Type{types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal128, 38, 38)}, 11, types.T_decimal128},
		{"decimal256", []types.Type{types.New(types.T_decimal256, 65, 30), types.New(types.T_decimal128, 38, 0)}, 12, types.T_decimal256},
		{"integer decimal", []types.Type{types.T_uint64.ToType(), types.New(types.T_decimal64, 10, 3)}, 11, types.T_decimal128},
		{"mixed integer", []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()}, 13, types.T_any},
		{"float control", []types.Type{types.T_float64.ToType(), types.New(types.T_decimal128, 38, 0)}, 10, types.T_float64},
		{"string control", []types.Type{types.T_varchar.ToType(), types.New(types.T_decimal128, 38, 0)}, 10, types.T_float64},
		{"binary strings", []types.Type{types.T_varbinary.ToType(), types.T_binary.ToType()}, 0, types.T_any},
		{"mixed string families", []types.Type{types.T_text.ToType(), types.T_blob.ToType()}, 0, types.T_any},
		{"bit", []types.Type{types.T_bit.ToType(), types.T_bit.ToType()}, 8, types.T_any},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := fieldCheck(nil, tc.inputs)
			require.Equal(t, tc.idx, got.idx)
			if tc.target == types.T_any {
				require.Equal(t, succeedMatched, got.status)
			} else {
				require.Equal(t, succeedWithCast, got.status)
				for i, target := range got.finalType {
					require.Equal(t, tc.target, target.Oid)
					if target.IsDecimal() {
						require.Equal(t, tc.inputs[i].Scale, target.Scale)
					}
				}
			}
		})
	}
}

func TestFieldStringSubjectDomain(t *testing.T) {
	for _, tc := range []struct {
		name          string
		subjectType   types.T
		candidateType types.T
		want          uint64
	}{
		{"binary", types.T_binary, types.T_binary, 2},
		{"varbinary", types.T_varbinary, types.T_varbinary, 2},
		{"blob", types.T_blob, types.T_blob, 2},
		{"binary subject text candidates", types.T_varbinary, types.T_varchar, 2},
		{"text subject binary candidates", types.T_varchar, types.T_varbinary, 1},
		{"text control", types.T_varchar, types.T_varchar, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			inputs := []FunctionTestInput{
				NewFunctionTestInput(tc.subjectType.ToType(), []string{"a", "\xff", "", "a\x00b", "missing", "ignored"}, []bool{false, false, false, false, false, true}),
				NewFunctionTestInput(tc.candidateType.ToType(), []string{"A", "\xfe", "", "a\x00c", "other", "ignored"}, []bool{false, false, true, false, false, false}),
				NewFunctionTestInput(tc.candidateType.ToType(), []string{"a", "\xff", "", "a\x00b", "other", "ignored"}, nil),
				NewFunctionTestInput(tc.candidateType.ToType(), []string{"a", "\xff", "", "a\x00b", "other", "ignored"}, nil),
			}
			fc := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{tc.want, tc.want, 2, 2, 0, 0}, nil), FieldString)
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestFieldStringRuntimeSubjectDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, subjectType := range []types.T{types.T_varchar, types.T_varbinary} {
		t.Run(subjectType.String(), func(t *testing.T) {
			subject := makeBinaryStringTestInput(t, proc, subjectType.ToType(), [][]byte{
				[]byte("a"), []byte("a"), {0xff}, {0xff},
			}, []types.RuntimeStringDomain{
				types.RuntimeStringText, types.RuntimeStringBinary, types.RuntimeStringText, types.RuntimeStringBinary,
			})
			defer subject.Free(proc.Mp())
			first := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{
				[]byte("A"), []byte("A"), {0xfe}, {0xfe},
			}, nil)
			defer first.Free(proc.Mp())
			second := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
				[]byte("a"), []byte("a"), {0xff}, {0xff},
			}, nil)
			defer second.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(subject.Length()))
			require.NoError(t, FieldString([]*vector.Vector{subject, first, second}, result, proc, subject.Length(), nil))
			require.Equal(t, []uint64{1, 2, 1, 2}, vector.MustFixedColNoTypeCheck[uint64](result.GetResultVector()))
		})
	}
}

func TestFieldStringConstantSubject(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestConstInput(types.T_varbinary.ToType(), []string{"a"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"A", "a", ""}, nil),
		NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a"}, nil),
	}
	fc := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{2, 1, 2}, nil), FieldString)
	// A constant subject has one physical row but broadcasts to the batch.
	fc.fnLength = 3
	ok, info := fc.Run()
	require.True(t, ok, info)
	require.Equal(t, 3, fc.GetResultVectorDirectly().Length())
	require.Equal(t, []uint64{2, 1, 2}, vector.MustFixedColNoTypeCheck[uint64](fc.GetResultVectorDirectly()))
}

func TestFieldIntegerRepresentations(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{^uint64(0), 1, 7}, nil),
		NewFunctionTestInput(types.T_int8.ToType(), []int8{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_int16.ToType(), []int16{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_int32.ToType(), []int32{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_uint8.ToType(), []uint8{255, 2, 8}, nil),
		NewFunctionTestInput(types.T_uint16.ToType(), []uint16{65535, 2, 8}, nil),
		NewFunctionTestInput(types.T_uint32.ToType(), []uint32{^uint32(0), 1, 8}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
	}
	fc := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 7, 0}, nil), FieldInteger)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
}

func TestFieldDecimalScalesAndNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.New(types.T_decimal128, 38, 1), []types.Decimal128{{B0_63: 12}, {B0_63: 12}, {}}, []bool{false, false, true}),
		NewFunctionTestInput(types.New(types.T_decimal128, 38, 2), []types.Decimal128{{B0_63: 120}, {B0_63: 121}, {}}, []bool{false, true, false}),
		NewFunctionTestConstInput(types.New(types.T_decimal128, 38, 2), []types.Decimal128{{B0_63: 120}}, nil),
	}
	fc := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 2, 0}, nil), FieldDecimal128)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)

	d256Inputs := []FunctionTestInput{
		NewFunctionTestInput(types.New(types.T_decimal256, 65, 1), []types.Decimal256{{B0_63: 12}, {B0_63: 13}, {}}, []bool{false, false, true}),
		NewFunctionTestInput(types.New(types.T_decimal256, 65, 2), []types.Decimal256{{B0_63: 120}, {B0_63: 121}, {}}, nil),
	}
	fc = NewFunctionTestCase(proc, d256Inputs,
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 0, 0}, nil), FieldDecimal256)
	ok, info = fc.RunAndFree()
	require.True(t, ok, info)
	require.True(t, decimal256Equal(types.Decimal256{B0_63: 120}, types.Decimal256{B0_63: 12}, 2, 1))
	require.True(t, decimal256Equal(types.Decimal256{B0_63: 12}, types.Decimal256{B0_63: 12}, 1, 1))
	maximum, err := types.ParseDecimal256("99999999999999999999999999999999999999999999999999999999999999999", 65, 0)
	require.NoError(t, err)
	require.False(t, decimal256Equal(maximum, types.Decimal256{B0_63: 1}, 0, 30))
	require.False(t, decimal256Equal(types.Decimal256{B0_63: 1}, maximum, 30, 0))
}
