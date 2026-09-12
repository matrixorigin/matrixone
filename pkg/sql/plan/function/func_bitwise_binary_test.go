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
	"bytes"
	"context"
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBitwiseBinaryOperatorsMatchBinaryDomain(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name    string
		typ     types.Type
		notID   int32
		leftID  int32
		rightID int32
	}{
		{name: "binary", typ: types.New(types.T_binary, 8, 0), notID: 8, leftID: 4, rightID: 4},
		{name: "varbinary", typ: types.New(types.T_varbinary, 8, 0), notID: 9, leftID: 6, rightID: 6},
		{name: "varbinary over aggregate limit", typ: types.New(types.T_varbinary, 512, 0), notID: 9, leftID: 6, rightID: 6},
		{name: "blob", typ: types.New(types.T_blob, 1024, 0), notID: 10, leftID: 8, rightID: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			get, err := GetFunctionByName(ctx, "unary_tilde", []types.Type{test.typ})
			require.NoError(t, err)
			require.Equal(t, test.notID, get.overloadId)
			assertBinaryBitwiseResultType(t, test.typ, get.GetReturnType())

			for _, op := range []string{"<<", ">>"} {
				for _, countType := range []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()} {
					get, err = GetFunctionByName(ctx, op, []types.Type{test.typ, countType})
					require.NoError(t, err)
					wantID := test.leftID
					if countType.Oid == types.T_uint64 {
						wantID++
					}
					require.Equal(t, wantID, get.overloadId, "%s count %s", op, countType.Oid)
					targets, cast := get.ShouldDoImplicitTypeCast()
					require.False(t, cast)
					require.Nil(t, targets)
					assertBinaryBitwiseResultType(t, test.typ, get.GetReturnType())
				}
			}
		})
	}

	// The right count remains numeric even when the left value selects bytewise
	// evaluation. Textual counts follow the existing numeric-prefix cast path.
	left := types.New(types.T_varbinary, 8, 0)
	get, err := GetFunctionByName(ctx, "<<", []types.Type{left, types.T_varchar.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(6), get.overloadId)
	targets, cast := get.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, []types.Type{left, types.T_int64.ToType()}, targets)
	assertBinaryBitwiseResultType(t, left, get.GetReturnType())

	get, err = GetFunctionByName(ctx, "<<", []types.Type{left, types.T_any.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(6), get.overloadId)
	targets, cast = get.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, []types.Type{left, types.T_int64.ToType()}, targets)
	assertBinaryBitwiseResultType(t, left, get.GetReturnType())

	// New binary overloads must not attract ordinary text or numeric operands.
	for _, tc := range []struct {
		name string
		args []types.Type
		id   int32
	}{
		{name: "text prefix", args: []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}, id: 0},
		{name: "signed numeric", args: []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, id: 0},
		{name: "unsigned numeric", args: []types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, id: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"<<", ">>"} {
				get, err := GetFunctionByName(ctx, op, tc.args)
				require.NoError(t, err)
				require.Equal(t, tc.id, get.overloadId)
				require.Equal(t, types.T_uint64, get.GetReturnType().Oid)
			}
		})
	}
}

func cleanupBitwiseTestCase(t *testing.T, tc *FunctionTestCase) {
	t.Helper()
	t.Cleanup(func() {
		for _, parameter := range tc.parameters {
			parameter.Free(tc.proc.Mp())
		}
		tc.result.Free()
	})
}

func assertBinaryBitwiseResultType(t *testing.T, want, got types.Type) {
	t.Helper()
	require.Equal(t, want.Oid, got.Oid)
	require.Equal(t, want.Width, got.Width)
	require.Equal(t, types.CharsetBinary, got.Charset)
}

func TestBitwiseBinaryComplementAndShifts(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := []string{string([]byte{0x00, 0xff}), string([]byte{0x01, 0x02}), ""}

	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		typ := types.New(oid, 8, 0)
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(typ, input, nil)},
			NewFunctionTestResult(typ, false, []string{string([]byte{0xff, 0x00}), string([]byte{0xfe, 0xfd}), ""}, nil),
			operatorOpBitwiseBinaryNotFn)
		cleanupBitwiseTestCase(t, &tc)
		ok, info := tc.Run()
		require.True(t, ok, info, "complement %s", oid)
	}

	typ := types.New(types.T_varbinary, 8, 0)
	shiftCases := []struct {
		name  string
		fn    fEvalFn
		count []int64
		want  []string
	}{
		{
			name:  "left",
			fn:    operatorOpBitShiftLeftBinaryInt64Fn,
			count: []int64{1, 8, 0, 0},
			want:  []string{string([]byte{0x02, 0x04}), string([]byte{0x02, 0x00}), string([]byte{}), string([]byte{0xab})},
		},
		{
			name:  "right",
			fn:    operatorOpBitShiftRightBinaryInt64Fn,
			count: []int64{1, 8, 0, 0},
			want:  []string{string([]byte{0x00, 0x81}), string([]byte{0x00, 0x01}), string([]byte{}), string([]byte{0xab})},
		},
	}
	shiftInput := []string{string([]byte{0x01, 0x02}), string([]byte{0x01, 0x02}), "", string([]byte{0xab})}
	for _, test := range shiftCases {
		t.Run(test.name, func(t *testing.T) {
			tc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(typ, shiftInput, nil),
					NewFunctionTestInput(types.T_int64.ToType(), test.count, nil),
				},
				NewFunctionTestResult(typ, false, test.want, nil), test.fn)
			cleanupBitwiseTestCase(t, &tc)
			ok, info := tc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestBitwiseBinaryShiftHandlesLargeRowsAndCounts(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_varbinary, 512, 0)
	src := bytes.Repeat([]byte{0xff}, 512)
	wantOne := bytes.Repeat([]byte{0xff}, 511)
	wantOne = append(wantOne, 0xfe)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, []string{string(src), string(src)}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, math.MaxUint64}, nil),
		},
		NewFunctionTestResult(typ, false,
			[]string{string(wantOne), string(make([]byte, 512))}, nil),
		operatorOpBitShiftLeftBinaryUint64Fn)
	cleanupBitwiseTestCase(t, &tc)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseBinaryShiftHandlesNullsMasksAndConstants(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_varbinary, 8, 0)

	t.Run("mixed nulls and partial mask", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(typ,
					[]string{string([]byte{0x01, 0x02}), string([]byte{0x01, 0x02}), string([]byte{0x01, 0x02}), string([]byte{0x01, 0x02})},
					[]bool{false, true, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1, 1}, []bool{false, false, true, false}),
			},
			NewFunctionTestResult(typ, false,
				[]string{string([]byte{0x02, 0x04}), "", "", ""},
				[]bool{false, true, true, true}),
			operatorOpBitShiftLeftBinaryInt64Fn).WithSelectList(
			&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}})
		cleanupBitwiseTestCase(t, &tc)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})

	t.Run("all rows masked", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(typ, []string{"bad", "also-not-read"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 2}, nil),
			},
			NewFunctionTestResult(typ, false, []string{"", ""}, []bool{true, true}),
			operatorOpBitShiftLeftBinaryInt64Fn).WithSelectList(&FunctionSelectList{AllNull: true})
		cleanupBitwiseTestCase(t, &tc)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})

	t.Run("constant inputs", func(t *testing.T) {
		constValue := string([]byte{0x01, 0x02})
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(typ, []string{constValue, "ignored"}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{1, 9}, nil),
			},
			NewFunctionTestResult(typ, false,
				[]string{string([]byte{0x02, 0x04}), string([]byte{0x02, 0x04})}, nil),
			operatorOpBitShiftLeftBinaryInt64Fn)
		cleanupBitwiseTestCase(t, &tc)
		ok, info := tc.Run()
		require.True(t, ok, info)
		got, isNull := vector.GenerateFunctionStrParameter(tc.parameters[0]).GetStrValue(0)
		require.False(t, isNull)
		require.Equal(t, constValue, string(got), "operator mutated its input")
	})

	t.Run("constant result fallback", func(t *testing.T) {
		constValue := string([]byte{0x01, 0x02})
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(typ, []string{constValue, "ignored"}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{1, 9}, nil),
			},
			NewFunctionTestResult(typ, false, nil, nil),
			operatorOpBitShiftLeftBinaryInt64Fn)
		cleanupBitwiseTestCase(t, &tc)
		constResult, err := vector.NewConstBytes(typ, []byte("old"), 2, proc.Mp())
		require.NoError(t, err)
		tc.result.SetResultVector(constResult)
		require.NoError(t, tc.fn(tc.parameters, tc.result, proc, tc.fnLength, nil))
		got := vector.GenerateFunctionStrParameter(tc.result.GetResultVector())
		for row := uint64(0); row < 2; row++ {
			value, isNull := got.GetStrValue(row)
			require.False(t, isNull)
			require.Equal(t, string([]byte{0x02, 0x04}), string(value))
		}
	})
}

func TestBitwiseBinaryShiftMatchesFixedWidthOracle(t *testing.T) {
	for _, size := range []int{0, 1, 2, 16, 512} {
		src := make([]byte, size)
		for i := range src {
			src[i] = byte(i*37 + 11)
		}
		width := uint64(size) * 8
		counts := []uint64{0, 1, 7, 8, 9, 15, 16, 17, math.MaxUint64}
		if width > 0 {
			counts = append(counts, width-1, width, width+1)
		}
		for _, count := range counts {
			for _, test := range []struct {
				name string
				fn   func([]byte, []byte, uint64)
			}{
				{name: "left", fn: shiftBinaryLeft},
				{name: "right", fn: shiftBinaryRight},
			} {
				t.Run(test.name+"/size="+strconv.Itoa(size)+"/count="+strconv.FormatUint(count, 10), func(t *testing.T) {
					input := append([]byte(nil), src...)
					got := make([]byte, size)
					test.fn(src, got, count)
					require.True(t, bytes.Equal(input, src), "input was mutated")
					require.Equal(t, fixedWidthShiftOracle(src, count, test.name == "left"), got)
				})
			}
		}
	}
}

func fixedWidthShiftOracle(src []byte, count uint64, left bool) []byte {
	out := make([]byte, len(src))
	if len(src) == 0 || count >= uint64(len(src))*8 {
		return out
	}
	value := new(big.Int).SetBytes(src)
	if left {
		value.Lsh(value, uint(count))
		mask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(len(src)*8)), big.NewInt(1))
		value.And(value, mask)
	} else {
		value.Rsh(value, uint(count))
	}
	bytes := value.Bytes()
	copy(out[len(out)-len(bytes):], bytes)
	return out
}
