// Copyright 2021 - 2022 Matrix Origin
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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func Test_fixedTypeCastRule1(t *testing.T) {
	inputs := []struct {
		shouldCast bool
		in         [2]types.Type
		want       [2]types.Type
	}{
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_int32.ToType()},
			want:       [2]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		},

		{
			shouldCast: false,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_binary.ToType(), types.T_varbinary.ToType()},
			want:       [2]types.Type{types.T_varbinary.ToType(), types.T_varbinary.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_varbinary.ToType(), types.T_binary.ToType()},
			want:       [2]types.Type{types.T_varbinary.ToType(), types.T_varbinary.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_json.ToType(), types.T_bool.ToType()},
			want:       [2]types.Type{types.T_bool.ToType(), types.T_bool.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_bool.ToType(), types.T_json.ToType()},
			want:       [2]types.Type{types.T_bool.ToType(), types.T_bool.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_binary.ToType(), types.T_blob.ToType()},
			want:       [2]types.Type{types.T_blob.ToType(), types.T_blob.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_blob.ToType(), types.T_binary.ToType()},
			want:       [2]types.Type{types.T_blob.ToType(), types.T_blob.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_binary.ToType(), types.T_text.ToType()},
			want:       [2]types.Type{types.T_blob.ToType(), types.T_blob.ToType()},
		},

		{
			shouldCast: true,
			in:         [2]types.Type{types.T_text.ToType(), types.T_binary.ToType()},
			want:       [2]types.Type{types.T_blob.ToType(), types.T_blob.ToType()},
		},

		{
			shouldCast: true,
			in: [2]types.Type{
				{Oid: types.T_decimal64, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
			want: [2]types.Type{
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
		},

		// special rule, null + null
		// we just cast it as int64 + int64
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_any.ToType(), types.T_any.ToType()},
			want:       [2]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		},
	}

	for i, in := range inputs {
		msg := fmt.Sprintf("i = %d", i)

		cast, t1, t2 := fixedTypeCastRule1(in.in[0], in.in[1])
		require.Equal(t, in.shouldCast, cast, msg)
		if in.shouldCast {
			require.Equal(t, in.want[0], t1, msg)
			require.Equal(t, in.want[1], t2, msg)
		}
	}
}

func TestComparisonTypeCastRulePreservesTextCharset(t *testing.T) {
	for _, test := range []struct {
		name     string
		left     types.Type
		right    types.Type
		leftOut  uint8
		rightOut uint8
		hasCast  bool
	}{
		{
			name:     "legacy column and default parameter",
			left:     types.NewWithCharset(types.T_varchar, 12, 0, types.CharsetLegacy),
			right:    types.T_varchar.ToType(),
			leftOut:  types.CharsetLegacy,
			rightOut: types.CharsetLegacy,
			hasCast:  true,
		},
		{
			name:     "utf8mb4 bin column and default literal",
			left:     types.NewWithCharset(types.T_varchar, 12, 0, types.CharsetUTF8MB4Bin),
			right:    types.T_varchar.ToType(),
			leftOut:  types.CharsetUTF8MB4Bin,
			rightOut: types.CharsetUTF8MB4Bin,
			hasCast:  true,
		},
		{
			name:     "legacy column and opaque binary value share byte domain",
			left:     types.NewWithCharset(types.T_varchar, 12, 0, types.CharsetLegacy),
			right:    types.NewWithCharset(types.T_varchar, 12, 0, types.CharsetBinary),
			leftOut:  types.CharsetLegacy,
			rightOut: types.CharsetBinary,
			hasCast:  false,
		},
		{
			name:     "ordinary general ci operands",
			left:     types.T_varchar.ToType(),
			right:    types.T_text.ToType(),
			leftOut:  types.CharsetUTF8,
			rightOut: types.CharsetUTF8,
			hasCast:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			hasCast, left, right := comparisonTypeCastRule(test.left, test.right)
			require.Equal(t, test.hasCast, hasCast)
			require.Equal(t, test.leftOut, left.Charset)
			require.Equal(t, test.rightOut, right.Charset)
		})
	}
}

func TestComparisonTypeCastRuleNormalizesCharToVarchar(t *testing.T) {
	leftIn := types.NewWithCharset(types.T_char, 8, 0, types.CharsetLegacy)
	rightIn := types.NewWithCharset(types.T_char, 4, 0, types.CharsetLegacy)

	hasCast, leftOut, rightOut := comparisonTypeCastRule(leftIn, rightIn)

	require.True(t, hasCast)
	require.Equal(t, types.T_varchar, leftOut.Oid)
	require.Equal(t, types.T_varchar, rightOut.Oid)
	require.Equal(t, leftIn.Width, leftOut.Width)
	require.Equal(t, rightIn.Width, rightOut.Width)
	require.Equal(t, types.CharsetLegacy, leftOut.Charset)
	require.Equal(t, types.CharsetLegacy, rightOut.Charset)
}

func Test_fixedTypeCastRule2(t *testing.T) {
	inputs := []struct {
		shouldCast bool
		in         [2]types.Type
		want       [2]types.Type
	}{
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_int32.ToType()},
			want:       [2]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
		},
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_uint64.ToType(), types.T_int64.ToType()},
			want:       [2]types.Type{types.T_decimal128.ToType(), types.T_decimal128.ToType()},
		},
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_uint64.ToType()},
			want:       [2]types.Type{types.T_decimal128.ToType(), types.T_decimal128.ToType()},
		},

		{
			shouldCast: false,
			in:         [2]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
		},

		{
			shouldCast: true,
			in: [2]types.Type{
				{Oid: types.T_decimal64, Width: 38, Size: 16, Scale: 6},
				types.T_float64.ToType(),
			},
			// After optimization: decimal64 + float64 converts to float64
			// This balances performance and precision (float64 has 15-16 digits)
			want: [2]types.Type{
				{Oid: types.T_float64, Width: 0, Size: 8, Scale: 6},
				{Oid: types.T_float64, Width: 0, Size: 8, Scale: 0},
			},
		},

		{
			shouldCast: true,
			in: [2]types.Type{
				{Oid: types.T_decimal64, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
			want: [2]types.Type{
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
		},

		// special rule, null / null
		// we just cast it as float64 / float64
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_any.ToType(), types.T_any.ToType()},
			want:       [2]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
		},
	}

	for i, in := range inputs {
		msg := fmt.Sprintf("i = %d", i)

		cast, t1, t2 := fixedTypeCastRule2(in.in[0], in.in[1])
		require.Equal(t, in.shouldCast, cast, msg)
		if in.shouldCast {
			require.Equal(t, in.want[0], t1, msg)
			require.Equal(t, in.want[1], t2, msg)
		}
	}
}

func Test_fixedImplicitTypeCast_Decimal256MirrorsDecimal128(t *testing.T) {
	for _, target := range []types.T{types.T_bool, types.T_timestamp} {
		can128, cost128 := fixedImplicitTypeCast(types.T_decimal128.ToType(), target)
		require.True(t, can128)

		can256, cost256 := fixedImplicitTypeCast(types.T_decimal256.ToType(), target)
		require.Equal(t, can128, can256, target.String())
		require.Equal(t, cost128, cost256, target.String())
	}
}

func Test_GetFunctionByName(t *testing.T) {
	type fInput struct {
		name string
		args []types.Type

		// expected
		shouldErr bool

		requireFid int32
		requireOid int32

		shouldCast bool
		requireTyp []types.Type

		requireRet types.Type
	}

	cs := []fInput{
		{
			name: "+", args: []types.Type{types.T_int8.ToType(), types.T_int16.ToType()},
			shouldErr:  false,
			requireFid: PLUS, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_int16.ToType(), types.T_int16.ToType()},
			requireRet: types.T_int16.ToType(),
		},

		{
			name: "+", args: []types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
			shouldErr:  false,
			requireFid: PLUS, requireOid: 0,
			shouldCast: false,
			requireRet: types.T_int64.ToType(),
		},

		{
			name: "/", args: []types.Type{types.T_int8.ToType(), types.T_int16.ToType()},
			shouldErr:  false,
			requireFid: DIV, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
			requireRet: types.T_float64.ToType(),
		},
		{
			name: "/", args: []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()},
			shouldErr:  false,
			requireFid: DIV, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_decimal128.ToType(), types.T_decimal128.ToType()},
			requireRet: types.New(types.T_decimal128, 38, 6),
		},
		{
			name: "/", args: []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()},
			shouldErr:  false,
			requireFid: DIV, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_decimal128.ToType(), types.T_decimal128.ToType()},
			requireRet: types.New(types.T_decimal128, 38, 6),
		},

		{
			name: "from_unixtime", args: []types.Type{types.New(types.T_decimal256, 65, 0)},
			shouldErr:  false,
			requireFid: FROM_UNIXTIME, requireOid: 3,
			shouldCast: false,
			requireRet: types.T_datetime.ToType(),
		},

		{
			name: "internal_numeric_scale", args: []types.Type{types.T_char.ToType()},
			shouldErr:  false,
			requireFid: INTERNAL_NUMERIC_SCALE, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_varchar.ToType()},
			requireRet: types.T_int64.ToType(),
		},

		{
			name: "internal_numeric_scale", args: []types.Type{types.T_char.ToType(), types.T_int64.ToType()},
			shouldErr: true,
		},
		{
			name: "char_length", args: []types.Type{types.T_binary.ToType()},
			shouldErr:  false,
			requireFid: LENGTH_UTF8, requireOid: 3,
			shouldCast: false,
			requireRet: types.T_uint64.ToType(),
		},
		{
			name: "char_length", args: []types.Type{types.T_varbinary.ToType()},
			shouldErr:  false,
			requireFid: LENGTH_UTF8, requireOid: 4,
			shouldCast: false,
			requireRet: types.T_uint64.ToType(),
		},
		{
			name: "char_length", args: []types.Type{types.T_blob.ToType()},
			shouldErr:  false,
			requireFid: LENGTH_UTF8, requireOid: 5,
			shouldCast: false,
			requireRet: types.T_uint64.ToType(),
		},
		{
			name: "character_length", args: []types.Type{types.T_varbinary.ToType()},
			shouldErr:  false,
			requireFid: LENGTH_UTF8, requireOid: 4,
			shouldCast: false,
			requireRet: types.T_uint64.ToType(),
		},

		{
			name: "iff", args: []types.Type{types.T_bool.ToType(), types.T_any.ToType(), types.T_int64.ToType()},
			shouldErr:  false,
			requireFid: IFF, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_bool.ToType(), types.T_int64.ToType(), types.T_int64.ToType()},
			requireRet: types.T_int64.ToType(),
		},
		{
			name: "elt", args: []types.Type{types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
			shouldErr:  false,
			requireFid: ELT, requireOid: 0,
			shouldCast: false,
			requireRet: types.T_varchar.ToType(),
		},
		{
			name: "elt", args: []types.Type{types.T_bit.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
			shouldErr:  false,
			requireFid: ELT, requireOid: 0,
			shouldCast: false,
			requireRet: types.T_varchar.ToType(),
		},
		{
			name: "uuid_to_bin", args: []types.Type{types.T_varchar.ToType(), types.T_float64.ToType()},
			shouldErr:  false,
			requireFid: UUID_TO_BIN, requireOid: 0,
			shouldCast: false,
			requireRet: types.T_varbinary.ToType(),
		},
		{
			name: "bin_to_uuid", args: []types.Type{types.T_varbinary.ToType(), types.T_float64.ToType()},
			shouldErr:  false,
			requireFid: BIN_TO_UUID, requireOid: 0,
			shouldCast: false,
			requireRet: types.T_varchar.ToType(),
		},
		{
			name: "date_trunc", args: []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
			shouldErr: true,
		},
		{
			name: "date_trunc", args: []types.Type{types.T_varchar.ToType(), types.T_datetime.ToTypeWithScale(6)},
			shouldErr:  false,
			requireFid: DATE_TRUNC, requireOid: 0,
			shouldCast: false,
			requireRet: types.T_datetime.ToType(),
		},
		{
			name: "date_trunc", args: []types.Type{types.T_varchar.ToType(), types.T_timestamp.ToType()},
			shouldErr:  false,
			requireFid: DATE_TRUNC, requireOid: 2,
			shouldCast: false,
			requireRet: types.T_timestamp.ToType(),
		},
	}

	proc := testutil.NewProcess(t)
	for i, c := range cs {
		msg := fmt.Sprintf("%dth case", i)

		get, err := GetFunctionByName(proc.Ctx, c.name, c.args)
		if c.shouldErr {
			require.True(t, err != nil, msg)
		} else {
			require.NoError(t, err, msg)
			require.Equal(t, c.requireFid, get.fid, msg)
			require.Equal(t, c.requireOid, get.overloadId, msg)
			require.Equal(t, c.shouldCast, get.needCast, msg)
			if c.shouldCast {
				require.Equal(t, len(c.requireTyp), len(get.targetTypes), msg)
				for j := range c.requireTyp {
					require.Equal(t, c.requireTyp[j], get.targetTypes[j], msg)
				}
			}
			require.Equal(t, c.requireRet, get.retType, msg)
		}
	}
}

func TestGetFunctionByNameWithoutError(t *testing.T) {
	args := []types.Type{types.T_int8.ToType(), types.T_int16.ToType()}

	want, err := GetFunctionByName(context.Background(), "+", args)
	require.NoError(t, err)

	got, ok := GetFunctionByNameWithoutError("+", args)
	require.True(t, ok)
	require.Equal(t, want.fid, got.fid)
	require.Equal(t, want.overloadId, got.overloadId)
	require.Equal(t, want.retType, got.retType)
	require.Equal(t, want.needCast, got.needCast)
	require.Equal(t, want.targetTypes, got.targetTypes)

	_, ok = GetFunctionByNameWithoutError("date_trunc", []types.Type{
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	})
	require.False(t, ok)

	_, ok = GetFunctionByNameWithoutError("function_does_not_exist", nil)
	require.False(t, ok)
}

func TestMakeTimeReturnScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	integerResult, err := GetFunctionByName(proc.Ctx, "maketime", []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	})
	require.NoError(t, err)
	require.Equal(t, types.T_time.ToType(), integerResult.retType)

	fractionalResult, err := GetFunctionByName(proc.Ctx, "maketime", []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.New(types.T_decimal128, 20, 6),
	})
	require.NoError(t, err)
	require.True(t, fractionalResult.needCast)
	require.Equal(t, types.T_varchar, fractionalResult.targetTypes[2].Oid)
	require.Equal(t, int32(6), fractionalResult.targetTypes[2].Scale)
	require.Equal(t, types.T_time.ToTypeWithScale(6), fractionalResult.retType)

	defaultFloatResult, err := GetFunctionByName(proc.Ctx, "maketime", []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		{Oid: types.T_float64, Size: 8, Scale: -1},
	})
	require.NoError(t, err)
	require.Equal(t, types.T_time.ToTypeWithScale(6), defaultFloatResult.retType)
}

func TestSecToTimeReturnScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	integerResult, err := GetFunctionByName(proc.Ctx, "sec_to_time", []types.Type{
		types.T_int64.ToType(),
	})
	require.NoError(t, err)
	require.Equal(t, types.T_time.ToType(), integerResult.retType)

	decimalResult, err := GetFunctionByName(proc.Ctx, "sec_to_time", []types.Type{
		types.New(types.T_decimal128, 20, 3),
	})
	require.NoError(t, err)
	require.True(t, decimalResult.needCast)
	require.Equal(t, types.T_varchar, decimalResult.targetTypes[0].Oid)
	require.Equal(t, int32(3), decimalResult.targetTypes[0].Scale)
	require.Equal(t, types.T_time.ToTypeWithScale(3), decimalResult.retType)

	stringResult, err := GetFunctionByName(proc.Ctx, "sec_to_time", []types.Type{
		types.T_varchar.ToType(),
	})
	require.NoError(t, err)
	require.True(t, stringResult.needCast)
	require.Equal(t, int32(-1), stringResult.targetTypes[0].Scale)
	require.Equal(t, types.T_time.ToTypeWithScale(6), stringResult.retType)

	floatResult, err := GetFunctionByName(proc.Ctx, "sec_to_time", []types.Type{
		{Oid: types.T_float64, Size: 8, Scale: -1},
	})
	require.NoError(t, err)
	require.Equal(t, types.T_time.ToTypeWithScale(6), floatResult.retType)
}

func TestUnixTimestampTemporalReturnScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	integerResult, err := GetFunctionByName(proc.Ctx, "unix_timestamp", []types.Type{
		types.T_timestamp.ToType(),
	})
	require.NoError(t, err)
	require.False(t, integerResult.needCast)
	require.Equal(t, types.T_int64.ToType(), integerResult.retType)

	fractionalTimestampResult, err := GetFunctionByName(proc.Ctx, "unix_timestamp", []types.Type{
		types.T_timestamp.ToTypeWithScale(6),
	})
	require.NoError(t, err)
	require.False(t, fractionalTimestampResult.needCast)
	require.Equal(t, types.New(types.T_decimal128, 38, 6), fractionalTimestampResult.retType)

	fractionalDatetimeResult, err := GetFunctionByName(proc.Ctx, "unix_timestamp", []types.Type{
		types.T_datetime.ToTypeWithScale(6),
	})
	require.NoError(t, err)
	require.True(t, fractionalDatetimeResult.needCast)
	require.Equal(t, []types.Type{types.T_timestamp.ToTypeWithScale(6)}, fractionalDatetimeResult.targetTypes)
	require.Equal(t, types.New(types.T_decimal128, 38, 6), fractionalDatetimeResult.retType)
}

func TestMakeTimeDecimalHourMinuteUseExactOverloads(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimalType := types.New(types.T_decimal128, 30, 20)
	decimal256Type := types.New(types.T_decimal256, 65, 30)

	tests := []struct {
		inputs []types.Type
		args   []types.T
	}{
		{[]types.Type{decimalType, types.T_int64.ToType(), types.T_int64.ToType()}, []types.T{types.T_decimal128, types.T_float64, types.T_float64}},
		{[]types.Type{decimalType, types.T_varchar.ToType(), types.T_int64.ToType()}, []types.T{types.T_decimal128, types.T_varchar, types.T_float64}},
		{[]types.Type{decimalType, decimalType, types.T_int64.ToType()}, []types.T{types.T_decimal128, types.T_decimal128, types.T_float64}},
		{[]types.Type{decimalType, types.T_int64.ToType(), types.T_varchar.ToType()}, []types.T{types.T_decimal128, types.T_float64, types.T_varchar}},
		{[]types.Type{decimalType, types.T_varchar.ToType(), types.T_varchar.ToType()}, []types.T{types.T_decimal128, types.T_varchar, types.T_varchar}},
		{[]types.Type{types.T_int64.ToType(), decimalType, types.T_int64.ToType()}, []types.T{types.T_float64, types.T_decimal128, types.T_float64}},
		{[]types.Type{types.T_varchar.ToType(), decimalType, types.T_int64.ToType()}, []types.T{types.T_varchar, types.T_decimal128, types.T_float64}},
		{[]types.Type{types.T_int64.ToType(), decimalType, types.T_varchar.ToType()}, []types.T{types.T_float64, types.T_decimal128, types.T_varchar}},
		{[]types.Type{types.T_varchar.ToType(), decimalType, types.T_varchar.ToType()}, []types.T{types.T_varchar, types.T_decimal128, types.T_varchar}},
		{[]types.Type{decimalType, decimalType, types.New(types.T_decimal128, 20, 6)}, []types.T{types.T_decimal128, types.T_decimal128, types.T_varchar}},
		{[]types.Type{decimal256Type, types.T_int64.ToType(), types.T_int64.ToType()}, []types.T{types.T_decimal256, types.T_float64, types.T_float64}},
		{[]types.Type{types.T_int64.ToType(), decimal256Type, types.T_int64.ToType()}, []types.T{types.T_float64, types.T_decimal256, types.T_float64}},
		{[]types.Type{decimal256Type, decimalType, types.T_varchar.ToType()}, []types.T{types.T_decimal256, types.T_decimal128, types.T_varchar}},
		{[]types.Type{decimalType, decimal256Type, types.T_varchar.ToType()}, []types.T{types.T_decimal128, types.T_decimal256, types.T_varchar}},
		{[]types.Type{decimal256Type, decimal256Type, types.T_varchar.ToType()}, []types.T{types.T_decimal256, types.T_decimal256, types.T_varchar}},
	}

	for _, test := range tests {
		result, err := GetFunctionByName(proc.Ctx, "maketime", test.inputs)
		require.NoError(t, err)
		require.True(t, result.needCast)
		selected, err := GetFunctionById(proc.Ctx, result.GetEncodedOverloadID())
		require.NoError(t, err)
		require.Equal(t, test.args, selected.args)
	}
}

func TestMakeTimeDecimal256OverloadMatrix(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal128Type := types.New(types.T_decimal128, 30, 20)
	decimal256Type := types.New(types.T_decimal256, 65, 30)
	type typeChoice struct {
		input  types.Type
		target types.T
	}
	hourMinuteChoices := []typeChoice{
		{types.T_int64.ToType(), types.T_float64},
		{types.T_varchar.ToType(), types.T_varchar},
		{decimal128Type, types.T_decimal128},
		{decimal256Type, types.T_decimal256},
	}
	secondChoices := []typeChoice{
		{types.T_int64.ToType(), types.T_float64},
		{types.T_varchar.ToType(), types.T_varchar},
	}

	for _, hour := range hourMinuteChoices {
		for _, minute := range hourMinuteChoices {
			if hour.target != types.T_decimal256 && minute.target != types.T_decimal256 {
				continue
			}
			for _, second := range secondChoices {
				result, err := GetFunctionByName(proc.Ctx, "maketime", []types.Type{hour.input, minute.input, second.input})
				require.NoError(t, err)
				selected, err := GetFunctionById(proc.Ctx, result.GetEncodedOverloadID())
				require.NoError(t, err)
				require.Equal(t, []types.T{hour.target, minute.target, second.target}, selected.args)
			}
		}
	}
}

func TestMakeTimeStringSecondUsesExactOverload(t *testing.T) {
	proc := testutil.NewProcess(t)

	result, err := GetFunctionByName(proc.Ctx, "maketime", []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	})
	require.NoError(t, err)
	require.True(t, result.needCast)
	require.Len(t, result.targetTypes, 3)
	require.Equal(t, types.T_float64, result.targetTypes[0].Oid)
	require.Equal(t, types.T_float64, result.targetTypes[1].Oid)
	require.Equal(t, types.T_varchar, result.targetTypes[2].Oid)
	require.Equal(t, int32(-1), result.targetTypes[2].Scale)
	require.Equal(t, types.T_time.ToTypeWithScale(6), result.retType)
}

func TestSerialFunctionsReturnBinaryVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}

	for _, name := range []string{SerialFunctionName, SerialFullFunctionName} {
		t.Run(name, func(t *testing.T) {
			result, err := GetFunctionByName(proc.Ctx, name, inputs)
			require.NoError(t, err)
			require.Equal(t, types.T_varchar, result.GetReturnType().Oid)
			require.Equal(t, types.CharsetBinary, result.GetReturnType().Charset)
		})
	}
}

func TestConcatFunctionsPreserveStringCollation(t *testing.T) {
	proc := testutil.NewProcess(t)
	general := types.T_varchar.ToType()
	legacy := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetLegacy)
	utf8mb4Bin := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetUTF8MB4Bin)
	opaqueBinary := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetBinary)

	testCases := []struct {
		name        string
		function    string
		inputs      []types.Type
		wantOID     types.T
		wantCharset uint8
	}{
		{
			name:        "concat keeps legacy byte ordering",
			function:    "concat",
			inputs:      []types.Type{general, legacy},
			wantOID:     types.T_text,
			wantCharset: types.CharsetLegacy,
		},
		{
			name:        "concat keeps utf8mb4 bin",
			function:    "concat",
			inputs:      []types.Type{general, utf8mb4Bin},
			wantOID:     types.T_text,
			wantCharset: types.CharsetUTF8MB4Bin,
		},
		{
			name:        "concat ws keeps utf8mb4 bin",
			function:    "concat_ws",
			inputs:      []types.Type{general, utf8mb4Bin, utf8mb4Bin},
			wantOID:     types.T_text,
			wantCharset: types.CharsetUTF8MB4Bin,
		},
		{
			name:        "opaque binary dominates utf8mb4 bin",
			function:    "concat",
			inputs:      []types.Type{utf8mb4Bin, opaqueBinary},
			wantOID:     types.T_varbinary,
			wantCharset: types.CharsetBinary,
		},
		{
			name:        "binary string keeps blob result",
			function:    "concat",
			inputs:      []types.Type{general, types.T_varbinary.ToType()},
			wantOID:     types.T_blob,
			wantCharset: types.T_blob.ToType().Charset,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := GetFunctionByName(proc.Ctx, testCase.function, testCase.inputs)
			require.NoError(t, err)
			require.Equal(t, testCase.wantOID, result.GetReturnType().Oid)
			require.Equal(t, testCase.wantCharset, result.GetReturnType().Charset)
		})
	}
}

func TestConditionalStringFunctionsPreserveCommonCollation(t *testing.T) {
	proc := testutil.NewProcess(t)
	condition := types.T_bool.ToType()
	binNarrow := types.NewWithCharset(types.T_varchar, 10, 0, types.CharsetUTF8MB4Bin)
	binWide := types.NewWithCharset(types.T_varchar, 80, 0, types.CharsetUTF8MB4Bin)
	generalWide := types.NewWithCharset(types.T_varchar, 80, 0, types.CharsetUTF8)

	tests := []struct {
		name        string
		function    string
		inputs      []types.Type
		wantCharset uint8
		wantWidth   int32
	}{
		{
			name:        "case keeps matching utf8mb4 bin branches",
			function:    "case",
			inputs:      []types.Type{condition, binNarrow, binNarrow},
			wantCharset: types.CharsetUTF8MB4Bin,
			wantWidth:   10,
		},
		{
			name:        "case merges branch widths",
			function:    "case",
			inputs:      []types.Type{condition, binNarrow, binWide},
			wantCharset: types.CharsetUTF8MB4Bin,
			wantWidth:   80,
		},
		{
			name:        "if merges branch widths",
			function:    "if",
			inputs:      []types.Type{condition, binNarrow, binWide},
			wantCharset: types.CharsetUTF8MB4Bin,
			wantWidth:   80,
		},
		{
			name:        "coalesce merges argument widths",
			function:    "coalesce",
			inputs:      []types.Type{binNarrow, binWide},
			wantCharset: types.CharsetUTF8MB4Bin,
			wantWidth:   80,
		},
		{
			name:        "binary collation dominates general ci",
			function:    "coalesce",
			inputs:      []types.Type{generalWide, binNarrow},
			wantCharset: types.CharsetUTF8MB4Bin,
			wantWidth:   80,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := GetFunctionByName(proc.Ctx, test.function, test.inputs)
			require.NoError(t, err)
			require.Equal(t, types.T_varchar, result.GetReturnType().Oid)
			require.Equal(t, test.wantCharset, result.GetReturnType().Charset)
			require.Equal(t, test.wantWidth, result.GetReturnType().Width)

			castTypes, shouldCast := result.ShouldDoImplicitTypeCast()
			if shouldCast {
				for i, typ := range castTypes {
					if test.inputs[i].Oid == types.T_bool {
						continue
					}
					require.Equal(t, test.wantCharset, typ.Charset)
					require.Equal(t, test.wantWidth, typ.Width)
				}
			}
		})
	}
}

func TestMinConditionalStringsUsesPropagatedBinaryCollation(t *testing.T) {
	proc := testutil.NewProcess(t)
	narrowType := types.NewWithCharset(types.T_varchar, 10, 0, types.CharsetUTF8MB4Bin)
	wideType := types.NewWithCharset(types.T_varchar, 80, 0, types.CharsetUTF8MB4Bin)
	narrow := vector.NewVec(narrowType)
	defer narrow.Free(proc.Mp())
	wide := vector.NewVec(wideType)
	defer wide.Free(proc.Mp())
	for _, value := range []string{"a", "B"} {
		require.NoError(t, vector.AppendBytes(narrow, []byte(value), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(wide, []byte(value), false, proc.Mp()))
	}
	condition, err := vector.NewConstFixed(types.T_bool.ToType(), true, 2, proc.Mp())
	require.NoError(t, err)
	defer condition.Free(proc.Mp())

	assertBinaryMin := func(t *testing.T, values *vector.Vector) {
		require.Equal(t, types.CharsetUTF8MB4Bin, values.GetType().Charset)
		require.Equal(t, int32(80), values.GetType().Width)
		minExec, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfMin, false, *values.GetType())
		require.NoError(t, err)
		defer minExec.Free()
		require.NoError(t, minExec.GroupGrow(1))
		require.NoError(t, minExec.BulkFill(0, []*vector.Vector{values}))
		results, err := minExec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		defer results[0].Free(proc.Mp())
		require.Equal(t, "B", string(results[0].GetBytesAt(0)))
	}

	tests := []struct {
		name   string
		inputs []*vector.Vector
	}{
		{name: "case", inputs: []*vector.Vector{condition, narrow, wide}},
		{name: "if", inputs: []*vector.Vector{condition, narrow, wide}},
		{name: "coalesce", inputs: []*vector.Vector{narrow, wide}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputTypes := make([]types.Type, len(test.inputs))
			for i := range test.inputs {
				inputTypes[i] = *test.inputs[i].GetType()
			}
			resolved, err := GetFunctionByName(proc.Ctx, test.name, inputTypes)
			require.NoError(t, err)
			values, err := RunFunctionDirectly(
				proc, resolved.GetEncodedOverloadID(), test.inputs, 2)
			require.NoError(t, err)
			defer values.Free(proc.Mp())
			assertBinaryMin(t, values)
		})
	}
}

func TestDerivedStringFunctionsPreserveSourceCollation(t *testing.T) {
	proc := testutil.NewProcess(t)
	general := types.T_varchar.ToType()
	binaryCollation := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetUTF8MB4Bin)
	integer := types.T_int64.ToType()

	tests := []struct {
		name       string
		inputs     []types.Type
		wantSource int
	}{
		{name: "left", inputs: []types.Type{binaryCollation, integer}, wantSource: 0},
		{name: "right", inputs: []types.Type{binaryCollation, integer}, wantSource: 0},
		{name: "substring", inputs: []types.Type{binaryCollation, integer}, wantSource: 0},
		{name: "replace", inputs: []types.Type{binaryCollation, general, general}, wantSource: 0},
		{name: "ltrim", inputs: []types.Type{binaryCollation}, wantSource: 0},
		{name: "rtrim", inputs: []types.Type{binaryCollation}, wantSource: 0},
		{name: "trim", inputs: []types.Type{general, general, binaryCollation}, wantSource: 2},
		{name: "elt", inputs: []types.Type{integer, binaryCollation, general}, wantSource: 1},
		{name: "make_set", inputs: []types.Type{integer, binaryCollation, general}, wantSource: 1},
		{name: "export_set", inputs: []types.Type{integer, binaryCollation, general}, wantSource: 1},
		{name: "quote", inputs: []types.Type{binaryCollation}, wantSource: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := GetFunctionByName(proc.Ctx, test.name, test.inputs)
			require.NoError(t, err)
			require.Equal(t, test.inputs[test.wantSource].Charset, result.GetReturnType().Charset)
		})
	}
}

func TestMinConvertUsingAndSubstringUseDerivedCollation(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.NewWithCharset(types.T_varchar, 10, 0, types.CharsetUTF8MB4Bin)
	input := vector.NewVec(inputType)
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte("a"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input, []byte("B"), false, proc.Mp()))

	assertMin := func(t *testing.T, values *vector.Vector, expected string) {
		minExec, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfMin, false, *values.GetType())
		require.NoError(t, err)
		defer minExec.Free()
		require.NoError(t, minExec.GroupGrow(1))
		require.NoError(t, minExec.BulkFill(0, []*vector.Vector{values}))
		results, err := minExec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		defer results[0].Free(proc.Mp())
		require.Equal(t, expected, string(results[0].GetBytesAt(0)))
	}

	for _, test := range []struct {
		name        string
		charset     uint8
		expectedMin string
	}{
		{name: "binary", charset: types.CharsetBinary, expectedMin: "B"},
		{name: "utf8mb4", charset: types.CharsetUTF8, expectedMin: "a"},
	} {
		t.Run("convert_"+test.name, func(t *testing.T) {
			charsetType := types.NewWithCharset(types.T_varchar, int32(len(test.name)), 0, test.charset)
			charset, err := vector.NewConstBytes(charsetType, []byte(test.name), 2, proc.Mp())
			require.NoError(t, err)
			defer charset.Free(proc.Mp())
			convert, err := GetFunctionByName(proc.Ctx, "convert", []types.Type{inputType, charsetType})
			require.NoError(t, err)
			converted, err := RunFunctionDirectly(
				proc, convert.GetEncodedOverloadID(), []*vector.Vector{input, charset}, 2)
			require.NoError(t, err)
			defer converted.Free(proc.Mp())
			require.Equal(t, test.charset, converted.GetType().Charset)
			assertMin(t, converted, test.expectedMin)
		})
	}

	t.Run("substring_utf8mb4_bin", func(t *testing.T) {
		position, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 2, proc.Mp())
		require.NoError(t, err)
		defer position.Free(proc.Mp())
		substring, err := GetFunctionByName(proc.Ctx, "substring", []types.Type{inputType, types.T_int64.ToType()})
		require.NoError(t, err)
		values, err := RunFunctionDirectly(
			proc, substring.GetEncodedOverloadID(), []*vector.Vector{input, position}, 2)
		require.NoError(t, err)
		defer values.Free(proc.Mp())
		require.Equal(t, types.CharsetUTF8MB4Bin, values.GetType().Charset)
		assertMin(t, values, "B")
	})
}

func TestMinConcatUsesPropagatedBinaryCollation(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.NewWithCharset(types.T_varchar, 10, 0, types.CharsetUTF8MB4Bin)
	input := vector.NewVec(inputType)
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte("a"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input, []byte("B"), false, proc.Mp()))

	concat, err := GetFunctionByName(proc.Ctx, "concat", []types.Type{inputType, inputType})
	require.NoError(t, err)
	concatenated, err := RunFunctionDirectly(
		proc, concat.GetEncodedOverloadID(), []*vector.Vector{input, input}, 2)
	require.NoError(t, err)
	defer concatenated.Free(proc.Mp())
	require.Equal(t, types.CharsetUTF8MB4Bin, concatenated.GetType().Charset)

	minExec, err := aggexec.MakeAgg(
		proc.Mp(), aggexec.AggIdOfMin, false, *concatenated.GetType())
	require.NoError(t, err)
	defer minExec.Free()
	require.NoError(t, minExec.GroupGrow(1))
	require.NoError(t, minExec.BulkFill(0, []*vector.Vector{concatenated}))

	results, err := minExec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	defer results[0].Free(proc.Mp())
	require.Equal(t, "BB", string(results[0].GetBytesAt(0)))
}

func TestMakeTimeStringArgumentTargets(t *testing.T) {
	proc := testutil.NewProcess(t)
	defaultFloat := types.T_float64.ToType()
	defaultFloat.Scale = -1
	scaledFloat := types.T_float64.ToTypeWithScale(1)

	tests := []struct {
		name         string
		inputs       []types.Type
		overloadArgs []types.T
		needCast     bool
		targets      []types.Type
		returnType   types.Type
	}{
		{
			name: "varchar hour and minute with double second",
			inputs: []types.Type{
				types.T_varchar.ToType(), types.T_varchar.ToType(), defaultFloat,
			},
			overloadArgs: []types.T{types.T_varchar, types.T_varchar, types.T_float64},
			returnType:   types.T_time.ToTypeWithScale(6),
		},
		{
			name: "all varchar",
			inputs: []types.Type{
				types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
			},
			overloadArgs: []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
			needCast:     true,
			targets: []types.Type{
				types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToTypeWithScale(-1),
			},
			returnType: types.T_time.ToTypeWithScale(6),
		},
		{
			name: "only hour is varchar",
			inputs: []types.Type{
				types.T_varchar.ToType(), scaledFloat, scaledFloat,
			},
			overloadArgs: []types.T{types.T_varchar, types.T_float64, types.T_float64},
			returnType:   types.T_time.ToTypeWithScale(1),
		},
		{
			name: "only minute is varchar",
			inputs: []types.Type{
				scaledFloat, types.T_varchar.ToType(), scaledFloat,
			},
			overloadArgs: []types.T{types.T_float64, types.T_varchar, types.T_float64},
			returnType:   types.T_time.ToTypeWithScale(1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := GetFunctionByName(proc.Ctx, "maketime", test.inputs)
			require.NoError(t, err)
			require.Equal(t, test.needCast, result.needCast)
			require.Equal(t, test.targets, result.targetTypes)
			require.Equal(t, test.returnType, result.retType)

			selected, err := GetFunctionById(proc.Ctx, result.GetEncodedOverloadID())
			require.NoError(t, err)
			require.Equal(t, test.overloadArgs, selected.args)
		})
	}
}

func TestMakeTimeBinaryArgumentsUseNumericOverloads(t *testing.T) {
	proc := testutil.NewProcess(t)
	binaryTypes := []types.T{types.T_binary, types.T_varbinary, types.T_blob}

	for _, binaryType := range binaryTypes {
		for position := range 3 {
			inputs := []types.Type{
				types.T_int64.ToType(),
				types.T_int64.ToType(),
				types.T_int64.ToType(),
			}
			inputs[position] = binaryType.ToType()

			result, err := GetFunctionByName(proc.Ctx, "maketime", inputs)
			require.NoError(t, err)
			require.True(t, result.needCast)
			require.Equal(t, types.T_int64, result.targetTypes[position].Oid)
		}
	}
}

func TestGetFunctionByNameAESDecryptReturnsBlob(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name string
		args []types.Type
	}{
		{
			name: "blob input",
			args: []types.Type{types.T_blob.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "varchar input",
			args: []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "char input",
			args: []types.Type{types.T_char.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "text input",
			args: []types.Type{types.T_text.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "blob input with iv",
			args: []types.Type{types.T_blob.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "varchar input with iv",
			args: []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "char input with iv",
			args: []types.Type{types.T_char.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "text input with iv",
			args: []types.Type{types.T_text.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			get, err := GetFunctionByName(proc.Ctx, "aes_decrypt", tc.args)
			require.NoError(t, err)
			require.Equal(t, int32(AES_DECRYPT), get.fid)
			require.Equal(t, types.T_blob.ToType(), get.retType)
		})
	}
}

func TestGetFunctionIsWinfunByName(t *testing.T) {
	assert.Equal(t, true, GetFunctionIsWinFunByName("rank"))
	assert.Equal(t, false, GetFunctionIsWinFunByName("floor"))
}

func TestGetFunctionIgnoresWindowFrameByName(t *testing.T) {
	assert.True(t, GetFunctionIgnoresWindowFrameByName("lag"))
	assert.True(t, GetFunctionIgnoresWindowFrameByName("lead"))
	assert.False(t, GetFunctionIgnoresWindowFrameByName("first_value"))
	assert.False(t, GetFunctionIgnoresWindowFrameByName("last_value"))
	assert.False(t, GetFunctionIgnoresWindowFrameByName("nth_value"))
	assert.False(t, GetFunctionIgnoresWindowFrameByName("not_a_function"))
}

func TestGetFunctionIsVolatileOrRealTimeRelatedByName(t *testing.T) {
	assert.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("rand"))
	assert.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("uuid"))
	assert.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("now"))
	assert.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("current_timestamp"))
	assert.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("current_role_id"))
	assert.False(t, GetFunctionIsVolatileOrRealTimeRelatedByName("abs"))
	assert.False(t, GetFunctionIsVolatileOrRealTimeRelatedByName("unknown_function"))
}

func TestProducesNoNullUsesFunctionContract(t *testing.T) {
	require.True(t, ProducesNoNull(EncodeOverloadID(ISNULL, 0)))
	for _, fid := range []int32{COUNT, STARCOUNT, BIT_AND, BIT_OR, BIT_XOR} {
		require.True(t, ProducesNoNull(EncodeOverloadID(fid, 0)),
			"aggregate %d has a non-NULL neutral result", fid)
		require.True(t, HasExecutableCTASTypeDefault(EncodeOverloadID(fid, 0)),
			"aggregate %d can use its SQL type default after CTAS", fid)
	}
	for _, fid := range []int32{HLL_ADD_AGG, HLL_MERGE_AGG} {
		require.True(t, ProducesNoNull(EncodeOverloadID(fid, 0)),
			"aggregate %d always produces an encoded HLL sketch", fid)
		require.False(t, HasExecutableCTASTypeDefault(EncodeOverloadID(fid, 0)),
			"aggregate %d cannot use an empty byte string as an HLL sketch", fid)
	}
	for _, fid := range []int32{JSON_EXTRACT, JSON_EXTRACT_STRING, JSON_EXTRACT_FLOAT64} {
		require.False(t, ProducesNoNull(EncodeOverloadID(fid, 0)),
			"STRICT only describes NULL-input propagation; JSON extractors can still return SQL NULL")
	}
	require.False(t, ProducesNoNull(-1))
	require.False(t, HasExecutableCTASTypeDefault(-1))
}

func TestDeduceNotNullableKeepsNullSynthesizingFunctionsNullable(t *testing.T) {
	notNull := &plan.Expr{Typ: plan.Type{NotNullable: true}}

	for _, tt := range []struct {
		name     string
		fid      int32
		argCount int
	}{
		{name: "division by zero", fid: DIV, argCount: 2},
		{name: "integer division by zero", fid: INTEGER_DIV, argCount: 2},
		{name: "modulo by zero", fid: MOD, argCount: 2},
		{name: "missing JSON path", fid: JSON_EXTRACT, argCount: 2},
		{name: "JSON string extractor", fid: JSON_EXTRACT_STRING, argCount: 2},
		{name: "JSON float64 extractor", fid: JSON_EXTRACT_FLOAT64, argCount: 2},
		{name: "regexp without a match", fid: REGEXP_SUBSTR, argCount: 2},
		{name: "invalid IPv6 address", fid: INET6_ATON, argCount: 1},
		{name: "out of range elt index", fid: ELT, argCount: 3},
		{name: "invalid hex input", fid: UNHEX, argCount: 1},
		{name: "invalid day of year", fid: MAKEDATE, argCount: 2},
		{name: "invalid interval string", fid: TO_INTERVAL, argCount: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := make([]*plan.Expr, tt.argCount)
			for i := range args {
				args[i] = notNull
			}
			require.False(t, DeduceNotNullable(EncodeOverloadID(tt.fid, 0), args))
		})
	}
}

func TestDeduceNotNullablePreservesArgumentDependentContracts(t *testing.T) {
	notNull := &plan.Expr{Typ: plan.Type{NotNullable: true}}
	nullable := &plan.Expr{Typ: plan.Type{NotNullable: false}}

	for _, tt := range []struct {
		name     string
		fid      int32
		argCount int
	}{
		{name: "equality", fid: EQUAL, argCount: 2},
		{name: "addition", fid: PLUS, argCount: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := make([]*plan.Expr, tt.argCount)
			for i := range args {
				args[i] = notNull
			}
			require.True(t, DeduceNotNullable(EncodeOverloadID(tt.fid, 0), args))

			args[0] = nullable
			require.False(t, DeduceNotNullable(EncodeOverloadID(tt.fid, 0), args))
		})
	}
}

func TestDeduceNotNullableForJSONBooleanComparison(t *testing.T) {
	jsonNotNull := &plan.Expr{Typ: plan.Type{Id: int32(types.T_json), NotNullable: true}}
	booleanNotNull := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}}
	for _, fid := range []int32{EQUAL, NOT_EQUAL} {
		require.False(t, DeduceNotNullable(
			EncodeOverloadID(fid, 0), []*plan.Expr{jsonNotNull, booleanNotNull}))
		require.False(t, DeduceNotNullable(
			EncodeOverloadID(fid, 0), []*plan.Expr{booleanNotNull, jsonNotNull}))
	}
	require.True(t, DeduceNotNullable(
		EncodeOverloadID(NULL_SAFE_EQUAL, 0), []*plan.Expr{jsonNotNull, booleanNotNull}))
}

func TestDeduceNotNullablePreservesExplicitContracts(t *testing.T) {
	notNull := &plan.Expr{Typ: plan.Type{NotNullable: true}}
	nullable := &plan.Expr{Typ: plan.Type{NotNullable: false}}

	require.True(t, DeduceNotNullable(EncodeOverloadID(CASE, 0), []*plan.Expr{notNull, notNull, notNull}))
	require.True(t, DeduceNotNullable(EncodeOverloadID(COALESCE, 0), []*plan.Expr{nullable, notNull}))
	require.True(t, DeduceNotNullable(EncodeOverloadID(ISNULL, 0), []*plan.Expr{nullable}))
}

func TestDeduceNotNullableForWindowFunctions(t *testing.T) {
	notNull := &plan.Expr{Typ: plan.Type{NotNullable: true}}
	nullable := &plan.Expr{Typ: plan.Type{NotNullable: false}}

	for _, tt := range []struct {
		name string
		fid  int32
		args []*plan.Expr
		want bool
	}{
		{name: "lag without default", fid: LAG, args: []*plan.Expr{notNull}},
		{name: "lag with offset only", fid: LAG, args: []*plan.Expr{notNull, notNull}},
		{name: "lag with non-null default", fid: LAG, args: []*plan.Expr{notNull, notNull, notNull}, want: true},
		{name: "lag with nullable default", fid: LAG, args: []*plan.Expr{notNull, notNull, nullable}},
		{name: "lead without default", fid: LEAD, args: []*plan.Expr{notNull}},
		{name: "lead with non-null default", fid: LEAD, args: []*plan.Expr{notNull, notNull, notNull}, want: true},
		{name: "first value can see empty frame", fid: FIRST_VALUE, args: []*plan.Expr{notNull}},
		{name: "last value can see empty frame", fid: LAST_VALUE, args: []*plan.Expr{notNull}},
		{name: "nth value can miss requested row", fid: NTH_VALUE, args: []*plan.Expr{notNull, notNull}},
		{name: "row number is non-null", fid: ROW_NUMBER, want: true},
		{name: "rank is non-null", fid: RANK, want: true},
		{name: "dense rank is non-null", fid: DENSE_RANK, want: true},
		{name: "percent rank is non-null", fid: PERCENT_RANK, want: true},
		{name: "ntile with non-null bucket count", fid: NTILE, args: []*plan.Expr{notNull}, want: true},
		{name: "ntile with nullable bucket count", fid: NTILE, args: []*plan.Expr{nullable}},
		{name: "cume dist is non-null", fid: CUME_DIST, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, DeduceNotNullable(EncodeOverloadID(tt.fid, 0), tt.args))
		})
	}
}

func TestUserLevelLockBuiltinRegistration(t *testing.T) {
	cases := []struct {
		name string
		id   int
		args []types.T
		ret  types.Type
	}{
		{name: "get_lock", id: GET_LOCK, args: []types.T{types.T_varchar, types.T_float64}, ret: types.T_int64.ToType()},
		{name: "release_lock", id: RELEASE_LOCK, args: []types.T{types.T_varchar}, ret: types.T_int64.ToType()},
		{name: "is_free_lock", id: IS_FREE_LOCK, args: []types.T{types.T_varchar}, ret: types.T_int64.ToType()},
		{name: "is_used_lock", id: IS_USED_LOCK, args: []types.T{types.T_varchar}, ret: types.T_uint64.ToType()},
		{name: "release_all_locks", id: RELEASE_ALL_LOCKS, args: []types.T{}, ret: types.T_int64.ToType()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fn *FuncNew
			for i := range supportedControlBuiltIns {
				if supportedControlBuiltIns[i].functionId == tc.id {
					fn = &supportedControlBuiltIns[i]
					break
				}
			}
			require.NotNil(t, fn)
			require.Equal(t, plan.Function_STRICT, fn.class)
			require.Equal(t, STANDARD_FUNCTION, fn.layout)
			require.Len(t, fn.Overloads, 1)

			overload := fn.Overloads[0]
			require.Equal(t, tc.args, overload.args)
			require.True(t, overload.volatile)
			require.True(t, overload.realTimeRelated)
			require.Equal(t, tc.ret, overload.retType(nil))
			require.NotNil(t, overload.newOp())
		})
	}
}

func TestRunPositionCharFunctionDirectly(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []*vector.Vector{
		testutil.NewVector(2, types.T_char.ToType(), proc.Mp(), false, []string{"y", "a"}),
		testutil.NewVector(2, types.T_char.ToType(), proc.Mp(), false, []string{"xyz", "bbb"}),
	}
	startMp := proc.Mp().CurrNB()

	v, err := RunFunctionDirectly(proc, EncodeOverloadID(POSITION, 1), inputs, 2)
	require.NoError(t, err)
	require.Equal(t, types.T_int64, v.GetType().Oid)
	require.Equal(t, 2, v.Length())
	require.Equal(t, []int64{2, 0}, vector.MustFixedColNoTypeCheck[int64](v))

	v.Free(proc.Mp())
	proc.Free()
	require.Equal(t, startMp, proc.Mp().CurrNB())
}

func TestRunFunctionDirectly(t *testing.T) {
	// fold case.
	{
		proc := testutil.NewProcess(t)
		v0, err1 := vector.NewConstFixed(types.T_bool.ToType(), true, 10, proc.Mp())
		require.NoError(t, err1)
		v1, err2 := vector.NewConstFixed(types.T_bool.ToType(), true, 10, proc.Mp())
		require.NoError(t, err2)
		inputs := []*vector.Vector{v0, v1}
		startMp := proc.Mp().CurrNB()

		v, err := RunFunctionDirectly(proc, AndFunctionEncodedID, inputs, 10)
		require.NoError(t, err)

		require.Equal(t, 10, v.Length())
		wrapper := vector.GenerateFunctionFixedTypeParameter[bool](v)
		for i := 0; i < 10; i++ {
			value, null := wrapper.GetValue(uint64(i))
			require.Equal(t, false, null)
			require.Equal(t, true, value)
		}

		v.Free(proc.Mp())
		proc.Free()
		require.Equal(t, startMp, proc.Mp().CurrNB())
	}

	// non-fold case.
	{
		proc := testutil.NewProcess(t)
		inputs := []*vector.Vector{
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, true}),
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, true}),
		}
		startMp := proc.Mp().CurrNB()

		v, err := RunFunctionDirectly(proc, AndFunctionEncodedID, inputs, 2)
		require.NoError(t, err)

		require.Equal(t, 2, v.Length())
		wrapper := vector.GenerateFunctionFixedTypeParameter[bool](v)
		for i := 0; i < 2; i++ {
			value, null := wrapper.GetValue(uint64(i))
			require.Equal(t, false, null)
			require.Equal(t, true, value)
		}

		v.Free(proc.Mp())
		require.Equal(t, startMp, proc.Mp().CurrNB())
	}
}

func TestCastNanoToTimestamp(t *testing.T) {
	inputs := []string{
		"2021-04-13 08:00:00.000000099",
		"2021-04-13 08:00:00.000000101",
		"2021-04-13 08:00:00",
	}
	outputs := make([]int64, len(inputs))
	for i, in := range inputs {
		outputs[i] = convertStringToTimeUtcNano(in)
	}

	testCases := initCastNanoToTimestampTestCase(inputs, outputs)

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, CastNanoToTimestamp)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("err info is '%s'", info))
	}

}

func initCastNanoToTimestampTestCase(inputs []string, outputs []int64) []tcTemp {
	res := make([]tcTemp, len(inputs))
	for i := range inputs {
		res[i] = tcTemp{
			info: fmt.Sprintf("case %d", i),
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{outputs[i]},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{inputs[i]},
				[]bool{false}),
		}
	}
	return res
}

func convertStringToTimeUtcNano(str string) int64 {
	ts, _ := time.Parse("2006-01-02 15:04:05.999999999", str)
	return ts.UTC().UnixNano()
}
