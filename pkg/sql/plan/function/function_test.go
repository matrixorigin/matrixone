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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
