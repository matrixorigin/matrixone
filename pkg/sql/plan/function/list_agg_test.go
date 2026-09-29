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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func TestOrderedCollectionCompatibilityAliases(t *testing.T) {
	listAgg, err := GetFunctionByName(
		t.Context(), "listagg", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	listAggID, _ := DecodeOverloadID(listAgg.GetEncodedOverloadID())
	require.Equal(t, int32(GROUP_CONCAT), listAggID)
	require.Equal(t, types.T_text, listAgg.GetReturnType().Oid)

	arrayAgg, err := GetFunctionByName(
		t.Context(), "array_agg", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	arrayAggID, _ := DecodeOverloadID(arrayAgg.GetEncodedOverloadID())
	require.Equal(t, int32(JSON_ARRAYAGG), arrayAggID)
	require.Equal(t, types.T_json, arrayAgg.GetReturnType().Oid)
}

func TestJSONObjectAggNumericKeyResolution(t *testing.T) {
	ctx := context.Background()
	valueType := types.T_varchar.ToType()

	for _, keyType := range []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint8.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.New(types.T_decimal64, 18, 4),
		types.New(types.T_decimal128, 38, 6),
		types.New(types.T_decimal256, 76, 8),
	} {
		t.Run(keyType.Oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{keyType, valueType})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, []types.Type{types.T_varchar.ToType(), valueType}, targets)
		})
	}
}

func TestMySQLNumericAggTypeCheck(t *testing.T) {
	for _, name := range []string{"var_pop", "var_samp", "stddev_pop", "stddev_samp"} {
		t.Run(name, func(t *testing.T) {
			for _, input := range []struct {
				typ  types.Type
				want types.Type
			}{
				{types.T_varchar.ToType(), types.T_float64.ToType()},
				{types.T_date.ToType(), types.New(types.T_decimal128, 38, 0)},
			} {
				got, err := GetFunctionByName(context.Background(), name, []types.Type{input.typ})
				require.NoError(t, err)
				castTypes, shouldCast := got.ShouldDoImplicitTypeCast()
				require.True(t, shouldCast)
				require.Equal(t, []types.Type{input.want}, castTypes)
			}
		})
	}
}

func TestMySQLNumericAggSupportsDecimal256(t *testing.T) {
	input := types.New(types.T_decimal256, 65, 30)
	for _, name := range []string{"var_pop", "var_samp", "stddev_pop", "stddev_samp"} {
		t.Run(name, func(t *testing.T) {
			got, err := GetFunctionByName(context.Background(), name, []types.Type{input})
			require.NoError(t, err)
			require.Equal(t, types.T_float64.ToType(), got.GetReturnType())
			_, shouldCast := got.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
		})
	}
}

func TestBitSumAvgUsesExistingUnsignedDomain(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		for _, width := range []int32{1, 8, 64} {
			got, err := GetFunctionByName(context.Background(), name, []types.Type{types.New(types.T_bit, width, 0)})
			require.NoError(t, err)
			casts, cast := got.ShouldDoImplicitTypeCast()
			require.True(t, cast)
			require.Equal(t, []types.Type{types.T_uint64.ToType()}, casts)
			unsigned, err := GetFunctionByName(context.Background(), name, []types.Type{types.T_uint64.ToType()})
			require.NoError(t, err)
			require.Equal(t, unsigned.GetReturnType(), got.GetReturnType())
		}
	}
}

func TestEnumSumAvgUsesOrdinalDomain(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		got, err := GetFunctionByName(context.Background(), name, []types.Type{types.T_enum.ToType()})
		require.NoError(t, err)
		casts, cast := got.ShouldDoImplicitTypeCast()
		require.True(t, cast)
		require.Equal(t, []types.Type{types.T_uint16.ToType()}, casts)
		ordinal, err := GetFunctionByName(context.Background(), name, casts)
		require.NoError(t, err)
		require.Equal(t, ordinal.GetReturnType(), got.GetReturnType())
		for _, inputs := range [][]types.Type{nil, {types.T_enum.ToType(), types.T_enum.ToType()}, {types.T_varchar.ToType()}} {
			_, err := GetFunctionByName(context.Background(), name, inputs)
			require.Error(t, err)
		}
	}
}

func TestLegacyBitAggregateStateRemainsReadable(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		mp := mpool.MustNewZero()
		func() {
			bound, err := GetFunctionByName(context.Background(), name, []types.Type{types.T_uint64.ToType()})
			require.NoError(t, err)
			legacy, err := aggexec.MakeAgg(mp, bound.GetEncodedOverloadID(), false, types.New(types.T_bit, 64, 0))
			require.NoError(t, err)
			defer legacy.Free()
			require.NoError(t, legacy.GroupGrow(1))
			v := vector.NewVec(types.New(types.T_bit, 64, 0))
			defer v.Free(mp)
			require.NoError(t, vector.AppendFixed(v, uint64(3), false, mp))
			require.NoError(t, legacy.Fill(0, 0, []*vector.Vector{v}))
			var wire bytes.Buffer
			require.NoError(t, legacy.SaveIntermediateResult(1, [][]uint8{{1}}, &wire))
			restored, err := aggexec.MakeAgg(mp, bound.GetEncodedOverloadID(), false, types.New(types.T_bit, 64, 0))
			require.NoError(t, err)
			defer restored.Free()
			require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
			out, err := restored.Flush()
			require.NoError(t, err)
			defer out[0].Free(mp)
			if name == "sum" {
				require.Equal(t, types.T_uint64, out[0].GetType().Oid)
				require.Equal(t, uint64(3), vector.MustFixedColNoTypeCheck[uint64](out[0])[0])
			} else {
				require.Equal(t, types.T_float64, out[0].GetType().Oid)
				require.Equal(t, float64(3), vector.MustFixedColNoTypeCheck[float64](out[0])[0])
			}
		}()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	}
}

func TestBoundBitSumAvgPartialStateRoundTrip(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		for _, tc := range []struct {
			a, b     uint64
			sum, avg string
		}{
			{math.MaxUint64 - 2, 1, "18446744073709551614", "9223372036854775807.0000"},
			{math.MaxUint64 - 1, 1, "18446744073709551615", "9223372036854775807.5000"},
			{uint64(1) << 63, math.MaxUint64, "27670116110564327423", "13835058055282163711.5000"},
		} {
			t.Run(name+"/"+tc.sum, func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
				bound, err := GetFunctionByName(context.Background(), name, []types.Type{types.New(types.T_bit, 64, 0)})
				require.NoError(t, err)
				cast, _ := bound.ShouldDoImplicitTypeCast()
				makeExec := func() aggexec.AggFuncExec {
					x, err := aggexec.MakeAgg(mp, bound.GetEncodedOverloadID(), false, cast[0])
					require.NoError(t, err)
					return x
				}
				partials := make([]aggexec.AggFuncExec, 0, 2)
				for _, value := range []uint64{tc.a, tc.b} {
					x := makeExec()
					defer x.Free()
					require.NoError(t, x.GroupGrow(1))
					v := vector.NewVec(cast[0])
					defer v.Free(mp)
					require.NoError(t, vector.AppendFixed(v, value, false, mp))
					require.NoError(t, x.Fill(0, 0, []*vector.Vector{v}))
					var wire bytes.Buffer
					require.NoError(t, x.SaveIntermediateResult(1, [][]uint8{{1}}, &wire))
					restored := makeExec()
					defer restored.Free()
					require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
					partials = append(partials, restored)
				}
				require.NoError(t, partials[0].Merge(partials[1], 0, 0))
				out, err := partials[0].Flush()
				require.NoError(t, err)
				defer out[0].Free(mp)
				want := tc.sum
				if name == "avg" {
					want = tc.avg
				}
				require.Equal(t, want, vector.MustFixedColNoTypeCheck[types.Decimal128](out[0])[0].Format(out[0].GetType().Scale))
			})
		}
	}
}

func TestMySQLNumericAggTypeCheckPreservesTemporalScale(t *testing.T) {
	for _, name := range []string{"var_pop", "var_samp", "stddev_pop", "stddev_samp"} {
		t.Run(name, func(t *testing.T) {
			for _, oid := range []types.T{types.T_time, types.T_datetime, types.T_timestamp} {
				for scale := int32(1); scale <= 6; scale++ {
					input := types.New(oid, 0, scale)
					got, err := GetFunctionByName(context.Background(), name, []types.Type{input})
					require.NoError(t, err)
					castTypes, shouldCast := got.ShouldDoImplicitTypeCast()
					require.True(t, shouldCast)
					require.Equal(t, []types.Type{
						types.New(types.T_decimal128, 38, scale),
					}, castTypes, "oid=%v scale=%d", oid, scale)
				}
			}
		})
	}
}

func TestMySQLNumericAggTypeCheckRejectsAndHandlesSpecialTypes(t *testing.T) {
	result := mysqlNumericAggTypeCheck(nil)
	require.Equal(t, failedAggParametersWrong, result.status)

	result = mysqlNumericAggTypeCheck([]types.Type{types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, []types.Type{types.T_float64.ToType()}, result.finalType)

	result = mysqlNumericAggTypeCheck([]types.Type{types.T_int64.ToType()})
	require.Equal(t, succeedMatched, result.status)

	result = mysqlNumericAggTypeCheck([]types.Type{types.T_bool.ToType()})
	require.Equal(t, failedAggParametersWrong, result.status)
}

func TestJSONObjectAggKeyTypeBoundaries(t *testing.T) {
	ctx := context.Background()
	valueType := types.T_varchar.ToType()

	t.Run("any becomes varchar", func(t *testing.T) {
		resolved, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{types.T_any.ToType(), valueType})
		require.NoError(t, err)
		targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
		require.True(t, shouldCast)
		require.Equal(t, []types.Type{valueType, valueType}, targets)
	})

	for _, keyType := range []types.Type{
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_text.ToType(),
		types.T_binary.ToType(),
		types.T_varbinary.ToType(),
		types.T_blob.ToType(),
	} {
		t.Run("string/"+keyType.Oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{keyType, valueType})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, keyType, targets[0])
			require.Equal(t, valueType, targets[1])
		})
	}

	for _, keyType := range []types.Type{
		types.T_bit.ToType(),
		types.T_bool.ToType(),
		types.T_date.ToType(),
		types.T_time.ToType(),
		types.T_year.ToType(),
		types.T_datetime.ToType(),
		types.T_timestamp.ToType(),
		types.T_enum.ToType(),
		types.T_json.ToType(),
		types.T_uuid.ToType(),
	} {
		t.Run("rejected/"+keyType.Oid.String(), func(t *testing.T) {
			if keyType.Oid == types.T_bit {
				require.True(t, keyType.IsNumeric(), "BIT is numeric in the shared type predicate")
			}
			_, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{keyType, valueType})
			require.Error(t, err)
		})
	}

	for _, valueType := range []types.Type{
		types.New(types.T_bit, 1, 0),
		types.New(types.T_bit, 8, 0),
		types.New(types.T_bit, 64, 0),
		types.T_binary.ToType(),
		types.T_varbinary.ToType(),
		types.T_blob.ToType(),
	} {
		t.Run("value/"+valueType.Oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{types.T_int64.ToType(), valueType})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, []types.Type{types.T_varchar.ToType(), valueType}, targets)
			require.Equal(t, types.T_json, resolved.GetReturnType().Oid)
		})
	}

	for _, inputs := range [][]types.Type{
		nil,
		{types.T_int64.ToType()},
		{types.T_int64.ToType(), valueType, valueType},
	} {
		_, err := GetFunctionByName(ctx, "json_objectagg", inputs)
		require.Error(t, err)
	}
}

func TestJSONArrayAggOpaqueValueTypeCheck(t *testing.T) {
	for _, valueType := range []types.Type{
		types.New(types.T_bit, 1, 0),
		types.New(types.T_bit, 8, 0),
		types.New(types.T_bit, 64, 0),
		types.T_binary.ToType(),
		types.T_varbinary.ToType(),
		types.T_blob.ToType(),
	} {
		t.Run(valueType.Oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "json_arrayagg", []types.Type{valueType})
			require.NoError(t, err)
			require.Equal(t, types.T_json, resolved.GetReturnType().Oid)
			_, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast, "opaque value type must retain its OID and width")
		})
	}
}
