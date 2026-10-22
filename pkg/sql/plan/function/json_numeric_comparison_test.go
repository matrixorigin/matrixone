// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONNumericComparisonDomains(t *testing.T) {
	parse := func(s string) bytejson.ByteJson {
		value, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		return value
	}
	for _, tc := range []struct {
		name, json, peer string
		typ              types.T
		want             int
		fails            bool
	}{
		{"int8 maximum", "127", "127", types.T_int8, 0, false},
		{"int8 accepted fraction", "127.9", "127", types.T_int8, 1, false},
		{"int8 overflow", "128", "1", types.T_int8, 0, true},
		{"int8 minimum fraction", "-128.9", "-128", types.T_int8, -1, false},
		{"int8 underflow", "-129", "1", types.T_int8, 0, true},
		{"int16 maximum", "32767", "32767", types.T_int16, 0, false},
		{"int16 overflow", "32768", "1", types.T_int16, 0, true},
		{"int32 maximum", "2147483647", "2147483647", types.T_int32, 0, false},
		{"int32 overflow", "2147483648", "1", types.T_int32, 0, true},
		{"int64 precision", "9007199254740993", "9007199254740992", types.T_int64, 1, false},
		{"int64 maximum", "9223372036854775807", "9223372036854775807", types.T_int64, 0, false},
		{"int64 minimum", "-9223372036854775808", "-9223372036854775808", types.T_int64, 0, false},
		{"int64 overflow", "18446744073709551615", "1", types.T_int64, 0, true},
		{"uint8 maximum", "255", "255", types.T_uint8, 0, false},
		{"uint8 accepted fraction", "255.9", "255", types.T_uint8, 1, false},
		{"uint8 overflow", "256", "1", types.T_uint8, 0, true},
		{"uint16 maximum", "65535", "65535", types.T_uint16, 0, false},
		{"uint16 overflow", "65536", "1", types.T_uint16, 0, true},
		{"uint32 maximum", "4294967295", "4294967295", types.T_uint32, 0, false},
		{"uint32 overflow", "4294967296", "1", types.T_uint32, 0, true},
		{"uint64 maximum", "18446744073709551615", "18446744073709551615", types.T_uint64, 0, false},
		{"unsigned negative fraction", "-0.5", "0", types.T_uint64, 0, true},
		{"negative fraction", "-1.5", "-1", types.T_int64, -1, false},
		{"float32 rounding", "16777217", "16777216", types.T_float32, 0, false},
		{"float32 overflow", "1e100", "1", types.T_float32, 0, true},
		{"double fraction", "1.5", "1", types.T_float64, 1, false},
		{"decimal64 exact", "9007199254740993", "9007199254740992", types.T_decimal64, 1, false},
		{"decimal128 exact", "9007199254740993", "9007199254740992", types.T_decimal128, 1, false},
		{"decimal256 exact", "9007199254740993", "9007199254740992", types.T_decimal256, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, err := makeJSONNumericPeer(context.Background(), parse(tc.peer), tc.typ, vector.PrepareParamNone)
			require.NoError(t, err)
			got, null, err := compareJSONNumeric(context.Background(), parse(tc.json), &peer)
			if tc.fails {
				require.ErrorContains(t, err, "JSON -> "+tc.typ.String())
				return
			}
			require.NoError(t, err)
			require.False(t, null)
			require.Equal(t, tc.want, got)
		})
	}
	// Exact DECIMAL range validation retains the pre-existing truncation
	// acceptance domain even when the original number is above the peer.
	peer, err := makeJSONNumericPeer(context.Background(), parse("18446744073709551615"), types.T_uint64, vector.PrepareParamNone)
	require.NoError(t, err)
	cmp, _, err := compareJSONNumeric(context.Background(), newTypedByteJson(bytejson.TpCodeDecimal, "18446744073709551615.9"), &peer)
	require.NoError(t, err)
	require.Equal(t, 1, cmp)
	peer, err = makeJSONNumericPeer(context.Background(), newTypedByteJson(bytejson.TpCodeDecimal, "1.0000000000000000001"), types.T_decimal256, vector.PrepareParamNone)
	require.NoError(t, err)
	cmp, _, err = compareJSONNumeric(context.Background(), newTypedByteJson(bytejson.TpCodeDecimal, "1.0000000000000000002"), &peer)
	require.NoError(t, err)
	require.Equal(t, 1, cmp)
}

func TestJSONNumericDirectTypesAndResolution(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	jsonValue, err := bytejson.CreateByteJSON(1.5)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(jsonValue)
	require.NoError(t, err)
	jsonVector, err := vector.NewConstBytes(types.T_json.ToType(), encoded, 2, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { jsonVector.Free(proc.Mp()) })
	for _, typ := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		t.Run(typ.String(), func(t *testing.T) {
			numericType := typ.ToType()
			if typ.IsDecimal() {
				numericType.Scale = 2
			}
			numbers := vector.NewVec(numericType)
			t.Cleanup(func() { numbers.Free(proc.Mp()) })
			for range 2 {
				switch typ {
				case types.T_int8:
					err = vector.AppendFixed(numbers, int8(1), false, proc.Mp())
				case types.T_int16:
					err = vector.AppendFixed(numbers, int16(1), false, proc.Mp())
				case types.T_int32:
					err = vector.AppendFixed(numbers, int32(1), false, proc.Mp())
				case types.T_int64:
					err = vector.AppendFixed(numbers, int64(1), false, proc.Mp())
				case types.T_uint8:
					err = vector.AppendFixed(numbers, uint8(1), false, proc.Mp())
				case types.T_uint16:
					err = vector.AppendFixed(numbers, uint16(1), false, proc.Mp())
				case types.T_uint32:
					err = vector.AppendFixed(numbers, uint32(1), false, proc.Mp())
				case types.T_uint64:
					err = vector.AppendFixed(numbers, uint64(1), false, proc.Mp())
				case types.T_float32:
					err = vector.AppendFixed(numbers, float32(1), false, proc.Mp())
				case types.T_float64:
					err = vector.AppendFixed(numbers, float64(1), false, proc.Mp())
				case types.T_decimal64:
					err = vector.AppendFixed(numbers, types.Decimal64(100), false, proc.Mp())
				case types.T_decimal128:
					err = vector.AppendFixed(numbers, types.Decimal128{B0_63: 100}, false, proc.Mp())
				case types.T_decimal256:
					err = vector.AppendFixed(numbers, types.Decimal256{B0_63: 100}, false, proc.Mp())
				}
				require.NoError(t, err)
			}
			result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
			t.Cleanup(result.Free)
			require.NoError(t, result.PreExtendAndReset(2))
			require.NoError(t, greatThanFn([]*vector.Vector{jsonVector, numbers}, result, proc, 2, nil))
			require.Equal(t, []bool{true, true}, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
			// Exercise concrete prepared dispatch for every standard numeric OID,
			// including all three DECIMAL physical types.
			paramJSON, err := bytejson.CreateByteJSON(int64(1))
			require.NoError(t, err)
			paramBytes, err := types.EncodeJson(paramJSON)
			require.NoError(t, err)
			param, err := vector.NewConstBytes(types.T_json.ToType(), paramBytes, 2, proc.Mp())
			require.NoError(t, err)
			t.Cleanup(func() { param.Free(proc.Mp()) })
			param.SetPreparedJSONComparisonParam()
			param.SetPrepareParamType(typ)
			kind, ok := vector.PrepareParamKindForType(typ)
			require.True(t, ok)
			param.SetPrepareParamKind(kind)
			require.NoError(t, greatThanFn([]*vector.Vector{jsonVector, param}, result, proc, 2, nil))
			require.Equal(t, []bool{true, true}, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
			for _, name := range []string{"=", "!=", "<=>", "<", "<=", ">", ">="} {
				for _, reversed := range []bool{false, true} {
					args := []types.Type{types.T_json.ToType(), numericType}
					if reversed {
						args[0], args[1] = args[1], args[0]
					}
					resolved, err := GetFunctionByName(context.Background(), name, args)
					require.NoError(t, err)
					require.Empty(t, resolved.needCast)
					_, overload := DecodeOverloadID(resolved.GetEncodedOverloadID())
					require.Equal(t, int32(1), overload)
					planArgs := []*plan.Expr{{Typ: args[0].PlanType()}, {Typ: args[1].PlanType()}}
					for _, arg := range planArgs {
						arg.Typ.NotNullable = true
					}
					require.Equal(t, name == "<=>", DeduceNotNullable(resolved.GetEncodedOverloadID(), planArgs))
					require.False(t, CanUseZoneMapComparison(resolved.GetEncodedOverloadID(), planArgs))
				}
			}
		})
	}
	for _, excluded := range []types.T{types.T_bool, types.T_bit, types.T_year, types.T_enum, types.T_int128, types.T_uint128} {
		require.False(t, IsJSONNumericComparison(types.T_json, excluded))
	}
}

func TestJSONNumericOrderingAdapterMetadataAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	result := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
	t.Cleanup(result.Free)
	for _, tc := range []struct {
		text   string
		typ    types.T
		kind   vector.PrepareParamKind
		marker bool
		null   bool
	}{
		{"1", types.T_int64, vector.PrepareParamInteger, true, false},
		{"1.5", types.T_float64, vector.PrepareParamFloat, true, false},
		{"1.25", types.T_decimal256, vector.PrepareParamDecimal, true, false},
		{"", types.T_int64, vector.PrepareParamNone, true, true},
		{`"text"`, types.T_text, vector.PrepareParamNone, false, false},
	} {
		func() {
			input, err := vector.NewConstBytes(types.T_text.ToType(), []byte(tc.text), 2, proc.Mp())
			require.NoError(t, err)
			defer input.Free(proc.Mp())
			if tc.null {
				input.SetAllNulls(2)
			}
			// Publishing all-NULL data clears transient parameter metadata.
			input.SetPrepareParamType(tc.typ)
			input.SetPrepareParamKind(tc.kind)
			require.NoError(t, result.PreExtendAndReset(2))
			require.NoError(t, normalizeJsonOrderingParam([]*vector.Vector{input}, result, proc, 2, nil))
			got := result.GetResultVector()
			require.Equal(t, tc.marker, got.IsPreparedJSONComparisonParam())
			require.Equal(t, tc.null, got.IsNull(0))
			if tc.marker {
				require.Equal(t, tc.typ, got.GetPrepareParamType())
			}
		}()
	}
	empty := vector.NewVec(types.T_text.ToType())
	t.Cleanup(func() { empty.Free(proc.Mp()) })
	empty.SetPrepareParamType(types.T_int64)
	empty.SetPrepareParamKind(vector.PrepareParamInteger)
	require.NoError(t, result.PreExtendAndReset(0))
	require.NoError(t, normalizeJsonOrderingParam([]*vector.Vector{empty}, result, proc, 0, nil))
}

func BenchmarkJSONNumericDirectComparison(b *testing.B) {
	for _, decimal := range []bool{false, true} {
		for _, constant := range []bool{false, true} {
			name := "integer/flat"
			if decimal {
				name = "decimal/flat"
			}
			if constant {
				name = name[:len(name)-4] + "const"
			}
			b.Run(name, func(b *testing.B) {
				const length = 1024
				proc := testutil.NewProcess(b)
				defer proc.Free()
				j, err := bytejson.CreateByteJSON(1.5)
				require.NoError(b, err)
				encoded, err := types.EncodeJson(j)
				require.NoError(b, err)
				jsonValues := vector.NewVec(types.T_json.ToType())
				defer jsonValues.Free(proc.Mp())
				for range length {
					require.NoError(b, vector.AppendBytes(jsonValues, encoded, false, proc.Mp()))
				}
				var numbers *vector.Vector
				if decimal {
					typ := types.New(types.T_decimal128, 20, 2)
					if constant {
						numbers, err = vector.NewConstFixed(typ, types.Decimal128{B0_63: 100}, length, proc.Mp())
					} else {
						numbers = vector.NewVec(typ)
					}
				} else if constant {
					numbers, err = vector.NewConstFixed(types.T_int64.ToType(), int64(1), length, proc.Mp())
				} else {
					numbers = vector.NewVec(types.T_int64.ToType())
				}
				require.NoError(b, err)
				defer numbers.Free(proc.Mp())
				if !constant {
					for range length {
						if decimal {
							err = vector.AppendFixed(numbers, types.Decimal128{B0_63: 100}, false, proc.Mp())
						} else {
							err = vector.AppendFixed(numbers, int64(1), false, proc.Mp())
						}
						require.NoError(b, err)
					}
				}
				result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(b, result.PreExtendAndReset(length))
				args := []*vector.Vector{jsonValues, numbers}
				b.ReportAllocs()
				b.ReportMetric(length, "rows/op")
				b.ResetTimer()
				for b.Loop() {
					if err := result.PreExtendAndReset(length); err != nil {
						b.Fatal(err)
					}
					if err := greatThanFn(args, result, proc, length, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
