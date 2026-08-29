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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJsonComparisonParamPreservesPreparedScalarType(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	encode := func(value any) string {
		bj, err := bytejson.CreateByteJSON(value)
		require.NoError(t, err)
		encoded, err := types.EncodeJson(bj)
		require.NoError(t, err)
		return string(encoded)
	}

	inputs := []FunctionTestInput{NewFunctionTestInput(
		types.T_text.ToType(),
		[]string{"true", "false", "7", "1.25", "true"},
		[]bool{false, false, false, false, true},
	)}
	expect := NewFunctionTestResult(types.T_json.ToType(), false,
		[]string{encode(true), encode(false), encode(int64(7)), encode(1.25), ""}, []bool{false, false, false, false, true})
	testCase := NewFunctionTestCase(proc, inputs, expect, normalizeJsonComparisonParam)
	testCase.parameters[0].SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamBoolean,
		vector.PrepareParamBoolean,
		vector.PrepareParamInteger,
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	ok, info := testCase.Run()
	require.True(t, ok, info)
	resultVector := testCase.GetResultVectorDirectly()
	require.True(t, resultVector.HasPrepareParamKind())
	require.True(t, resultVector.IsPreparedJSONComparisonParam())
	for row, want := range []vector.PrepareParamKind{
		vector.PrepareParamBoolean,
		vector.PrepareParamBoolean,
		vector.PrepareParamInteger,
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	} {
		require.Equal(t, want, resultVector.GetPrepareParamKindAt(row))
	}

	t.Run("invalid prepared value", func(t *testing.T) {
		invalid := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), []string{"not-an-integer"}, nil)},
			NewFunctionTestResult(types.T_json.ToType(), true, nil, nil), normalizeJsonComparisonParam)
		invalid.parameters[0].SetPrepareParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
		ok, info := invalid.Run()
		require.True(t, ok, info)
	})

	t.Run("masked invalid parameter is not evaluated", func(t *testing.T) {
		input := vector.NewVec(types.T_text.ToType())
		defer input.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(input, []byte("7"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(input, []byte("invalid"), false, proc.Mp()))
		input.SetPrepareParamKind(vector.PrepareParamInteger)

		result := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, normalizeJsonComparisonParam(
			[]*vector.Vector{input}, result, proc, 2,
			&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}))
		require.False(t, result.GetResultVector().IsNull(0))
		require.True(t, result.GetResultVector().IsNull(1))
		require.True(t, result.GetResultVector().IsPreparedJSONComparisonParam())
	})

	t.Run("constant parameter shares one encoded payload", func(t *testing.T) {
		const length = 4
		value := []byte(strings.Repeat("prepared-json-string-", 4))
		input, err := vector.NewConstBytes(types.T_text.ToType(), value, length, proc.Mp())
		require.NoError(t, err)
		defer input.Free(proc.Mp())
		input.SetPrepareParamKind(vector.PrepareParamNone)

		result := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(length))
		require.NoError(t, normalizeJsonComparisonParam(
			[]*vector.Vector{input}, result, proc, length, nil))

		want, err := encodeJsonComparisonParam(proc.Ctx, value, vector.PrepareParamNone)
		require.NoError(t, err)
		got := result.GetResultVector()
		require.True(t, got.HasPrepareParamKind())
		require.Equal(t, vector.PrepareParamNone, got.GetPrepareParamKind())
		require.Equal(t, types.T_any, got.GetPrepareParamType())
		require.True(t, got.IsPreparedJSONComparisonParam())
		require.Empty(t, got.GetPrepareParamKinds())
		require.Len(t, got.GetArea(), len(want))
		for row := 0; row < length; row++ {
			require.Equal(t, want, got.GetBytesAt(row))
		}
	})

	t.Run("constant parameter propagates concrete numeric type", func(t *testing.T) {
		input, err := vector.NewConstBytes(types.T_text.ToType(), []byte("42"), 1, proc.Mp())
		require.NoError(t, err)
		defer input.Free(proc.Mp())
		input.SetPrepareParamKind(vector.PrepareParamInteger)
		input.SetPrepareParamType(types.T_int64)

		result := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(1))
		require.NoError(t, normalizeJsonComparisonParam(
			[]*vector.Vector{input}, result, proc, 1, nil))

		got := result.GetResultVector()
		require.Equal(t, vector.PrepareParamInteger, got.GetPrepareParamKind())
		require.Equal(t, types.T_int64, got.GetPrepareParamType())
		require.True(t, got.IsPreparedJSONComparisonParam())
	})
}

func TestPreparedJSONComparisonCoercion(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	encode := func(value any) []byte {
		bj, err := bytejson.CreateByteJSON(value)
		require.NoError(t, err)
		data, err := types.EncodeJson(bj)
		require.NoError(t, err)
		return data
	}
	makeJSON := func(values []any, nullAt ...uint64) *vector.Vector {
		v := vector.NewVec(types.T_json.ToType())
		nullsAt := make(map[uint64]bool)
		for _, i := range nullAt {
			nullsAt[i] = true
		}
		for i, value := range values {
			require.NoError(t, vector.AppendBytes(v, encode(value), nullsAt[uint64(i)], proc.Mp()))
		}
		return v
	}
	makePreparedJSON := func(values []any, nullAt ...uint64) *vector.Vector {
		v := makeJSON(values, nullAt...)
		v.SetPreparedJSONComparisonParam()
		return v
	}
	makeResult := func(length int) *vector.FunctionResult[bool] {
		result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp()).(*vector.FunctionResult[bool])
		require.NoError(t, result.PreExtendAndReset(length))
		return result
	}

	t.Run("mixed JSON boolean operators preserve scalar category", func(t *testing.T) {
		jsonValues := makeJSON([]any{true, false, float64(1), float64(0), "true", "false", nil, nil, true}, 7)
		booleanValues := vector.NewVec(types.T_bool.ToType())
		for i, value := range []bool{true, true, true, false, true, false, true, false, false} {
			require.NoError(t, vector.AppendFixed(booleanValues, value, i == 7 || i == 8, proc.Mp()))
		}

		for _, orientation := range []struct {
			name       string
			parameters []*vector.Vector
		}{
			{name: "JSON left", parameters: []*vector.Vector{jsonValues, booleanValues}},
			{name: "JSON right", parameters: []*vector.Vector{booleanValues, jsonValues}},
		} {
			t.Run(orientation.name, func(t *testing.T) {
				for _, operator := range []struct {
					name          string
					fn            fEvalFn
					expected      []bool
					expectedNulls []bool
				}{
					{
						name:          "equal",
						fn:            equalFn,
						expected:      []bool{true, false, true, true, false, false, false, false, false},
						expectedNulls: []bool{false, false, false, false, false, false, true, true, true},
					},
					{
						name:          "not equal",
						fn:            notEqualFn,
						expected:      []bool{false, true, false, false, true, true, false, false, false},
						expectedNulls: []bool{false, false, false, false, false, false, true, true, true},
					},
					{
						name:          "null safe equal",
						fn:            nullSafeEqualFn,
						expected:      []bool{true, false, true, true, false, false, false, true, false},
						expectedNulls: []bool{false, false, false, false, false, false, false, false, false},
					},
				} {
					t.Run(operator.name, func(t *testing.T) {
						result := makeResult(len(operator.expected))
						require.NoError(t, operator.fn(
							orientation.parameters, result, proc, len(operator.expected), nil))
						got := result.GetResultVector()
						require.Equal(t, operator.expected, vector.MustFixedColNoTypeCheck[bool](got))
						for row, expectedNull := range operator.expectedNulls {
							require.Equal(t, expectedNull, got.IsNull(uint64(row)), "row %d", row)
						}
					})
				}
			})
		}
	})

	t.Run("mixed JSON boolean overloads do not insert a cast", func(t *testing.T) {
		for _, operator := range []string{"=", "!=", "<>", "<=>"} {
			for _, inputTypes := range [][]types.Type{
				{types.T_json.ToType(), types.T_bool.ToType()},
				{types.T_bool.ToType(), types.T_json.ToType()},
			} {
				resolved, err := GetFunctionByName(proc.Ctx, operator, inputTypes)
				require.NoError(t, err)
				_, needsCast := resolved.ShouldDoImplicitTypeCast()
				require.False(t, needsCast, "%s %v", operator, inputTypes)
			}
		}
	})

	t.Run("mixed JSON boolean comparison rejects containers", func(t *testing.T) {
		booleanValue, err := vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
		require.NoError(t, err)
		for _, value := range []any{map[string]any{"v": true}, []any{true}} {
			jsonValue := makeJSON([]any{value})
			require.Error(t, equalFn(
				[]*vector.Vector{jsonValue, booleanValue}, makeResult(1), proc, 1, nil))
		}
	})

	t.Run("mixed JSON boolean comparison masks rows before conversion", func(t *testing.T) {
		jsonValues := makeJSON([]any{true, map[string]any{"unsupported": true}})
		booleanValues, err := vector.NewConstFixed(types.T_bool.ToType(), true, 2, proc.Mp())
		require.NoError(t, err)
		result := makeResult(2)
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
		require.NoError(t, equalFn(
			[]*vector.Vector{jsonValues, booleanValues}, result, proc, 2, selectList))
		got := result.GetResultVector()
		require.True(t, vector.MustFixedColNoTypeCheck[bool](got)[0])
		require.True(t, got.IsNull(1))
	})

	t.Run("mixed JSON boolean comparison skips an all-masked batch", func(t *testing.T) {
		malformedJSON := vector.NewVec(types.T_json.ToType())
		require.NoError(t, vector.AppendBytes(malformedJSON, []byte("invalid"), false, proc.Mp()))
		booleanValue, err := vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
		require.NoError(t, err)
		result := makeResult(1)
		require.NoError(t, equalFn(
			[]*vector.Vector{malformedJSON, booleanValue}, result, proc, 1,
			&FunctionSelectList{AnyNull: true, AllNull: true}))
		require.True(t, result.GetResultVector().IsNull(0))
	})

	t.Run("mixed JSON boolean comparison rejects truncated scalar encodings", func(t *testing.T) {
		booleanValue, err := vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
		require.NoError(t, err)
		for _, encoded := range [][]byte{
			{byte(bytejson.TpCodeString), 4, 't'},
			{byte(bytejson.TpCodeString), 0x80},
			{byte(bytejson.TpCodeLiteral)},
			{byte(bytejson.TpCodeInt64), 0, 0, 0, 0, 0, 0, 0},
		} {
			malformedJSON := vector.NewVec(types.T_json.ToType())
			require.NoError(t, vector.AppendBytes(malformedJSON, encoded, false, proc.Mp()))
			require.Error(t, equalFn(
				[]*vector.Vector{malformedJSON, booleanValue}, makeResult(1), proc, 1, nil))
		}
	})

	t.Run("mixed JSON boolean comparison accepts constant SQL null", func(t *testing.T) {
		jsonValues := makeJSON([]any{nil, true, "true"})
		booleanNull := vector.NewConstNull(types.T_bool.ToType(), 3, proc.Mp())

		ordinaryResult := makeResult(3)
		require.NoError(t, equalFn(
			[]*vector.Vector{jsonValues, booleanNull}, ordinaryResult, proc, 3, nil))
		for row := 0; row < 3; row++ {
			require.True(t, ordinaryResult.GetResultVector().IsNull(uint64(row)))
		}

		result := makeResult(3)
		require.NoError(t, nullSafeEqualFn(
			[]*vector.Vector{jsonValues, booleanNull}, result, proc, 3, nil))
		require.Equal(t, []bool{true, false, false},
			vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
	})

	t.Run("ordinary JSON provenance stays on ordinary comparison path", func(t *testing.T) {
		left := makeJSON([]any{true})
		right := makeJSON([]any{true})
		left.SetPrepareParamKind(vector.PrepareParamBoolean)
		right.SetPrepareParamKind(vector.PrepareParamBoolean)
		result := makeResult(1)
		require.NoError(t, equalFn(
			[]*vector.Vector{left, right}, result, proc, 1, nil))
		require.Equal(t, []bool{true},
			vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
	})

	t.Run("boolean numeric and string categories", func(t *testing.T) {
		jsonValues := makeJSON([]any{float64(1), "true", true})
		params := makePreparedJSON([]any{true, true, true})
		params.SetPrepareParamKinds([]vector.PrepareParamKind{vector.PrepareParamBoolean, vector.PrepareParamBoolean, vector.PrepareParamBoolean})
		result := makeResult(3)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 3, false, func(c int) bool { return c == 0 }, nil))
		got := result.GetResultVector()
		require.Equal(t, []bool{true, false, true}, vector.MustFixedColNoTypeCheck[bool](got))
	})

	t.Run("boolean metadata requires boolean adapter payload", func(t *testing.T) {
		jsonValues := makeJSON([]any{true})
		params := makePreparedJSON([]any{float64(1)})
		params.SetPrepareParamKind(vector.PrepareParamBoolean)
		require.Error(t, comparePreparedJSON(
			[]*vector.Vector{jsonValues, params}, makeResult(1), proc, 1, false,
			func(c int) bool { return c == 0 }, nil))
	})

	t.Run("reversed numeric and null safe nulls", func(t *testing.T) {
		jsonValues := makeJSON([]any{float64(2), nil}, 1)
		params := makePreparedJSON([]any{2.0, nil}, 1)
		params.SetPrepareParamKinds([]vector.PrepareParamKind{vector.PrepareParamFloat, vector.PrepareParamFloat})
		result := makeResult(2)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{params, jsonValues}, result, proc, 2, true, func(c int) bool { return c == 0 }, nil))
		got := result.GetResultVector()
		require.Equal(t, []bool{true, true}, vector.MustFixedColNoTypeCheck[bool](got))
	})

	t.Run("decimal category follows floating comparison coercion", func(t *testing.T) {
		jsonValues := makeJSON([]any{float64(1.25), "1.25"})
		decimal := newTypedByteJson(bytejson.TpCodeDecimal, "1.25")
		params := makePreparedJSON([]any{decimal, decimal})
		params.SetPrepareParamKind(vector.PrepareParamDecimal)
		result := makeResult(2)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 2, false, func(c int) bool { return c == 0 }, nil))
		require.Equal(t, []bool{true, true}, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
	})

	t.Run("ordinary comparison propagates null", func(t *testing.T) {
		jsonValues := makeJSON([]any{nil}, 0)
		params := makePreparedJSON([]any{true})
		params.SetPrepareParamKinds([]vector.PrepareParamKind{vector.PrepareParamBoolean})
		result := makeResult(1)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 1, false, func(c int) bool { return c == 0 }, nil))
		require.True(t, result.GetResultVector().IsNull(0))
	})

	t.Run("invalid encoded JSON is rejected", func(t *testing.T) {
		left := vector.NewVec(types.T_json.ToType())
		require.NoError(t, vector.AppendBytes(left, []byte("invalid"), false, proc.Mp()))
		right := makePreparedJSON([]any{true})
		right.SetPrepareParamKinds([]vector.PrepareParamKind{vector.PrepareParamBoolean})
		require.Error(t, comparePreparedJSON([]*vector.Vector{left, right}, makeResult(1), proc, 1, false, func(c int) bool { return c == 0 }, nil))
	})

	t.Run("string category and malformed adapter output", func(t *testing.T) {
		jsonValues := makeJSON([]any{"7"})
		params := makePreparedJSON([]any{"7"})
		params.SetPrepareParamKind(vector.PrepareParamNone)
		result := makeResult(1)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 1, false, func(c int) bool { return c == 0 }, nil))
		require.Equal(t, []bool{true}, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))

		malformed := makePreparedJSON([]any{"bad"})
		malformed.SetPrepareParamKind(vector.PrepareParamInteger)
		require.Error(t, comparePreparedJSON([]*vector.Vector{jsonValues, malformed}, makeResult(1), proc, 1, false, func(c int) bool { return c == 0 }, nil))
	})

	t.Run("integer comparison preserves precision", func(t *testing.T) {
		jsonValues := makeJSON([]any{
			int64(9007199254740992),
			int64(math.MaxInt64 - 1),
			uint64(math.MaxUint64 - 1),
		})
		params := makePreparedJSON([]any{
			int64(9007199254740993),
			int64(math.MaxInt64),
			uint64(math.MaxUint64),
		})
		params.SetPrepareParamKind(vector.PrepareParamInteger)
		result := makeResult(3)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 3, false, func(c int) bool { return c == 0 }, nil))
		got := result.GetResultVector()
		require.Equal(t, []bool{false, false, false}, vector.MustFixedColNoTypeCheck[bool](got))
		require.True(t, got.GetNulls().IsEmpty())
	})

	t.Run("integer comparison spans signed and unsigned boundaries", func(t *testing.T) {
		jsonValues := makeJSON([]any{
			int64(math.MaxInt64),
			uint64(math.MaxUint64),
			uint64(math.MaxUint64),
			"18446744073709551615",
		})
		params := makePreparedJSON([]any{
			int64(math.MaxInt64),
			uint64(math.MaxUint64),
			int64(1),
			uint64(math.MaxUint64),
		})
		params.SetPrepareParamKind(vector.PrepareParamInteger)
		result := makeResult(4)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 4, false, func(c int) bool { return c == 0 }, nil))
		require.Equal(t, []bool{true, true, false, true}, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
	})

	t.Run("concrete signed type preserves overflow errors", func(t *testing.T) {
		jsonValues := makeJSON([]any{uint64(math.MaxUint64)})
		params := makePreparedJSON([]any{int64(math.MaxInt64)})
		params.SetPrepareParamKind(vector.PrepareParamInteger)
		params.SetPrepareParamType(types.T_int64)
		err := comparePreparedJSON(
			[]*vector.Vector{jsonValues, params}, makeResult(1), proc, 1,
			false, func(c int) bool { return c == 0 }, nil)
		require.ErrorContains(t, err, "JSON -> BIGINT")
	})

	t.Run("concrete narrow integer rejects out of range JSON", func(t *testing.T) {
		jsonValues := makeJSON([]any{int64(math.MaxInt8 + 1)})
		params := makePreparedJSON([]any{int64(1)})
		params.SetPrepareParamKind(vector.PrepareParamInteger)
		params.SetPrepareParamType(types.T_int8)
		err := comparePreparedJSON(
			[]*vector.Vector{jsonValues, params}, makeResult(1), proc, 1,
			false, func(c int) bool { return c == 0 }, nil)
		require.ErrorContains(t, err, "JSON -> TINYINT")
	})

	t.Run("concrete unsigned type rejects negative JSON", func(t *testing.T) {
		jsonValues := makeJSON([]any{int64(-1)})
		params := makePreparedJSON([]any{uint64(1)})
		params.SetPrepareParamKind(vector.PrepareParamInteger)
		params.SetPrepareParamType(types.T_uint64)
		err := comparePreparedJSON(
			[]*vector.Vector{jsonValues, params}, makeResult(1), proc, 1,
			false, func(c int) bool { return c == 0 }, nil)
		require.ErrorContains(t, err, "JSON -> BIGINT UNSIGNED")
	})

	t.Run("concrete float32 rounds both operands", func(t *testing.T) {
		jsonValues := makeJSON([]any{float64(16777217)})
		params := makePreparedJSON([]any{float64(16777216)})
		params.SetPrepareParamKind(vector.PrepareParamFloat)
		params.SetPrepareParamType(types.T_float32)
		result := makeResult(1)
		require.NoError(t, comparePreparedJSON(
			[]*vector.Vector{jsonValues, params}, result, proc, 1,
			false, func(c int) bool { return c == 0 }, nil))
		require.Equal(t, []bool{true}, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
	})

	t.Run("concrete type and conversion kind must agree", func(t *testing.T) {
		jsonValues := makeJSON([]any{int64(1)})
		params := makePreparedJSON([]any{int64(1)})
		params.SetPrepareParamKind(vector.PrepareParamFloat)
		params.SetPrepareParamType(types.T_int64)
		err := comparePreparedJSON(
			[]*vector.Vector{jsonValues, params}, makeResult(1), proc, 1,
			false, func(c int) bool { return c == 0 }, nil)
		require.ErrorContains(t, err, "does not match conversion kind")
	})

	t.Run("json null follows boolean coercion for null safe equality", func(t *testing.T) {
		jsonValues := makeJSON([]any{nil})
		params := makePreparedJSON([]any{true})
		params.SetPrepareParamKind(vector.PrepareParamBoolean)
		result := makeResult(1)
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 1, true, func(c int) bool { return c == 0 }, nil))
		require.Equal(t, []bool{false}, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()))
		require.True(t, result.GetResultVector().GetNulls().IsEmpty())
	})

	t.Run("selection masks rows before evaluation", func(t *testing.T) {
		jsonValues := makeJSON([]any{true, true, map[string]any{"unsupported": true}})
		params := makePreparedJSON([]any{true, false, true})
		params.SetPrepareParamKind(vector.PrepareParamBoolean)
		result := makeResult(3)
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, false}}
		require.NoError(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, result, proc, 3, false, func(c int) bool { return c == 0 }, selectList))
		got := result.GetResultVector()
		require.True(t, vector.MustFixedColNoTypeCheck[bool](got)[0])
		require.True(t, got.IsNull(1))
		require.True(t, got.IsNull(2))
	})

	t.Run("unsupported boolean cast returns an error", func(t *testing.T) {
		jsonValues := makeJSON([]any{map[string]any{"unsupported": true}})
		params := makePreparedJSON([]any{true})
		params.SetPrepareParamKind(vector.PrepareParamBoolean)
		require.Error(t, comparePreparedJSON([]*vector.Vector{jsonValues, params}, makeResult(1), proc, 1, false, func(c int) bool { return c == 0 }, nil))
	})

}

func BenchmarkPreparedJSONIntegerComparison(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	const length = 1024

	encode := func(value any) []byte {
		bj, err := bytejson.CreateByteJSON(value)
		require.NoError(b, err)
		data, err := types.EncodeJson(bj)
		require.NoError(b, err)
		return data
	}

	jsonValues := vector.NewVec(types.T_json.ToType())
	defer jsonValues.Free(proc.Mp())
	for row := 0; row < length; row++ {
		require.NoError(b, vector.AppendBytes(
			jsonValues, encode(int64(9007199254740992+row)), false, proc.Mp()))
	}
	param, err := vector.NewConstBytes(
		types.T_json.ToType(), encode(int64(9007199254740993)), length, proc.Mp())
	require.NoError(b, err)
	defer param.Free(proc.Mp())
	param.SetPrepareParamKind(vector.PrepareParamInteger)
	param.SetPrepareParamType(types.T_int64)
	param.SetPreparedJSONComparisonParam()

	result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp()).(*vector.FunctionResult[bool])
	defer result.Free()
	require.NoError(b, result.PreExtendAndReset(length))
	b.ReportAllocs()
	b.ReportMetric(length, "rows/op")
	b.ResetTimer()
	for b.Loop() {
		if err := result.PreExtendAndReset(length); err != nil {
			b.Fatal(err)
		}
		if err := comparePreparedJSON(
			[]*vector.Vector{jsonValues, param}, result, proc, length,
			false, func(c int) bool { return c == 0 }, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONBooleanComparison(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	const length = 1024

	jsonValues := vector.NewVec(types.T_json.ToType())
	defer jsonValues.Free(proc.Mp())
	for row := 0; row < length; row++ {
		value := any(row&1 == 0)
		if row%4 == 3 {
			value = "true"
		}
		bj, err := bytejson.CreateByteJSON(value)
		require.NoError(b, err)
		data, err := types.EncodeJson(bj)
		require.NoError(b, err)
		require.NoError(b, vector.AppendBytes(jsonValues, data, false, proc.Mp()))
	}
	booleanValue, err := vector.NewConstFixed(types.T_bool.ToType(), true, length, proc.Mp())
	require.NoError(b, err)
	defer booleanValue.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp()).(*vector.FunctionResult[bool])
	defer result.Free()
	require.NoError(b, result.PreExtendAndReset(length))

	b.ReportAllocs()
	b.ReportMetric(length, "rows/op")
	b.ResetTimer()
	for b.Loop() {
		if err := result.PreExtendAndReset(length); err != nil {
			b.Fatal(err)
		}
		if err := compareJSONBoolean(
			[]*vector.Vector{jsonValues, booleanValue}, result, proc, length,
			false, func(c int) bool { return c == 0 }, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDatetimeTimestampComparisonPreservesInstantSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = zone

	datetime, err := types.ParseDatetime("2024-11-03 01:30:00", 6)
	require.NoError(t, err)
	secondFoldTimestamp, err := types.ParseTimestamp(time.UTC, "2024-11-03 06:15:00", 6)
	require.NoError(t, err)
	datetimeAsTimestamp := datetime.ToTimestamp(zone)

	tests := []struct {
		name string
		fn   fEvalFn
		want bool
	}{
		{name: "equal", fn: equalFn, want: datetimeAsTimestamp == secondFoldTimestamp},
		{name: "not equal", fn: notEqualFn, want: datetimeAsTimestamp != secondFoldTimestamp},
		{name: "greater", fn: greatThanFn, want: datetimeAsTimestamp > secondFoldTimestamp},
		{name: "greater equal", fn: greatEqualFn, want: datetimeAsTimestamp >= secondFoldTimestamp},
		{name: "less", fn: lessThanFn, want: datetimeAsTimestamp < secondFoldTimestamp},
		{name: "less equal", fn: lessEqualFn, want: datetimeAsTimestamp <= secondFoldTimestamp},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{datetime, datetime}, []bool{false, true}),
				NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{secondFoldTimestamp, secondFoldTimestamp}, nil),
			}
			expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{test.want, false}, []bool{false, true})
			testCase := NewFunctionTestCase(proc, inputs, expect, test.fn)
			ok, info := testCase.Run()
			require.True(t, ok, info)
		})
	}

	t.Run("reversed operands", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{secondFoldTimestamp}, []bool{false}),
			NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{datetime}, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{secondFoldTimestamp > datetimeAsTimestamp}, []bool{false})
		testCase := NewFunctionTestCase(proc, inputs, expect, greatThanFn)
		ok, info := testCase.Run()
		require.True(t, ok, info)
	})

	t.Run("timestamp scale remains the comparison precision", func(t *testing.T) {
		preciseDatetime, err := types.ParseDatetime("2024-01-10 12:00:00.123456", 6)
		require.NoError(t, err)
		millisecondTimestamp := preciseDatetime.ToTimestamp(zone).TruncateToScale(3)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{preciseDatetime}, nil),
			NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(3), []types.Timestamp{millisecondTimestamp}, nil),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil)
		testCase := NewFunctionTestCase(proc, inputs, expect, equalFn)
		ok, info := testCase.Run()
		require.True(t, ok, info)
	})
}

func TestJsonOrderingOperatorsUseExactComparison(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	encode := func(t *testing.T, value string) string {
		t.Helper()
		encoded, err := encodeJsonOrderingParam([]byte(value))
		require.NoError(t, err)
		return string(encoded)
	}

	tests := []struct {
		name  string
		fn    fEvalFn
		left  string
		right string
	}{
		{name: "less adjacent integers", fn: lessThanFn, left: "9007199254740992", right: "9007199254740993"},
		{name: "greater adjacent integers", fn: greatThanFn, left: "9007199254740993", right: "9007199254740992"},
		{name: "less equal precise decimals", fn: lessEqualFn, left: "0.123456789123456788", right: "0.123456789123456789"},
		{name: "greater equal precise decimals", fn: greatEqualFn, left: "0.123456789123456789", right: "0.123456789123456788"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{encode(t, test.left)}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(), []string{encode(t, test.right)}, []bool{false}),
			}
			expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, []bool{false})
			testCase := NewFunctionTestCase(proc, inputs, expect, test.fn)
			ok, info := testCase.Run()
			require.True(t, ok, info)
		})
	}

	t.Run("null propagates", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{encode(t, "0")}, []bool{true}),
			NewFunctionTestInput(types.T_json.ToType(), []string{encode(t, "1")}, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{true})
		testCase := NewFunctionTestCase(proc, inputs, expect, lessThanFn)
		ok, info := testCase.Run()
		require.True(t, ok, info)
	})
}

func TestJSONBinaryEqualityUsesSubtypeAndRawPayload(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	encode := func(t *testing.T, value bytejson.ByteJson) string {
		t.Helper()
		encoded, err := types.EncodeJson(value)
		require.NoError(t, err)
		return string(encoded)
	}
	run := func(t *testing.T, fn fEvalFn, left, right bytejson.ByteJson, want bool) {
		t.Helper()
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{encode(t, left)}, []bool{false}),
			NewFunctionTestInput(types.T_json.ToType(), []string{encode(t, right)}, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{want}, []bool{false})
		testCase := NewFunctionTestCase(proc, inputs, expect, fn)
		ok, info := testCase.Run()
		require.True(t, ok, info)
	}

	legacyBlob := newTypedByteJson(bytejson.TpCodeBlob, "AA==")
	rawBlob := newTypedByteJson(bytejson.TpCodeOpaque, string([]byte{0x00}))
	bit := newTypedByteJson(bytejson.TpCodeBit, string([]byte{0x00}))

	run(t, equalFn, legacyBlob, rawBlob, true)
	run(t, nullSafeEqualFn, legacyBlob, rawBlob, true)
	run(t, notEqualFn, legacyBlob, rawBlob, false)
	run(t, equalFn, bit, rawBlob, false)
	run(t, lessThanFn, bit, rawBlob, true)
}

func TestVecF32EqualityDoesNotDependOnVarlenaStorage(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	negativeZero := float32(math.Copysign(0, -1))
	nan := math.Float32frombits(0x7fc00001)

	tests := []struct {
		name  string
		left  []float32
		right []float32
		want  bool
	}{
		{name: "inline signed zero", left: []float32{1, 0, 3}, right: []float32{1, negativeZero, 3}, want: true},
		{name: "area signed zero", left: []float32{1, 2, 3, 0, 5, 6, 7, 8}, right: []float32{1, 2, 3, negativeZero, 5, 6, 7, 8}, want: true},
		{name: "inline different", left: []float32{1, 0, 3}, right: []float32{1, 2, 3}, want: false},
		{name: "inline nan self", left: []float32{1, nan, 3}, right: []float32{1, nan, 3}, want: false},
		{name: "inline nan versus number", left: []float32{1, nan, 3}, right: []float32{1, 2, 3}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{test.left}, []bool{false}),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{test.right}, []bool{false}),
			}
			for _, fn := range []struct {
				name string
				eval fEvalFn
				want bool
			}{
				{name: "equal", eval: equalFn, want: test.want},
				{name: "null-safe-equal", eval: nullSafeEqualFn, want: test.want},
				{name: "not-equal", eval: notEqualFn, want: !test.want},
			} {
				t.Run(fn.name, func(t *testing.T) {
					expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{fn.want}, []bool{false})
					testCase := NewFunctionTestCase(proc, inputs, expect, fn.eval)
					ok, info := testCase.Run()
					require.True(t, ok, info)
				})
			}
		})
	}
}

func TestVecF64EqualityDoesNotDependOnVarlenaStorage(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	negativeZero := math.Copysign(0, -1)
	nan := math.Float64frombits(0x7ff8000000000001)

	tests := []struct {
		name  string
		left  []float64
		right []float64
		want  bool
	}{
		{name: "inline signed zero", left: []float64{1, 0}, right: []float64{1, negativeZero}, want: true},
		{name: "area signed zero", left: []float64{1, 2, 0, 4}, right: []float64{1, 2, negativeZero, 4}, want: true},
		{name: "inline different", left: []float64{1, 0}, right: []float64{1, 2}, want: false},
		{name: "inline nan self", left: []float64{1, nan}, right: []float64{1, nan}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{test.left}, []bool{false}),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{test.right}, []bool{false}),
			}
			for _, fn := range []struct {
				name string
				eval fEvalFn
				want bool
			}{
				{name: "equal", eval: equalFn, want: test.want},
				{name: "null-safe-equal", eval: nullSafeEqualFn, want: test.want},
				{name: "not-equal", eval: notEqualFn, want: !test.want},
			} {
				t.Run(fn.name, func(t *testing.T) {
					expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{fn.want}, []bool{false})
					testCase := NewFunctionTestCase(proc, inputs, expect, fn.eval)
					ok, info := testCase.Run()
					require.True(t, ok, info)
				})
			}
		})
	}
}

func TestNarrowFloatArrayEqualityUsesElementSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	negativeZero := float32(math.Copysign(0, -1))
	nan := math.Float32frombits(0x7fc00001)

	run := func(t *testing.T, inputs []FunctionTestInput, want bool) {
		t.Helper()
		for _, fn := range []struct {
			name string
			eval fEvalFn
			want bool
		}{
			{name: "equal", eval: equalFn, want: want},
			{name: "null-safe-equal", eval: nullSafeEqualFn, want: want},
			{name: "not-equal", eval: notEqualFn, want: !want},
		} {
			t.Run(fn.name, func(t *testing.T) {
				expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{fn.want}, []bool{false})
				testCase := NewFunctionTestCase(proc, inputs, expect, fn.eval)
				ok, info := testCase.Run()
				require.True(t, ok, info)
			})
		}
	}

	t.Run("bf16 signed zero", func(t *testing.T) {
		run(t, []FunctionTestInput{
			NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{types.Float32ToBF16Slice([]float32{1, 0})}, []bool{false}),
			NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{types.Float32ToBF16Slice([]float32{1, negativeZero})}, []bool{false}),
		}, true)
	})
	t.Run("bf16 nan", func(t *testing.T) {
		value := types.Float32ToBF16Slice([]float32{1, nan})
		run(t, []FunctionTestInput{
			NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{value}, []bool{false}),
			NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{value}, []bool{false}),
		}, false)
	})
	t.Run("float16 signed zero", func(t *testing.T) {
		run(t, []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{types.Float32ToFloat16Slice([]float32{1, 0})}, []bool{false}),
			NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{types.Float32ToFloat16Slice([]float32{1, negativeZero})}, []bool{false}),
		}, true)
	})
	t.Run("float16 nan", func(t *testing.T) {
		value := types.Float32ToFloat16Slice([]float32{1, nan})
		run(t, []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{value}, []bool{false}),
			NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{value}, []bool{false}),
		}, false)
	})
}

func TestOperatorOpBitAndUint64Fn(t *testing.T) {
	// 1 & 2 = 0
	// max uint64 & 2 = 2
	// null & 2 = null
	tc := tcTemp{
		info: "& test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{1, math.MaxUint64, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{0, 2, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitAndUint64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitOrUint64Fn(t *testing.T) {
	// 1 | 2 = 3
	// max uint64 | 2 = max uint64
	// null | 2 = null
	tc := tcTemp{
		info: "| test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{1, math.MaxUint64, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{3, math.MaxUint64, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitOrUint64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitXorUint64Fn(t *testing.T) {
	// 1 ^ 2 = 3
	// max uint64 ^ 2 = max uint64 - 2
	// null ^ 2 = null
	tc := tcTemp{
		info: "^ test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{1, math.MaxUint64, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{3, math.MaxUint64 - 2, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitXorUint64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitRightShiftUint64Fn(t *testing.T) {
	// 1024 >> 2 = 256
	// (max uint64 - 4) >> 2 = 4611686018427387902
	// 2 >> max uint64 = 0
	// null >> 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{1024, math.MaxUint64 - 4, 2, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{2, 2, math.MaxUint64, 2}, []bool{false, false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{256, (math.MaxUint64 - 4) >> 2, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftRightUint64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitLeftShiftUint64Fn(t *testing.T) {
	// 1 << 2 = 4
	// max uint64 << 2 = max uint64 - 3
	// 2 << max uint64 = 0
	// null << 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{1, math.MaxUint64, 2, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{2, 2, math.MaxUint64, 2}, []bool{false, false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{4, math.MaxUint64 - 3, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftLeftUint64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFn(t *testing.T) {
	// 1 <=> 1 = true
	// 1 <=> 0 = false
	// 1 <=> null = false
	// null <=> 1 = false
	// null <=> null = true
	tcInt64 := tcTemp{
		info: "<=> int64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTCInt64 := NewFunctionTestCase(proc,
		tcInt64.inputs, tcInt64.expect, nullSafeEqualFn)
	s, info := fcTCInt64.Run()
	require.True(t, s, info, tcInt64.info)

	// Float64 Test
	tcFloat := tcTemp{
		info: "<=> float64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1.1, 1.1, 1.1, 0.0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1.1, 0.0, 0.0, 0.0}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCFloat := NewFunctionTestCase(proc,
		tcFloat.inputs, tcFloat.expect, nullSafeEqualFn)
	s, info = fcTCFloat.Run()
	require.True(t, s, info, tcFloat.info)

	// Varchar Test
	tcStr := tcTemp{
		info: "<=> varchar test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "a", "a", ""}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "b", "", ""}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCStr := NewFunctionTestCase(proc,
		tcStr.inputs, tcStr.expect, nullSafeEqualFn)
	s, info = fcTCStr.Run()
	require.True(t, s, info, tcStr.info)

	// Bool Test
	tcBool := tcTemp{
		info: "<=> bool test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, true, true, false}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, false, false, false}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCBool := NewFunctionTestCase(proc,
		tcBool.inputs, tcBool.expect, nullSafeEqualFn)
	s, info = fcTCBool.Run()
	require.True(t, s, info, tcBool.info)

	// Date Test
	d1, _ := types.ParseDateCast("2022-01-01")
	d2, _ := types.ParseDateCast("2022-01-02")
	tcDate := tcTemp{
		info: "<=> date test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{d1, d1, d1, d2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{d1, d2, d2, d2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDate := NewFunctionTestCase(proc,
		tcDate.inputs, tcDate.expect, nullSafeEqualFn)
	s, info = fcTCDate.Run()
	require.True(t, s, info, tcDate.info)

	// Time Test
	t1, _ := types.ParseTime("12:00:00", 0)
	t2, _ := types.ParseTime("13:00:00", 0)
	tcTime := tcTemp{
		info: "<=> time test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_time.ToType(), []types.Time{t1, t1, t1, t2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_time.ToType(), []types.Time{t1, t2, t2, t2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCTime := NewFunctionTestCase(proc,
		tcTime.inputs, tcTime.expect, nullSafeEqualFn)
	s, info = fcTCTime.Run()
	require.True(t, s, info, tcTime.info)

	// Timestamp Test
	ts1, _ := types.ParseTimestamp(time.UTC, "2022-01-01 12:00:00", 6)
	ts2, _ := types.ParseTimestamp(time.UTC, "2022-01-01 13:00:00", 6)
	tcTimestamp := tcTemp{
		info: "<=> timestamp test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{ts1, ts1, ts1, ts2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{ts1, ts2, ts2, ts2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCTimestamp := NewFunctionTestCase(proc,
		tcTimestamp.inputs, tcTimestamp.expect, nullSafeEqualFn)
	s, info = fcTCTimestamp.Run()
	require.True(t, s, info, tcTimestamp.info)

	// Decimal64 Test
	dec64_1, _ := types.ParseDecimal64("1.1", 10, 2)
	dec64_2, _ := types.ParseDecimal64("2.2", 10, 2)
	tcDecimal64 := tcTemp{
		info: "<=> decimal64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_decimal64.ToType(), []types.Decimal64{dec64_1, dec64_1, dec64_1, dec64_2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_decimal64.ToType(), []types.Decimal64{dec64_1, dec64_2, dec64_2, dec64_2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDecimal64 := NewFunctionTestCase(proc,
		tcDecimal64.inputs, tcDecimal64.expect, nullSafeEqualFn)
	s, info = fcTCDecimal64.Run()
	require.True(t, s, info, tcDecimal64.info)

	// Decimal128 Test
	dec128_1, _ := types.ParseDecimal128("1.1", 30, 2)
	dec128_2, _ := types.ParseDecimal128("2.2", 30, 2)
	tcDecimal128 := tcTemp{
		info: "<=> decimal128 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{dec128_1, dec128_1, dec128_1, dec128_2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{dec128_1, dec128_2, dec128_2, dec128_2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDecimal128 := NewFunctionTestCase(proc,
		tcDecimal128.inputs, tcDecimal128.expect, nullSafeEqualFn)
	s, info = fcTCDecimal128.Run()
	require.True(t, s, info, tcDecimal128.info)

	// UUID Test
	uuid1, _ := types.ParseUuid("00000000-0000-0000-0000-000000000001")
	uuid2, _ := types.ParseUuid("00000000-0000-0000-0000-000000000002")
	tcUuid := tcTemp{
		info: "<=> uuid test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uuid.ToType(), []types.Uuid{uuid1, uuid1, uuid1, uuid2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_uuid.ToType(), []types.Uuid{uuid1, uuid2, uuid2, uuid2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCUuid := NewFunctionTestCase(proc,
		tcUuid.inputs, tcUuid.expect, nullSafeEqualFn)
	s, info = fcTCUuid.Run()
	require.True(t, s, info, tcUuid.info)

	// Int8 Test
	tcInt8 := tcTemp{
		info: "<=> int8 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int8.ToType(), []int8{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int8.ToType(), []int8{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCInt8 := NewFunctionTestCase(proc, tcInt8.inputs, tcInt8.expect, nullSafeEqualFn)
	s, info = fcTCInt8.Run()
	require.True(t, s, info, tcInt8.info)

	// Int16 Test
	tcInt16 := tcTemp{
		info: "<=> int16 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int16.ToType(), []int16{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int16.ToType(), []int16{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCInt16 := NewFunctionTestCase(proc, tcInt16.inputs, tcInt16.expect, nullSafeEqualFn)
	s, info = fcTCInt16.Run()
	require.True(t, s, info, tcInt16.info)

	// Int32 Test
	tcInt32 := tcTemp{
		info: "<=> int32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int32.ToType(), []int32{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int32.ToType(), []int32{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCInt32 := NewFunctionTestCase(proc, tcInt32.inputs, tcInt32.expect, nullSafeEqualFn)
	s, info = fcTCInt32.Run()
	require.True(t, s, info, tcInt32.info)

	// Uint8 Test
	tcUint8 := tcTemp{
		info: "<=> uint8 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint8.ToType(), []uint8{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint8.ToType(), []uint8{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint8 := NewFunctionTestCase(proc, tcUint8.inputs, tcUint8.expect, nullSafeEqualFn)
	s, info = fcTCUint8.Run()
	require.True(t, s, info, tcUint8.info)

	// Uint16 Test
	tcUint16 := tcTemp{
		info: "<=> uint16 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint16.ToType(), []uint16{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint16.ToType(), []uint16{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint16 := NewFunctionTestCase(proc, tcUint16.inputs, tcUint16.expect, nullSafeEqualFn)
	s, info = fcTCUint16.Run()
	require.True(t, s, info, tcUint16.info)

	// Uint32 Test
	tcUint32 := tcTemp{
		info: "<=> uint32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint32 := NewFunctionTestCase(proc, tcUint32.inputs, tcUint32.expect, nullSafeEqualFn)
	s, info = fcTCUint32.Run()
	require.True(t, s, info, tcUint32.info)

	// Uint64 Test
	tcUint64 := tcTemp{
		info: "<=> uint64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCUint64 := NewFunctionTestCase(proc, tcUint64.inputs, tcUint64.expect, nullSafeEqualFn)
	s, info = fcTCUint64.Run()
	require.True(t, s, info, tcUint64.info)

	// Float32 Test
	tcFloat32 := tcTemp{
		info: "<=> float32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float32.ToType(), []float32{1.1, 1.1, 1.1, 0.0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_float32.ToType(), []float32{1.1, 0.0, 0.0, 0.0}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCFloat32 := NewFunctionTestCase(proc, tcFloat32.inputs, tcFloat32.expect, nullSafeEqualFn)
	s, info = fcTCFloat32.Run()
	require.True(t, s, info, tcFloat32.info)

	// Enum Test
	tcEnum := tcTemp{
		info: "<=> enum test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCEnum := NewFunctionTestCase(proc, tcEnum.inputs, tcEnum.expect, nullSafeEqualFn)
	s, info = fcTCEnum.Run()
	require.True(t, s, info, tcEnum.info)

	// Datetime Test
	dt1, _ := types.ParseDatetime("2022-01-01 12:00:00", 6)
	dt2, _ := types.ParseDatetime("2022-01-01 13:00:00", 6)
	tcDatetime := tcTemp{
		info: "<=> datetime test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{dt1, dt1, dt1, dt2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{dt1, dt2, dt2, dt2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCDatetime := NewFunctionTestCase(proc, tcDatetime.inputs, tcDatetime.expect, nullSafeEqualFn)
	s, info = fcTCDatetime.Run()
	require.True(t, s, info, tcDatetime.info)

	// Year Test
	y1 := types.MoYear(2022)
	y2 := types.MoYear(2023)
	tcYear := tcTemp{
		info: "<=> year test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{y1, y1, y1, y2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{y1, y2, y2, y2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCYear := NewFunctionTestCase(proc, tcYear.inputs, tcYear.expect, nullSafeEqualFn)
	s, info = fcTCYear.Run()
	require.True(t, s, info, tcYear.info)

	// Float32 with Scale Test
	f32Type := types.T_float32.ToType()
	f32Type.Scale = 2
	tcFloat32Scale := tcTemp{
		info: "<=> float32 scale test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(f32Type, []float32{1.111, 1.114, 1.115}, []bool{false, false, false}),
			NewFunctionTestInput(f32Type, []float32{1.11, 1.11, 1.12}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true, true}, []bool{false, false, false}),
	}
	fcTCFloat32Scale := NewFunctionTestCase(proc, tcFloat32Scale.inputs, tcFloat32Scale.expect, nullSafeEqualFn)
	s, info = fcTCFloat32Scale.Run()
	require.True(t, s, info, tcFloat32Scale.info)

	// JSON Test
	tcJson := tcTemp{
		info: "<=> json test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{`{"a":1}`, `{"a":1}`}, []bool{false, true}),
			NewFunctionTestInput(types.T_json.ToType(), []string{`{"a":1}`, `{"a":1}`}, []bool{false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true}, []bool{false, false}),
	}
	fcTCJson := NewFunctionTestCase(proc, tcJson.inputs, tcJson.expect, nullSafeEqualFn)
	s, info = fcTCJson.Run()
	require.True(t, s, info, tcJson.info)

	// Bit Test
	tcBit := tcTemp{
		info: "<=> bit test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bit.ToType(), []uint64{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_bit.ToType(), []uint64{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}
	fcTCBit := NewFunctionTestCase(proc, tcBit.inputs, tcBit.expect, nullSafeEqualFn)
	s, info = fcTCBit.Run()
	require.True(t, s, info, tcBit.info)

	// Rowid Test
	rid1 := types.Rowid([24]byte{1})
	rid2 := types.Rowid([24]byte{2})
	tcRowid := tcTemp{
		info: "<=> rowid test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_Rowid.ToType(), []types.Rowid{rid1, rid1, rid1, rid2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_Rowid.ToType(), []types.Rowid{rid1, rid2, rid2, rid2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCRowid := NewFunctionTestCase(proc, tcRowid.inputs, tcRowid.expect, nullSafeEqualFn)
	s, info = fcTCRowid.Run()
	require.True(t, s, info, tcRowid.info)

	// Array Float32 Test
	arr1 := []float32{1.0, 2.0}
	arr2 := []float32{3.0, 4.0}
	tcArrF32 := tcTemp{
		info: "<=> array float32 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{arr1, arr1, arr1, arr2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{arr1, arr2, arr2, arr2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCArrF32 := NewFunctionTestCase(proc, tcArrF32.inputs, tcArrF32.expect, nullSafeEqualFn)
	s, info = fcTCArrF32.Run()
	require.True(t, s, info, tcArrF32.info)

	// Array Float64 Test
	arrD1 := []float64{1.0, 2.0}
	arrD2 := []float64{3.0, 4.0}
	tcArrF64 := tcTemp{
		info: "<=> array float64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{arrD1, arrD1, arrD1, arrD2}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{arrD1, arrD2, arrD2, arrD2}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCArrF64 := NewFunctionTestCase(proc, tcArrF64.inputs, tcArrF64.expect, nullSafeEqualFn)
	s, info = fcTCArrF64.Run()
	require.True(t, s, info, tcArrF64.info)

	// Narrow array types (bf16/f16/int8/uint8) — same <=> equality pattern.
	{
		bf1 := types.Float32ToBF16Slice([]float32{1, 2})
		bf2 := types.Float32ToBF16Slice([]float32{3, 4})
		tc := tcTemp{
			info: "<=> array bf16 test",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{bf1, bf1, bf1, bf2}, []bool{false, false, false, true}),
				NewFunctionTestInput(types.T_array_bf16.ToType(), [][]types.BF16{bf1, bf2, bf2, bf2}, []bool{false, false, true, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true}, []bool{false, false, false, false}),
		}
		fc := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
		s, info = fc.Run()
		require.True(t, s, info, tc.info)
	}
	{
		f1 := types.Float32ToFloat16Slice([]float32{1, 2})
		f2 := types.Float32ToFloat16Slice([]float32{3, 4})
		tc := tcTemp{
			info: "<=> array f16 test",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{f1, f1, f1, f2}, []bool{false, false, false, true}),
				NewFunctionTestInput(types.T_array_float16.ToType(), [][]types.Float16{f1, f2, f2, f2}, []bool{false, false, true, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true}, []bool{false, false, false, false}),
		}
		fc := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
		s, info = fc.Run()
		require.True(t, s, info, tc.info)
	}
	{
		i1 := []int8{1, 2}
		i2 := []int8{3, 4}
		tc := tcTemp{
			info: "<=> array int8 test",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_int8.ToType(), [][]int8{i1, i1, i1, i2}, []bool{false, false, false, true}),
				NewFunctionTestInput(types.T_array_int8.ToType(), [][]int8{i1, i2, i2, i2}, []bool{false, false, true, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true}, []bool{false, false, false, false}),
		}
		fc := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
		s, info = fc.Run()
		require.True(t, s, info, tc.info)
	}
	{
		u1 := []uint8{1, 2}
		u2 := []uint8{3, 4}
		tc := tcTemp{
			info: "<=> array uint8 test",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_uint8.ToType(), [][]uint8{u1, u1, u1, u2}, []bool{false, false, false, true}),
				NewFunctionTestInput(types.T_array_uint8.ToType(), [][]uint8{u1, u2, u2, u2}, []bool{false, false, true, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true}, []bool{false, false, false, false}),
		}
		fc := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
		s, info = fc.Run()
		require.True(t, s, info, tc.info)
	}
}
