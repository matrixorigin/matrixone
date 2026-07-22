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
	"context"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func mustParseJSONOverlap(t *testing.T, text string) bytejson.ByteJson {
	t.Helper()
	value, err := types.ParseSliceToByteJson([]byte(text))
	require.NoError(t, err)
	return value
}

func TestJSONOverlapKernel(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{name: "arrays match top-level element", left: `[1,3,5,7]`, right: `[2,5,7]`, want: true},
		{name: "different length arrays do not overlap", left: `[1,2,3]`, right: `[4,5]`, want: false},
		{name: "different length arrays overlap", left: `[1,2]`, right: `[2,4,5]`, want: true},
		{name: "duplicate array elements preserve overlap", left: `[1,1,2]`, right: `[2,2]`, want: true},
		{name: "duplicate array elements preserve miss", left: `[1,1]`, right: `[2,2]`, want: false},
		{name: "nested arrays are complete values", left: `[[1,2],[3,4],5]`, right: `[1,[2,3],[4,5]]`, want: false},
		{name: "objects share key and equal value", left: `{"a":1,"d":10}`, right: `{"d":10,"x":1}`, want: true},
		{name: "object json null does not equal missing key", left: `{"a":1,"b":null}`, right: `{"a":2,"c":3}`, want: false},
		{name: "object json null does not equal non-null value", left: `{"a":1,"b":2}`, right: `{"a":null,"c":3}`, want: false},
		{name: "object values use equality not containment", left: `{"a":{"x":1,"y":2}}`, right: `{"a":{"x":1}}`, want: false},
		{name: "array autowraps object", left: `[{"a":1}]`, right: `{"a":1}`, want: true},
		{name: "array autowraps empty object", left: `[{}]`, right: `{}`, want: true},
		{name: "empty object differs from non-empty object", left: `[{}]`, right: `{"a":1,"b":2}`, want: false},
		{name: "array object and scalar do not overlap", left: `[{}]`, right: `1`, want: false},
		{name: "object and scalar do not overlap", left: `{"a":1}`, right: `1`, want: false},
		{name: "empty arrays", left: `[]`, right: `[]`, want: false},
		{name: "empty objects", left: `{}`, right: `{}`, want: false},
		{name: "boolean scalars differ", left: `true`, right: `false`, want: false},
		{name: "numeric scalar and object do not overlap", left: `123`, right: `{"value":123}`, want: false},
		{name: "json null", left: `null`, right: `null`, want: true},
		{name: "array and json null", left: `[null]`, right: `null`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := mustParseJSONOverlap(t, tt.left)
			right := mustParseJSONOverlap(t, tt.right)
			var workspace jsonOverlapWorkspace
			require.Equal(t, tt.want, workspace.overlaps(left, right))
			require.Equal(t, tt.want, workspace.overlaps(right, left))
			workspace.clear()
		})
	}
}

func TestJSONOverlapComparatorNumeric(t *testing.T) {
	tests := []struct {
		left  string
		right string
		want  int
	}{
		{left: `1`, right: `1.0`, want: 0},
		{left: `1`, right: `1.000000009`, want: -1},
		{left: `0`, right: `-0.0`, want: 0},
		{left: `9007199254740993`, right: `9007199254740992.0`, want: 1},
		{left: `18446744073709551615`, right: `18446744073709551615.0`, want: -1},
	}

	for _, tt := range tests {
		left := mustParseJSONOverlap(t, tt.left)
		right := mustParseJSONOverlap(t, tt.right)
		cmp := compareJSONOverlapExact(left, right)
		require.Equal(t, tt.want, cmp, "%s vs %s", tt.left, tt.right)
		require.Equal(t, -tt.want, compareJSONOverlapExact(right, left), "%s vs %s", tt.right, tt.left)
	}
}

func TestJSONOverlapComparatorDecimalAndDouble(t *testing.T) {
	tests := []struct {
		name  string
		left  bytejson.ByteJson
		right bytejson.ByteJson
		want  int
	}{
		{
			name:  "scientific double equals decimal",
			left:  mustParseJSONOverlap(t, `1e20`),
			right: newTypedByteJson(bytejson.TpCodeDecimal, "100000000000000000000"),
			want:  0,
		},
		{
			name:  "nearby decimal remains distinct",
			left:  mustParseJSONOverlap(t, `1e20`),
			right: newTypedByteJson(bytejson.TpCodeDecimal, "100000000000000000001"),
			want:  -1,
		},
		{
			name:  "double outside decimal range orders by magnitude",
			left:  mustParseJSONOverlap(t, `1e100`),
			right: newTypedByteJson(bytejson.TpCodeDecimal, strings.Repeat("9", 76)),
			want:  1,
		},
		{
			name:  "tiny scientific double retains its scale",
			left:  mustParseJSONOverlap(t, `1e-300`),
			right: newTypedByteJson(bytejson.TpCodeDecimal, "1e-300"),
			want:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, compareJSONOverlapExact(tt.left, tt.right))
			require.Equal(t, -tt.want, compareJSONOverlapExact(tt.right, tt.left))
		})
	}
}

func TestJSONOverlapComparatorLargeInternalDecimalsDoNotPanicOrEqualZero(t *testing.T) {
	zero := mustParseJSONOverlap(t, `0`)
	positive := newTypedByteJson(bytejson.TpCodeDecimal, "1e100")
	negative := newTypedByteJson(bytejson.TpCodeDecimal, "-1e100")
	larger := newTypedByteJson(bytejson.TpCodeDecimal, "1e200")

	require.NotPanics(t, func() {
		require.Greater(t, compareJSONOverlapExact(positive, zero), 0)
		require.Less(t, compareJSONOverlapExact(negative, zero), 0)
		require.Less(t, compareJSONOverlapExact(positive, larger), 0)
	})
}

func TestJSONOverlapComparatorFallbackPreservesLongDecimalAndExtremeScaleOrder(t *testing.T) {
	longPrefix := strings.Repeat("1", 76)
	left := newTypedByteJson(bytejson.TpCodeDecimal, "0."+longPrefix+"1")
	right := newTypedByteJson(bytejson.TpCodeDecimal, "0."+longPrefix+"2")
	require.Less(t, compareJSONOverlapExact(left, right), 0)

	tooWideInteger := newTypedByteJson(bytejson.TpCodeDecimal, "1"+strings.Repeat("0", 76))
	largestFastInteger := newTypedByteJson(bytejson.TpCodeDecimal, strings.Repeat("9", 76))
	require.Greater(t, compareJSONOverlapExact(tooWideInteger, largestFastInteger), 0)

	extremeZero := newTypedByteJson(bytejson.TpCodeDecimal, "0e-2147483647")
	one := mustParseJSONOverlap(t, `1`)
	require.Less(t, compareJSONOverlapExact(extremeZero, one), 0)
}

func TestJSONOverlapComparatorFallbackNormalizesEquivalentNumbers(t *testing.T) {
	tests := []struct {
		left  bytejson.ByteJson
		right bytejson.ByteJson
	}{
		{
			left:  newTypedByteJson(bytejson.TpCodeDecimal, "1e100"),
			right: newTypedByteJson(bytejson.TpCodeDecimal, "10e99"),
		},
		{
			left:  newTypedByteJson(bytejson.TpCodeDecimal, "1E0"),
			right: newTypedByteJson(bytejson.TpCodeDecimal, "1.0"),
		},
		{
			left:  newTypedByteJson(bytejson.TpCodeDecimal, "-0e999"),
			right: mustParseJSONOverlap(t, `0`),
		},
	}

	for _, tt := range tests {
		require.Zero(t, compareJSONOverlapExact(tt.left, tt.right))
		require.Zero(t, compareJSONOverlapExact(tt.right, tt.left))
	}
}

func TestJSONOverlapComparatorFallbackPreservesNonFiniteFloatOrder(t *testing.T) {
	negativeInfinity, err := bytejson.CreateByteJSON(math.Inf(-1))
	require.NoError(t, err)
	positiveInfinity, err := bytejson.CreateByteJSON(math.Inf(1))
	require.NoError(t, err)
	notANumber, err := bytejson.CreateByteJSON(math.NaN())
	require.NoError(t, err)
	finite := newTypedByteJson(bytejson.TpCodeDecimal, "1")
	fallback := newTypedByteJson(bytejson.TpCodeDecimal, "1e100")

	require.Less(t, compareJSONOverlapExact(negativeInfinity, finite), 0)
	require.Less(t, compareJSONOverlapExact(finite, fallback), 0)
	require.Less(t, compareJSONOverlapExact(negativeInfinity, fallback), 0)
	require.Greater(t, compareJSONOverlapExact(positiveInfinity, fallback), 0)
	require.Greater(t, compareJSONOverlapExact(notANumber, fallback), 0)
}

func TestJSONOverlapComparatorTemporalNormalizesScale(t *testing.T) {
	left := newTypedByteJson(bytejson.TpCodeTime, "12:00:00")
	right := newTypedByteJson(bytejson.TpCodeTime, "12:00:00.000000")
	require.Zero(t, compareJSONOverlapExact(left, right))

	left = newTypedByteJson(bytejson.TpCodeDatetime, "2026-07-20 12:00:00")
	right = newTypedByteJson(bytejson.TpCodeDatetime, "2026-07-20 12:00:00.000000")
	require.Zero(t, compareJSONOverlapExact(left, right))
}

func TestJSONOverlapComparatorIsAStableTotalOrder(t *testing.T) {
	values := []bytejson.ByteJson{
		mustParseJSONOverlap(t, `null`),
		mustParseJSONOverlap(t, `false`),
		mustParseJSONOverlap(t, `true`),
		mustParseJSONOverlap(t, `-1`),
		mustParseJSONOverlap(t, `0`),
		mustParseJSONOverlap(t, `-0.0`),
		mustParseJSONOverlap(t, `1`),
		mustParseJSONOverlap(t, `1.0`),
		mustParseJSONOverlap(t, `1.000000009`),
		mustParseJSONOverlap(t, `9007199254740993`),
		mustParseJSONOverlap(t, `9007199254740992.0`),
		mustParseJSONOverlap(t, `9223372036854775808.0`),
		mustParseJSONOverlap(t, `9223372036854776000.0`),
		mustParseJSONOverlap(t, `9223372036854775808`),
		newTypedByteJson(bytejson.TpCodeDecimal, "9223372036854775808"),
		newTypedByteJson(bytejson.TpCodeDecimal, "9223372036854776000"),
		mustParseJSONOverlap(t, `"x"`),
		mustParseJSONOverlap(t, `[1,{"a":2}]`),
		mustParseJSONOverlap(t, `{"a":[1,2]}`),
		newTypedByteJson(bytejson.TpCodeTime, "12:00:00.000000"),
		newTypedByteJson(bytejson.TpCodeDatetime, "2026-07-20 12:00:00"),
	}

	for i := range values {
		for j := range values {
			leftRight := normalizeJSONOverlapCompare(compareJSONOverlapExact(values[i], values[j]))
			rightLeft := normalizeJSONOverlapCompare(compareJSONOverlapExact(values[j], values[i]))
			require.Equal(t, -leftRight, rightLeft, "antisymmetry at %d,%d", i, j)
			for k := range values {
				if leftRight <= 0 && compareJSONOverlapExact(values[j], values[k]) <= 0 {
					require.LessOrEqual(t, compareJSONOverlapExact(values[i], values[k]), 0,
						"transitivity at %d,%d,%d", i, j, k)
				}
			}
		}
	}
}

func normalizeJSONOverlapCompare(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}

func TestJSONOverlapKernelUsesIndexedPath(t *testing.T) {
	left := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 40))
	right := mustParseJSONOverlap(t, jsonOverlapIntegerArray(39, 40))
	var workspace jsonOverlapWorkspace
	require.True(t, workspace.overlaps(left, right))
	require.Len(t, workspace.indexes, 40)
}

func TestJSONOverlapPreparedConstArrayIndexIsBuiltOnce(t *testing.T) {
	array := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 512))
	probe := mustParseJSONOverlap(t, `1000`)
	prepared := jsonOverlapPreparedArray{}
	constView := jsonOverlapValueView{document: array, prepared: &prepared}
	probeView := jsonOverlapValueView{document: probe}
	var workspace jsonOverlapWorkspace

	require.False(t, workspace.overlapsViews(constView, probeView, 8192))
	require.True(t, prepared.ready)
	require.Len(t, prepared.indexes, 512)
	require.Nil(t, prepared.numericKeys)
	firstIndexAddress := &prepared.indexes[0]

	require.False(t, workspace.overlapsViews(constView, probeView, 8192))
	require.Same(t, firstIndexAddress, &prepared.indexes[0])
}

func TestJSONOverlapPreparedConstArrayStaysLinearForOneEffectiveScalarRow(t *testing.T) {
	array := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 512))
	probe := mustParseJSONOverlap(t, `1000`)
	prepared := jsonOverlapPreparedArray{}
	var workspace jsonOverlapWorkspace

	require.False(t, workspace.overlapsViews(
		jsonOverlapValueView{document: array, prepared: &prepared},
		jsonOverlapValueView{document: probe},
		1,
	))
	require.False(t, prepared.ready)
}

func TestJSONOverlapPreparedConstArraySurvivesArrayLengthSwap(t *testing.T) {
	largeArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 512))
	probeArray := mustParseJSONOverlap(t, `[1000]`)

	for _, tt := range []struct {
		name      string
		constLeft bool
	}{
		{name: "const left", constLeft: true},
		{name: "const right", constLeft: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			prepared := jsonOverlapPreparedArray{}
			constView := jsonOverlapValueView{document: largeArray, prepared: &prepared}
			probeView := jsonOverlapValueView{document: probeArray}
			left, right := constView, probeView
			if !tt.constLeft {
				left, right = right, left
			}
			var workspace jsonOverlapWorkspace

			require.False(t, workspace.overlapsViews(left, right, 8192))
			require.True(t, prepared.ready)
			require.Len(t, prepared.indexes, 512)
			firstIndexAddress := &prepared.indexes[0]

			require.False(t, workspace.overlapsViews(left, right, 8192))
			require.Same(t, firstIndexAddress, &prepared.indexes[0])
		})
	}
}

func TestJSONOverlapPreparedConstArraySkipsPreparationForOneEvaluableArrayRow(t *testing.T) {
	constArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 40))
	probeArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(100, 40))
	prepared := jsonOverlapPreparedArray{}
	var workspace jsonOverlapWorkspace

	require.False(t, workspace.overlapsViews(
		jsonOverlapValueView{document: constArray, prepared: &prepared},
		jsonOverlapValueView{document: probeArray},
		1,
	))
	require.False(t, prepared.ready)
}

func TestJSONOverlapPreparedConstArraySelectsLowestCostCandidate(t *testing.T) {
	largeArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 512))
	smallArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(1000, 2))
	largePrepared := jsonOverlapPreparedArray{}
	smallPrepared := jsonOverlapPreparedArray{}
	var workspace jsonOverlapWorkspace

	require.False(t, workspace.overlapsViews(
		jsonOverlapValueView{document: largeArray, prepared: &largePrepared},
		jsonOverlapValueView{document: smallArray, prepared: &smallPrepared},
		8192,
	))
	require.True(t, largePrepared.ready)
	require.False(t, smallPrepared.ready)
}

func TestJSONOverlapPreparedConstArrayCostTieUsesSmallerConst(t *testing.T) {
	smallArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 3))
	largeArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(100, 78))
	smallPrepared := jsonOverlapPreparedArray{}
	largePrepared := jsonOverlapPreparedArray{}
	var workspace jsonOverlapWorkspace

	require.False(t, workspace.overlapsViews(
		jsonOverlapValueView{document: smallArray, prepared: &smallPrepared},
		jsonOverlapValueView{document: largeArray, prepared: &largePrepared},
		4,
	))
	require.True(t, smallPrepared.ready)
	require.False(t, largePrepared.ready)
}

func TestJSONOverlapArrayBatchCostsSaturate(t *testing.T) {
	require.Equal(t, int64(936), jsonOverlapGenericArrayBatchCost(3, 78, 4))
	require.Equal(t, int64(630), jsonOverlapPreparedArrayBatchCost(3, 78, 4))
	require.Equal(t, int64(630), jsonOverlapPreparedArrayBatchCost(78, 3, 4))

	require.Equal(t, int64(math.MaxInt64), jsonOverlapSaturatingMul(math.MaxInt64, 2))
	require.Equal(t, int64(math.MaxInt64), jsonOverlapSaturatingAdd(math.MaxInt64-1, 2))
	require.Equal(t, int64(math.MaxInt64),
		jsonOverlapGenericArrayBatchCost(math.MaxInt, math.MaxInt, math.MaxInt))
	require.Equal(t, int64(math.MaxInt64),
		jsonOverlapPreparedArrayBatchCost(math.MaxInt, math.MaxInt, math.MaxInt))
}

func TestJSONOverlapPreparedConstArrayHandlesMultiElementProbe(t *testing.T) {
	constArray := mustParseJSONOverlap(t, jsonOverlapIntegerArray(0, 512))
	prepared := jsonOverlapPreparedArray{}
	constView := jsonOverlapValueView{document: constArray, prepared: &prepared}
	var workspace jsonOverlapWorkspace

	require.False(t, workspace.overlapsViews(
		constView,
		jsonOverlapValueView{document: mustParseJSONOverlap(t, `[700,700]`)},
		8192,
	))
	require.True(t, workspace.overlapsViews(
		constView,
		jsonOverlapValueView{document: mustParseJSONOverlap(t, `[700,511,511]`)},
		8192,
	))
	require.True(t, prepared.ready)
}

func TestJSONOverlapPreparedConstArrayCachesFallbackNumericKeys(t *testing.T) {
	array := makeJSONOverlapArray(t, []bytejson.ByteJson{
		newTypedByteJson(bytejson.TpCodeDecimal, "1e100"),
		newTypedByteJson(bytejson.TpCodeDecimal, "1e200"),
	})
	prepared := jsonOverlapPreparedArray{}
	prepared.ensure(array)

	require.Len(t, prepared.numericKeys, 2)
	for _, key := range prepared.numericKeys {
		require.Equal(t, jsonOverlapNumericKeyValid, key.state)
	}

	probe := newTypedByteJson(bytejson.TpCodeDecimal, "10e99")
	var workspace jsonOverlapWorkspace
	require.True(t, workspace.overlapsViews(
		jsonOverlapValueView{document: array, prepared: &prepared},
		jsonOverlapValueView{document: probe},
		8192,
	))
}

func TestJSONOverlapPreparedConstArrayOrdersMixedFastAndFallbackDecimals(t *testing.T) {
	array := makeJSONOverlapArray(t, []bytejson.ByteJson{
		newTypedByteJson(bytejson.TpCodeDecimal, "-1e100"),
		newTypedByteJson(bytejson.TpCodeDecimal, "1E0"),
		newTypedByteJson(bytejson.TpCodeDecimal, strings.Repeat("9", 76)),
		newTypedByteJson(bytejson.TpCodeDecimal, "1"+strings.Repeat("0", 76)),
	})
	prepared := jsonOverlapPreparedArray{}
	prepared.ensure(array)

	for _, probe := range []bytejson.ByteJson{
		newTypedByteJson(bytejson.TpCodeDecimal, "-1e100"),
		mustParseJSONOverlap(t, `1`),
		newTypedByteJson(bytejson.TpCodeDecimal, strings.Repeat("9", 76)),
		newTypedByteJson(bytejson.TpCodeDecimal, "1e76"),
	} {
		require.True(t, jsonOverlapPreparedArrayContains(array, &prepared, probe))
	}
}

func TestJSONOverlapPreparedConstArrayOrdersNonFiniteFloatAndFallbackDecimal(t *testing.T) {
	negativeInfinity, err := bytejson.CreateByteJSON(math.Inf(-1))
	require.NoError(t, err)
	positiveInfinity, err := bytejson.CreateByteJSON(math.Inf(1))
	require.NoError(t, err)
	fallback := newTypedByteJson(bytejson.TpCodeDecimal, "1e100")
	array := makeJSONOverlapArray(t, []bytejson.ByteJson{
		positiveInfinity,
		fallback,
		negativeInfinity,
		mustParseJSONOverlap(t, `1`),
	})
	prepared := jsonOverlapPreparedArray{}
	prepared.ensure(array)

	for _, probe := range []bytejson.ByteJson{negativeInfinity, mustParseJSONOverlap(t, `1`), fallback, positiveInfinity} {
		require.True(t, jsonOverlapPreparedArrayContains(array, &prepared, probe))
	}
}

func TestJSONOverlapIndexedArraysMatchNaiveComparison(t *testing.T) {
	pool := []bytejson.ByteJson{
		mustParseJSONOverlap(t, `null`),
		mustParseJSONOverlap(t, `false`),
		mustParseJSONOverlap(t, `1`),
		mustParseJSONOverlap(t, `1.000000009`),
		mustParseJSONOverlap(t, `9223372036854775808`),
		mustParseJSONOverlap(t, `9223372036854775808.0`),
		newTypedByteJson(bytejson.TpCodeDecimal, "9223372036854775808"),
		newTypedByteJson(bytejson.TpCodeDecimal, "9223372036854776000"),
		mustParseJSONOverlap(t, `"x"`),
		mustParseJSONOverlap(t, `[1,2]`),
		mustParseJSONOverlap(t, `{"a":1}`),
	}
	random := rand.New(rand.NewSource(1))
	for iteration := 0; iteration < 100; iteration++ {
		left := make([]bytejson.ByteJson, 32)
		right := make([]bytejson.ByteJson, 32)
		for index := range left {
			left[index] = pool[random.Intn(len(pool))]
			right[index] = pool[random.Intn(len(pool))]
		}
		leftArray := makeJSONOverlapArray(t, left)
		rightArray := makeJSONOverlapArray(t, right)

		var workspace jsonOverlapWorkspace
		require.Equal(t, jsonOverlapNaiveArrays(leftArray, rightArray), workspace.arraysOverlap(leftArray, rightArray),
			"iteration %d", iteration)
	}
}

func makeJSONOverlapArray(t *testing.T, values []bytejson.ByteJson) bytejson.ByteJson {
	t.Helper()
	items := make([]any, len(values))
	for index := range values {
		items[index] = values[index]
	}
	array, err := bytejson.CreateByteJSON(items)
	require.NoError(t, err)
	return array
}

func jsonOverlapNaiveArrays(left, right bytejson.ByteJson) bool {
	for leftIndex := 0; leftIndex < left.GetElemCnt(); leftIndex++ {
		for rightIndex := 0; rightIndex < right.GetElemCnt(); rightIndex++ {
			if equalJSONOverlapExact(left.GetArrayElem(leftIndex), right.GetArrayElem(rightIndex)) {
				return true
			}
		}
	}
	return false
}

func jsonOverlapIntegerArray(start, count int) string {
	var builder strings.Builder
	builder.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.Itoa(start + i))
	}
	builder.WriteByte(']')
	return builder.String()
}

func TestJSONOverlapsSQL(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`[1,3,5,7]`, `{"a":1}`, `null`, ``, `[{"a":1}]`},
				[]bool{false, false, false, true, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`[2,5,7]`, `{"a":2}`, `null`, `invalid`, `{"a":1}`},
				[]bool{false, false, false, false, false}),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{1, 0, 1, 0, 1}, []bool{false, false, false, true, false}),
		jsonOverlaps,
	)
	succeed, message := testCase.Run()
	require.True(t, succeed, message)
}

func TestJSONOverlapsMySQLDocumentCases(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{
					`6`,
					`{"a":1}`,
					`{"a":null}`,
					`{"a":1,"a":2}`,
					`[]`,
					`true`,
					`"1"`,
					`[[1,2]]`,
					`"x"`,
				}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{
					`[4,5,6,7]`,
					`[{"a":1}]`,
					`{"a":null}`,
					`{"a":2}`,
					`[]`,
					`1`,
					`1`,
					`[[1,2]]`,
					`"x"`,
				}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{1, 1, 1, 1, 0, 0, 0, 1, 1}, nil),
		jsonOverlaps,
	)
	succeed, message := testCase.Run()
	require.True(t, succeed, message)
}

func TestJSONOverlapsTypedJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(),
				[]string{mustJsonBinaryString(t, `[1,{"a":2}]`)}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":2}`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil),
		jsonOverlaps,
	)
	succeed, message := testCase.Run()
	require.True(t, succeed, message)

	tooDeep := strings.Repeat(`{"a":`, bytejson.JSONDocumentMaxNestingDepth+1) + `1` +
		strings.Repeat(`}`, bytejson.JSONDocumentMaxNestingDepth+1)
	testCase = NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, tooDeep)}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`1`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
		jsonOverlaps,
	)
	require.NoError(t, testCase.result.PreExtendAndReset(1))
	err := testCase.fn(testCase.parameters, testCase.result, proc, 1, nil)
	require.ErrorContains(t, err, "nesting depth exceeds 100")
}

func TestJSONOverlapsEvaluationOrder(t *testing.T) {
	proc := testutil.NewProcess(t)
	tooDeep := strings.Repeat(`{"a":`, bytejson.JSONDocumentMaxNestingDepth+1) + `1` +
		strings.Repeat(`}`, bytejson.JSONDocumentMaxNestingDepth+1)

	tests := []struct {
		name      string
		left      string
		leftNull  bool
		right     string
		rightNull bool
		wantError string
	}{
		{name: "left null skips invalid right", leftNull: true, right: `invalid`},
		{name: "invalid left precedes right null", left: `invalid`, rightNull: true, wantError: "invalid JSON document"},
		{name: "deep left precedes right null", left: tooDeep, rightNull: true, wantError: "nesting depth exceeds 100"},
		{name: "left null skips deep right", leftNull: true, right: tooDeep},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{tt.left}, []bool{tt.leftNull}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{tt.right}, []bool{tt.rightNull}),
				},
				NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{tt.wantError == ""}),
				jsonOverlaps,
			)
			require.NoError(t, testCase.result.PreExtendAndReset(1))
			err := testCase.fn(testCase.parameters, testCase.result, proc, 1, nil)
			if tt.wantError == "" {
				require.NoError(t, err)
				require.True(t, testCase.result.GetResultVector().IsNull(0))
			} else {
				require.ErrorContains(t, err, tt.wantError)
			}
		})
	}
}

func TestJSONOverlapsCheckFn(t *testing.T) {
	ctx := context.Background()
	for _, inputs := range [][]types.Type{
		{types.T_json.ToType(), types.T_varchar.ToType()},
		{types.T_char.ToType(), types.T_text.ToType()},
		{types.T_binary.ToType(), types.T_varchar.ToType()},
		{types.T_varchar.ToType(), types.T_binary.ToType()},
		{types.T_binary.ToType(), types.T_binary.ToType()},
		{types.T_varbinary.ToType(), types.T_varchar.ToType()},
		{types.T_varchar.ToType(), types.T_varbinary.ToType()},
		{types.T_varbinary.ToType(), types.T_varbinary.ToType()},
		{types.T_blob.ToType(), types.T_varchar.ToType()},
		{types.T_varchar.ToType(), types.T_blob.ToType()},
		{types.T_blob.ToType(), types.T_blob.ToType()},
		{types.T_any.ToType(), types.T_varchar.ToType()},
	} {
		_, err := GetFunctionByName(ctx, "json_overlaps", inputs)
		require.NoError(t, err)
	}

	for _, inputs := range [][]types.Type{
		{types.T_varchar.ToType()},
		{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		{types.T_int64.ToType(), types.T_varchar.ToType()},
	} {
		_, err := GetFunctionByName(ctx, "json_overlaps", inputs)
		require.Error(t, err)
	}
}

func TestJSONOverlapsMySQLBinaryStringTypes(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(oid.ToType(), []string{`[1,2]`, `{"a":1}`, `null`, `invalid`},
						[]bool{false, false, false, true}),
					NewFunctionTestInput(oid.ToType(), []string{`[2,3]`, `{"a":2}`, `null`, `[1]`}, nil),
				},
				NewFunctionTestResult(types.T_int64.ToType(), false,
					[]int64{1, 0, 1, 0}, []bool{false, false, false, true}),
				jsonOverlaps,
			)
			succeed, message := testCase.Run()
			require.True(t, succeed, message)
		})
	}
}

func TestJSONOverlapsIgnoreAllRowsDoesNotParse(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`invalid`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`also invalid`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonOverlaps,
	)
	require.NoError(t, testCase.result.PreExtendAndReset(1))
	err := testCase.fn(testCase.parameters, testCase.result, proc, 1, &FunctionSelectList{AllNull: true})
	require.NoError(t, err)
	require.True(t, testCase.result.GetResultVector().IsNull(0))
}

func TestJSONOverlapEvaluableRowsExcludesSelectedAndNullRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`[1]`, `[2]`, `[3]`, `[4]`},
				[]bool{false, false, true, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`[1]`, `[2]`, `[3]`, `[4]`},
				[]bool{false, true, false, false}),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonOverlaps,
	)
	left := jsonOverlapOperand{wrapper: vector.GenerateFunctionStrParameter(testCase.parameters[0])}
	right := jsonOverlapOperand{wrapper: vector.GenerateFunctionStrParameter(testCase.parameters[1])}
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}}

	require.Equal(t, 1, jsonOverlapEvaluableRows(&left, &right, 4, selectList))
}

func TestJSONOverlapsPartialSelectListSkipsParsingAndPreservesNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`invalid`, `invalid`, `[1,2]`, `also invalid`},
				[]bool{false, true, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`invalid`, `[1]`, `[2,3]`, `also invalid`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonOverlaps,
	)
	require.NoError(t, testCase.result.PreExtendAndReset(4))
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true, false}}

	require.NoError(t, testCase.fn(testCase.parameters, testCase.result, proc, 4, selectList))
	result := testCase.result.GetResultVector()
	require.True(t, result.IsNull(0))
	require.True(t, result.IsNull(1))
	require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](result)[2])
	require.True(t, result.IsNull(3))
}

func TestJSONOverlapsAccessorAllocationsDoNotScaleWithRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	trueJSON := mustJsonBinaryString(t, `true`)
	falseJSON := mustJsonBinaryString(t, `false`)

	measure := func(rows int) float64 {
		left := make([]string, rows)
		right := make([]string, rows)
		for i := range rows {
			left[i] = trueJSON
			right[i] = falseJSON
		}
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), left, nil),
				NewFunctionTestInput(types.T_json.ToType(), right, nil),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
			jsonOverlaps,
		)
		var runErr error
		allocations := testing.AllocsPerRun(3, func() {
			runErr = testCase.result.PreExtendAndReset(rows)
			if runErr == nil {
				runErr = testCase.fn(testCase.parameters, testCase.result, proc, rows, nil)
			}
		})
		require.NoError(t, runErr)
		return allocations
	}

	oneRow := measure(1)
	manyRows := measure(8192)
	require.LessOrEqual(t, manyRows, oneRow+10,
		"accessor allocations must be batch-scoped: one=%f many=%f", oneRow, manyRows)
}

func BenchmarkJSONOverlapIndexedArrays(b *testing.B) {
	left, err := types.ParseSliceToByteJson([]byte(jsonOverlapIntegerArray(0, 8192)))
	if err != nil {
		b.Fatal(err)
	}
	right, err := types.ParseSliceToByteJson([]byte(jsonOverlapIntegerArray(8192, 8192)))
	if err != nil {
		b.Fatal(err)
	}
	var workspace jsonOverlapWorkspace
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if workspace.overlaps(left, right) {
			b.Fatal("unexpected overlap")
		}
	}
}

func BenchmarkJSONOverlapPreparedConstArrayVectorBatch(b *testing.B) {
	array, err := types.ParseSliceToByteJson([]byte(jsonOverlapIntegerArray(0, 8192)))
	if err != nil {
		b.Fatal(err)
	}
	probe, err := types.ParseSliceToByteJson([]byte(`[16384]`))
	if err != nil {
		b.Fatal(err)
	}
	for _, tt := range []struct {
		name      string
		constLeft bool
	}{
		{name: "const_left", constLeft: true},
		{name: "const_right", constLeft: false},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				prepared := jsonOverlapPreparedArray{}
				constView := jsonOverlapValueView{document: array, prepared: &prepared}
				probeView := jsonOverlapValueView{document: probe}
				left, right := constView, probeView
				if !tt.constLeft {
					left, right = right, left
				}
				var workspace jsonOverlapWorkspace
				for range 8192 {
					if workspace.overlapsViews(left, right, 8192) {
						b.Fatal("unexpected overlap")
					}
				}
			}
		})
	}
}
