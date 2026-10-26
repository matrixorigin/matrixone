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
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestJqConstResultPreservesCardinalityMaskAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInJq()
	inputs := []FunctionTestInput{
		NewFunctionTestConstInput(
			types.T_varchar.ToType(),
			[]string{`{"foo":128}`, `{"foo":128}`, `{"foo":128}`},
			nil,
		),
		NewFunctionTestConstInput(
			types.T_varchar.ToType(),
			[]string{".foo", ".foo", ".foo"},
			nil,
		),
	}
	testCase := NewFunctionTestCase(
		proc,
		inputs,
		NewFunctionTestResult(
			types.T_varchar.ToType(),
			false,
			[]string{"128", "128", "128"},
			nil,
		),
		op.tryJq,
	)

	defer testCase.Free()
	succeed, info := testCase.Run()
	require.True(t, succeed, info)

	// Reuse the result wrapper and operator with one masked row. Reuse must be
	// equivalent to a fresh result, and every logical row must still have one
	// physical varlena descriptor.
	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, op.tryJq(
		testCase.parameters,
		testCase.result,
		proc,
		testCase.fnLength,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}},
	))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, 3, result.Length())
	param := vector.GenerateFunctionStrParameter(result)
	for row, wantNull := range []bool{false, true, false} {
		value, isNull := param.GetStrValue(uint64(row))
		require.Equalf(t, wantNull, isNull, "row %d null state", row)
		if !wantNull {
			require.Equal(t, "128", string(value))
		}
	}

	resultBatch := batch.NewWithSize(1)
	resultBatch.Vecs[0] = result
	resultBatch.SetRowCount(3)
	require.NoError(t, resultBatch.Shuffle([]int64{2, 1, 0}, proc.Mp()))
	require.Equal(t, 3, resultBatch.RowCount())
}

func TestJqConstNonInlineResultSharesPayload(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInJq()
	payload := strings.Repeat("j", types.VarlenaInlineSize+17)
	jsonPayload := strconv.Quote(payload)
	result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
		NewFunctionTestConstInput(
			types.T_varchar.ToType(), []string{jsonPayload, jsonPayload, jsonPayload}, nil),
		NewFunctionTestConstInput(
			types.T_varchar.ToType(), []string{".", ".", "."}, nil),
	}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}, false)
	require.NoError(t, err)
	requireJqResult(t, result,
		[]string{jsonPayload, "", jsonPayload}, []bool{false, true, false})
	require.Len(t, result.GetArea(), len(jsonPayload))
}

func evalJqForTest(
	t *testing.T,
	proc *process.Process,
	op *opBuiltInJq,
	inputs []FunctionTestInput,
	selectList *FunctionSelectList,
	isTry bool,
) (*vector.Vector, error) {
	t.Helper()
	testCase := NewFunctionTestCase(
		proc,
		inputs,
		NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
		op.jq,
	)
	// The result remains borrowed through the current test scope.
	t.Cleanup(testCase.Free)
	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	fn := op.jq
	if isTry {
		fn = op.tryJq
	}
	err := fn(testCase.parameters, testCase.result, proc, testCase.fnLength, selectList)
	return testCase.GetResultVectorDirectly(), err
}

func requireJqResult(t *testing.T, result *vector.Vector, values []string, nulls []bool) {
	t.Helper()
	require.Equal(t, len(values), result.Length())
	parameter := vector.GenerateFunctionStrParameter(result)
	for i := range values {
		value, isNull := parameter.GetStrValue(uint64(i))
		require.Equalf(t, nulls[i], isNull, "row %d null state", i)
		if !isNull {
			require.Equalf(t, values[i], string(value), "row %d value", i)
		}
	}
}

func TestJqVarlenaProducerMatrix(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("zero rows", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{}, nil),
		}, nil, false)
		require.NoError(t, err)
		require.Zero(t, result.Length())
	})

	t.Run("both constant try error", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"invalid", "invalid"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{".", "."}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})
	})

	t.Run("one constant null and try compile error", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"", ""}, []bool{true, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{".", "."}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})

		result, err = evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"foo":1}`, `{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"", ""}, []bool{true, false}),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})

		result, err = evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"foo":1}`, `{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"(", "("}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})
	})

	t.Run("constant json vector query", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, `{"foo":1}`, `{"foo":1}`, `{"foo":1}`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{".foo", "(", ".foo", ".foo"}, []bool{false, false, true, false}),
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}}, true)
		require.NoError(t, err)
		requireJqResult(t, result,
			[]string{"1", "", "", ""}, []bool{false, true, true, true})
	})

	t.Run("vector json constant query", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, "invalid", `{"foo":1}`, `{"foo":1}`},
				[]bool{false, false, true, false}),
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{".foo", ".foo", ".foo", ".foo"}, nil),
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}}, true)
		require.NoError(t, err)
		requireJqResult(t, result,
			[]string{"1", "", "", ""}, []bool{false, true, true, true})

		// A try_jq row error must reset the reusable encoder before the next
		// evaluation on the same operator.
		result, err = evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{".foo"}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"2"}, []bool{false})
	})

	t.Run("both vectors", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, `{"foo":2}`, "invalid", `{"foo":4}`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{".foo", "(", ".foo", ".foo"}, []bool{false, false, true, false}),
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}}, true)
		require.NoError(t, err)
		requireJqResult(t, result,
			[]string{"1", "", "", ""}, []bool{false, true, true, true})
	})

	t.Run("strict compile error", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, `{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"(", "("}, nil),
		}, nil, false)
		require.Error(t, err)
		require.Zero(t, result.Length())
	})
}

func TestJqFramesMultipleResultsAndPreservesLargeIntegers(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInJq()

	tests := []struct {
		name  string
		json  string
		query string
		want  string
		isTry bool
	}{
		{name: "zero results", json: `[]`, query: `.[]`, want: ""},
		{name: "safe integer boundary", json: `9007199254740991`, query: `.`, want: `9007199254740991`},
		{name: "positive integer beyond boundary", json: `9007199254740993`, query: `.`, want: `9007199254740993`},
		{name: "negative integer beyond boundary", json: `-9007199254740993`, query: `.`, want: `-9007199254740993`},
		{
			name:  "multiple results",
			json:  `[1, -9007199254740993, 9007199254740993]`,
			query: `.[]`,
			want:  "1\n-9007199254740993\n9007199254740993",
		},
		{
			name:  "try multiple results",
			json:  `[1, 2, 3]`,
			query: `.[]`,
			want:  "1\n2\n3",
			isTry: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{test.json}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{test.query}, nil),
			}, nil, test.isTry)
			require.NoError(t, err)
			requireJqResult(t, result, []string{test.want}, []bool{false})
		})
	}

	_, err := evalJqForTest(t, proc, op, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, nil),
		NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"."}, nil),
	}, nil, false)
	require.EqualError(t, err, "unexpected end of JSON input")

	_, err = evalJqForTest(t, proc, op, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"1e1000"}, nil),
		NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"."}, nil),
	}, nil, false)
	require.EqualError(t, err, "json: cannot unmarshal number 1e1000 into Go value of type float64")

	result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"value":-1e1000}`}, nil),
		NewFunctionTestConstInput(types.T_varchar.ToType(), []string{".value"}, nil),
	}, nil, true)
	require.NoError(t, err)
	requireJqResult(t, result, []string{""}, []bool{true})
}
func TestJqRejectsOutOfRangeNumbersBeforeDuplicateKeysOverwriteThem(t *testing.T) {
	require.False(t, jqJSONNumberOutOfFloat64Range([]byte(`{"text":"1e1000"}`)))
	require.False(t, jqJSONNumberOutOfFloat64Range([]byte(`{"text":"\"1e1000"}`)))
	require.False(t, jqJSONNumberOutOfFloat64Range([]byte(`{"text":"\\1e1000"}`)))

	proc := testutil.NewProcess(t)
	tests := []struct {
		name string
		json string
		want string
	}{
		{name: "positive", json: `{"n":1e1000,"n":1}`, want: "json: cannot unmarshal number 1e1000 into Go struct field .n of type float64"},
		{name: "negative", json: `{"n":-1e1000,"n":1}`, want: "json: cannot unmarshal number -1e1000 into Go struct field .n of type float64"},
		{name: "nested object", json: `{"outer":{"n":1e1000,"n":1}}`, want: "json: cannot unmarshal number 1e1000 into Go struct field .outer.n of type float64"},
		{name: "nested array", json: `{"outer":[{"n":-1e1000,"n":1}]}`, want: "json: cannot unmarshal number -1e1000 into Go struct field .outer.0.n of type float64"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := evalJqForTest(t, proc, newOpBuiltInJq(), []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{test.json}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"."}, nil),
			}, nil, false)
			require.EqualError(t, err, test.want)

			result, err := evalJqForTest(t, proc, newOpBuiltInJq(), []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{test.json}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"."}, nil),
			}, nil, true)
			require.NoError(t, err)
			requireJqResult(t, result, []string{""}, []bool{true})
		})
	}
}

// evalJqSingleForTest evaluates one const json/query pair and returns the
// single row's string value. It is the focused harness for jq output-shape
// assertions where every row repeats the same const inputs.
func evalJqSingleForTest(
	t *testing.T,
	proc *process.Process,
	op *opBuiltInJq,
	jsonStr, query string,
	isTry bool,
) (string, bool, error) {
	t.Helper()
	result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
		NewFunctionTestConstInput(types.T_varchar.ToType(),
			[]string{jsonStr, jsonStr, jsonStr}, nil),
		NewFunctionTestConstInput(types.T_varchar.ToType(),
			[]string{query, query, query}, nil),
	}, nil, isTry)
	if err != nil {
		return "", false, err
	}
	require.Equal(t, 3, result.Length())
	parameter := vector.GenerateFunctionStrParameter(result)
	value, isNull := parameter.GetStrValue(0)
	return string(value), isNull, nil
}

func TestJqMultiResultFraming(t *testing.T) {
	proc := testutil.NewProcess(t)

	// jq yields one value per iteration; the SQL result keeps their
	// boundaries with a newline between successive encoded results.
	framingCases := []struct {
		jsonStr string
		query   string
		want    string
	}{
		// issue #30924 reproduction set
		{`[1,2,3,4,5]`, `.[]`, "1\n2\n3\n4\n5"},
		{`{"a":1,"b":2}`, `.a, .b`, "1\n2"},
		{`[1,2,3]`, `.[] | . * 10`, "10\n20\n30"},
		{`[1,2,3,4,5]`, `.[] | select(. > 2)`, "3\n4\n5"},
		// multi-result objects and strings keep framing as well
		{`{"user":"stedolan","titles":["JQ Primer","More JQ"]}`, `{user, title: .titles[]}`,
			`{"title":"JQ Primer","user":"stedolan"}` + "\n" + `{"title":"More JQ","user":"stedolan"}`},
		{`["a","b","c","d","e"]`, `.[]`, "\"a\"\n\"b\"\n\"c\"\n\"d\"\n\"e\""},
		// zero and one results never gain a separator
		{`[]`, `.[]`, ""},
		{`[1,2,3]`, `.[] | select(. > 10)`, ""},
		{`{"a":1}`, `.a`, "1"},
		{`[10,20,30]`, `map(.)`, "[10,20,30]"},
	}
	for _, tc := range framingCases {
		t.Run("const "+tc.jsonStr+" "+tc.query, func(t *testing.T) {
			op := newOpBuiltInJq()
			got, isNull, err := evalJqSingleForTest(t, proc, op, tc.jsonStr, tc.query, false)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("vector json and vector query", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`["a","b","c","d","e"]`, `{"a":1,"b":2}`, `[1,2,3,4,5]`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`.[]`, `.[]`, `.[] | select(. > 2)`}, nil),
		}, nil, false)
		require.NoError(t, err)
		requireJqResult(t, result, []string{
			"\"a\"\n\"b\"\n\"c\"\n\"d\"\n\"e\"",
			"1\n2",
			"3\n4\n5",
		}, []bool{false, false, false})
	})

	t.Run("const json vector query", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{`[1,2,3,4,5]`, `[1,2,3,4,5]`, `[1,2,3,4,5]`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`.[]`, `.[] | select(. > 3)`, `.[]`}, nil),
		}, nil, false)
		require.NoError(t, err)
		requireJqResult(t, result, []string{
			"1\n2\n3\n4\n5",
			"4\n5",
			"1\n2\n3\n4\n5",
		}, []bool{false, false, false})
	})

	t.Run("vector json const query", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`[1,2,3]`, `[4,5]`, `[]`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{`.[]`, `.[]`, `.[]`}, nil),
		}, nil, false)
		require.NoError(t, err)
		requireJqResult(t, result, []string{
			"1\n2\n3",
			"4\n5",
			"",
		}, []bool{false, false, false})
	})

	t.Run("mid evaluation error keeps strict and try contract", func(t *testing.T) {
		op := newOpBuiltInJq()
		const errQuery = `.[] | if . == 3 then error("boom") else . end`
		_, _, err := evalJqSingleForTest(t, proc, op, `[1,2,3]`, errQuery, false)
		require.Error(t, err)

		opTry := newOpBuiltInJq()
		got, isNull, err := evalJqSingleForTest(t, proc, opTry, `[1,2,3]`, errQuery, true)
		require.NoError(t, err)
		require.True(t, isNull, "try_jq must hide mid-evaluation errors, got %q", got)
	})

	t.Run("trailing values stay rejected", func(t *testing.T) {
		op := newOpBuiltInJq()
		_, _, err := evalJqSingleForTest(t, proc, op, `{"a":1} [2] 3`, `.`, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "after top-level value")

		opTry := newOpBuiltInJq()
		got, isNull, err := evalJqSingleForTest(t, proc, opTry, `{"a":1} [2] 3`, `.`, true)
		require.NoError(t, err)
		require.True(t, isNull, "try_jq must be null for trailing data, got %q", got)
	})
}

func TestJqPreservesExactIntegerBoundaries(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Integers must pass through without a float64 round trip: jq semantics
	// keep int within range and big.Int beyond it. 9007199254740993 is 2^53+1,
	// the first double that cannot represent its own successor.
	exactCases := []struct {
		jsonStr string
		query   string
		want    string
	}{
		{`{"n":9007199254740993}`, `.n`, "9007199254740993"},
		{`{"n":-9007199254740993}`, `.n`, "-9007199254740993"},
		{`{"n":9007199254740993}`, `.n * 3`, "27021597764222979"},
		{`{"n":9007199254740993}`, `.n * 1000000000000`, "9007199254740993000000000000"},
		{`{"n":4722366482869645213696}`, `.n`, "4722366482869645213696"},
		{`{"n":4722366482869645213696}`, `.n + 1`, "4722366482869645213697"},
		{`{"a":{"b":4722366482869645213696}}`, `.a.b`, "4722366482869645213696"},
		{`[9007199254740993,4722366482869645213696]`, `.[]`,
			"9007199254740993\n4722366482869645213696"},
		// ordinary numbers and floats keep their current rendering
		{`{"n":128}`, `.n`, "128"},
		{`{"n":1.5}`, `.n`, "1.5"},
		{`{"n":2.0}`, `.n`, "2"},
		{`{"n":1e21}`, `.n`, "1e+21"},
	}
	for _, tc := range exactCases {
		t.Run("const "+tc.jsonStr+" "+tc.query, func(t *testing.T) {
			op := newOpBuiltInJq()
			got, isNull, err := evalJqSingleForTest(t, proc, op, tc.jsonStr, tc.query, false)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, tc.want, got)

			// try_jq and a second evaluation on the same operator must be
			// identical: the shared encoder is reset between evaluations.
			opTry := newOpBuiltInJq()
			gotTry, isNullTry, err := evalJqSingleForTest(t, proc, opTry, tc.jsonStr, tc.query, true)
			require.NoError(t, err)
			require.False(t, isNullTry)
			require.Equal(t, tc.want, gotTry)

			state, isNull2, err := evalJqSingleForTest(t, proc, op, tc.jsonStr, tc.query, false)
			require.NoError(t, err)
			require.False(t, isNull2)
			require.Equal(t, tc.want, state)
		})
	}

	t.Run("vector parity", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"n":9007199254740993}`, `{"n":-9007199254740993}`, `{"n":4722366482869645213696}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{`.n`, `.n`, `.n`}, nil),
		}, nil, false)
		require.NoError(t, err)
		requireJqResult(t, result, []string{
			"9007199254740993",
			"-9007199254740993",
			"4722366482869645213696",
		}, []bool{false, false, false})
	})
}

func TestJqRejectsFloatOverflowLikeLegacyUnmarshal(t *testing.T) {
	proc := testutil.NewProcess(t)

	// json.Unmarshal into any (the pre-fix input path) rejected every number
	// token that overflows float64. That error contract is kept byte for
	// byte, including the decoder's context path: jq returns the legacy
	// error and try_jq yields NULL, whether the overflowing token is
	// fractional or integer-only. In-range integers still pass through
	// json.Number and stay exact through gojq's int/*big.Int path.
	for _, tc := range []struct {
		jsonStr string
		query   string
		wantErr string
	}{
		{`{"n":1e309}`, `.n`,
			"json: cannot unmarshal number 1e309 into Go struct field .n of type float64"},
		{`{"n":-1e309}`, `.n`,
			"json: cannot unmarshal number -1e309 into Go struct field .n of type float64"},
		{`{"n":1e10000000}`, `.n`,
			"json: cannot unmarshal number 1e10000000 into Go struct field .n of type float64"},
		{`{"a":{"b":[1e309,2]}}`, `.[]`,
			"json: cannot unmarshal number 1e309 into .a.b.0 of type float64"},
		{`[1.5,2e309]`, `.[]`,
			"json: cannot unmarshal number 2e309 into .1 of type float64"},
		{`1e309`, `.`,
			"json: cannot unmarshal number 1e309 into Go value of type float64"},
	} {
		t.Run("rejects "+tc.jsonStr, func(t *testing.T) {
			op := newOpBuiltInJq()
			_, _, err := evalJqSingleForTest(t, proc, op, tc.jsonStr, tc.query, false)
			require.EqualError(t, err, tc.wantErr)

			opTry := newOpBuiltInJq()
			got, isNull, err := evalJqSingleForTest(t, proc, opTry, tc.jsonStr, tc.query, true)
			require.NoError(t, err)
			require.True(t, isNull, "try_jq must be null for float overflow, got %q", got)
		})
	}

	t.Run("vector rows reject the overflowing row", func(t *testing.T) {
		op := newOpBuiltInJq()
		_, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"n":1}`, `{"n":1e309}`, `{"n":3}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{`.n`, `.n`, `.n`}, nil),
		}, nil, false)
		require.Error(t, err)

		opTry := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, opTry, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"n":1}`, `{"n":1e309}`, `{"n":3}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{`.n`, `.n`, `.n`}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result,
			[]string{"1", "", "3"}, []bool{false, true, false})
	})

	t.Run("integer-only overflow keeps the legacy rejection", func(t *testing.T) {
		// Out-of-range tokens stay rejected without regard to integer or
		// fractional form (#31060 upstream contract): a 401-digit literal
		// overflows float64, so the evaluation fails with the legacy
		// json.Unmarshal error byte for byte and try_jq yields NULL.
		hugeInt := "1" + strings.Repeat("0", 400)
		jsonStr := `{"n":` + hugeInt + `}`
		op := newOpBuiltInJq()
		_, _, err := evalJqSingleForTest(t, proc, op, jsonStr, `.n`, false)
		require.EqualError(t, err, "json: cannot unmarshal number "+hugeInt+
			" into Go struct field .n of type float64")

		opTry := newOpBuiltInJq()
		got, isNull, err := evalJqSingleForTest(t, proc, opTry, jsonStr, `.n`, true)
		require.NoError(t, err)
		require.True(t, isNull, "try_jq must be null for the legacy rejection, got %q", got)
	})

	t.Run("representable extreme floats still eval", func(t *testing.T) {
		op := newOpBuiltInJq()
		got, isNull, err := evalJqSingleForTest(t, proc, op, `{"n":2e-308}`, `.n`, false)
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, "2e-308", got)
	})
}

func TestJsonRowPropagatesResultAllocationFailureAndResetsEncoder(t *testing.T) {
	for _, masked := range []bool{false, true} {
		t.Run(strconv.FormatBool(masked), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			memory, err := mpool.NewMPool("json-row-result-failure", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			defer mpool.DeleteMPool(memory)
			held, err := memory.Alloc(1<<20, true)
			require.NoError(t, err)
			defer memory.Free(held)
			failed := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), memory)
			defer failed.Free()
			require.NoError(t, failed.PreExtendAndReset(0))
			op := newOpBuiltInJsonRow()
			var mask *FunctionSelectList
			if masked {
				mask = &FunctionSelectList{AnyNull: true, SelectList: []bool{false}}
			}
			require.Error(t, op.jsonRow(nil, failed, proc, 1, mask))
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))
			require.NoError(t, op.jsonRow(nil, result, proc, 1, nil))
			value, isNull := vector.GenerateFunctionStrParameter(result.GetResultVector()).GetStrValue(0)
			require.False(t, isNull)
			require.Equal(t, "[]", string(value))
		})
	}
}
