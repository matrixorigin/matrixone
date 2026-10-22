// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// LIKE needs only a context and pool. Avoid constructing file services or
// replacing a runtime-owned auto-increment service for each test/benchmark.
func newLikeTestProcess(t testing.TB) *process.Process {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(func() {
		proc.Free()
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	return proc
}

func TestLikePatternPreparation(t *testing.T) {
	proc := newLikeTestProcess(t)
	op := newOpBuiltInRegexp() // Reuse across patterns, batches and escape modes.
	for _, tc := range []struct {
		name, pattern, yes, no string
		escape                 string
		explicit, insensitive  bool
	}{
		{name: "anchored", pattern: "a%b%c", yes: "aXXbYYc", no: "aXXbYYcx"},
		{name: "prefix", pattern: "a%b%", yes: "aXXb", no: "xaXXb"},
		{name: "suffix", pattern: "%a%b", yes: "XXaYYb", no: "XXaYYbx"},
		{name: "no overlap", pattern: "ab%bc", yes: "abbc", no: "abc"},
		{name: "empty segments", pattern: "a%%%b", yes: "ab", no: "a"},
		{name: "underscore", pattern: "%a_b%c", yes: "a🙂bXXc", no: "a🙂🙂bXXc"},
		{name: "newline", pattern: "a_%b", yes: "a\nXXb", no: "ab"},
		{name: "unicode literal", pattern: "目%录%", yes: "目中录", no: "x目中录"},
		{name: "invalid UTF8", pattern: "a\xff_%", yes: "a\xfe🙂", no: "a🙂"},
		{name: "escaped percent", pattern: `a\%b_%`, yes: "a%b🙂", no: "aXXb🙂"},
		{name: "custom escape", pattern: "a!_%b", yes: "a_XXb", no: "axXXb", escape: "!", explicit: true},
		{name: "no escape", pattern: `a\_%b`, yes: "a\\xXXb", no: "axXXb", explicit: true},
		{name: "ILIKE segments", pattern: "A%B%C", yes: "aXXbYYc", no: "xaXXbYYc", insensitive: true},
		{name: "ILIKE unicode escape", pattern: "a界_%B", yes: "A_XXb", no: "AxXXb", escape: "界", explicit: true, insensitive: true},
		{name: "NUL literal", pattern: "a%\x00%b", yes: "aX\x00Yb", no: "aXYb"},
		{name: "segment limit boundary", pattern: "a%" + strings.Repeat("b", 64) + "%c", yes: "a" + strings.Repeat("b", 64) + "c", no: "abc"},
		{name: "segment limit fallback", pattern: "a%" + strings.Repeat("b", 65) + "%c", yes: "a" + strings.Repeat("b", 65) + "c", no: "abc"},
	} {
		for _, constant := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/constant=%v", tc.name, constant), func(t *testing.T) {
				pattern := NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.pattern, tc.pattern, tc.pattern}, nil)
				if constant {
					pattern = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.pattern}, nil)
				}
				inputs := []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.yes, tc.no, tc.yes}, []bool{false, false, true}), pattern,
				}
				if tc.explicit {
					inputs = append(inputs, NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.escape}, nil))
				}
				fn := op.likeFn
				if tc.insensitive {
					fn = op.iLikeFn
				}
				tcc := NewFunctionTestCase(proc, inputs,
					NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, false}, []bool{false, false, true}), fn)
				defer tcc.Free()
				ok, info := tcc.Run()
				require.True(t, ok, info)
			})
		}
	}
}

func TestLikeRowVaryingPatterns(t *testing.T) {
	proc := newLikeTestProcess(t)
	tcc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"abYc", "abYc", "abYc", "abYc", "abYc"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"a_%c", "a_%d", "x_%c", "a_%c", "a_%c"}, []bool{false, false, false, false, true}),
	}, NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, false, true, false}, []bool{false, false, false, false, true}), newOpBuiltInRegexp().likeFn)
	defer tcc.Free()
	ok, info := tcc.Run()
	require.True(t, ok, info)
}

func TestLikePatternSelection(t *testing.T) {
	proc := newLikeTestProcess(t)
	for _, pattern := range []string{"a%b%c", "%a_b%c", "a%"} {
		for _, allNull := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allNull=%v", pattern, allNull), func(t *testing.T) {
				tcc := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"aXbYc", "aXbYc"}, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{pattern}, nil),
				}, NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, true}, []bool{true, allNull}),
					func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
						selection := &FunctionSelectList{AnyNull: true, AllNull: allNull, SelectList: []bool{false, true}}
						return newOpBuiltInRegexp().likeFn(parameters, result, proc, length, selection)
					})
				defer tcc.Free()
				ok, info := tcc.Run()
				require.True(t, ok, info)
			})
		}
	}
}

// Compare the new anchored segment cases with the unchanged regexp conversion.
// This catches greedy overlap/anchoring errors across empty and repeated '%'.
func TestLikeAnchoredSegmentsAgainstRegexp(t *testing.T) {
	proc := newLikeTestProcess(t)
	op := newOpBuiltInRegexp()
	values := []string{"", "a", "b", "ab", "ba", "aba", "aabb", "ababa", "a\nb", "a中b", "a\xffb"}
	var visit func(string, int)
	visit = func(pattern string, depth int) {
		if strings.Contains(pattern, "%") && (pattern[0] != '%' || pattern[len(pattern)-1] != '%') {
			want := make([]bool, len(values))
			for row, value := range values {
				var err error
				want[row], err = op.regMap.regularMatchForLikeOp([]byte(pattern), []byte(value))
				require.NoError(t, err)
			}
			func() {
				tcc := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), values, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{pattern}, nil),
				}, NewFunctionTestResult(types.T_bool.ToType(), false, want, nil), op.likeFn)
				defer tcc.Free()
				ok, info := tcc.Run()
				require.True(t, ok, "%q: %s", pattern, info)
			}()
		}
		if depth > 0 {
			for _, next := range []string{"a", "b", "%"} {
				visit(pattern+next, depth-1)
			}
		}
	}
	visit("", 5)
}
