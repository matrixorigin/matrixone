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

// Cancel a real context synchronously at an observable evaluation boundary.
// No goroutine, deadline or production test hook is needed to cancel mid-batch.
type likeCancelAfterChecksContext struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func newLikeCancelAfterChecksContext(t *testing.T, checks int) *likeCancelAfterChecksContext {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &likeCancelAfterChecksContext{Context: ctx, cancel: cancel, remaining: checks}
}

func (ctx *likeCancelAfterChecksContext) Done() <-chan struct{} {
	ctx.remaining--
	if ctx.remaining == 0 {
		ctx.cancel()
	}
	return ctx.Context.Done()
}

func TestLikeRegexpBatchHonorsCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, value  string
		escape                string
		explicit, insensitive bool
		mixed                 bool
	}{
		{name: "LIKE", pattern: "a_%b", value: "aXXb"},
		{name: "ILIKE", pattern: "A_%B", value: "aXXb", insensitive: true},
		{name: "LIKE ESCAPE", pattern: "a!_%b", value: "a_XXb", explicit: true, escape: "!"},
		{name: "ILIKE ESCAPE", pattern: "A!_%B", value: "a_XXb", explicit: true, escape: "!", insensitive: true},
		{name: "mixed text", pattern: "a_%b", value: "aXXb", mixed: true},
		{name: "mixed text ESCAPE", pattern: "a!_%b", value: "a_XXb", explicit: true, escape: "!", mixed: true},
	} {
		for _, constant := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/constant=%v", tc.name, constant), func(t *testing.T) {
				proc := newLikeTestProcess(t)
				const rows = 3
				values := vector.NewVec(types.T_varchar.ToType())
				defer values.Free(proc.Mp())
				for range rows {
					require.NoError(t, vector.AppendBytes(values, []byte(tc.value), false, proc.Mp()))
				}
				if tc.mixed {
					require.NoError(t, values.SetRuntimeStringDomainsWithMP([]types.RuntimeStringDomain{
						types.RuntimeStringBinary, types.RuntimeStringText, types.RuntimeStringText,
					}, proc.Mp()))
				}
				var patterns *vector.Vector
				if constant {
					var err error
					patterns, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.pattern), rows, proc.Mp())
					require.NoError(t, err)
				} else {
					patterns = vector.NewVec(types.T_varchar.ToType())
					for range rows {
						require.NoError(t, vector.AppendBytes(patterns, []byte(tc.pattern), false, proc.Mp()))
					}
				}
				defer patterns.Free(proc.Mp())
				parameters := []*vector.Vector{values, patterns}
				if tc.explicit {
					escape, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.escape), rows, proc.Mp())
					require.NoError(t, err)
					defer escape.Free(proc.Mp())
					parameters = append(parameters, escape)
				}
				result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(rows))
				ctx := newLikeCancelAfterChecksContext(t, 2)
				proc.Ctx = ctx
				op := newOpBuiltInRegexp()
				fn := op.likeFn
				if tc.insensitive {
					fn = op.iLikeFn
				}
				err := fn(parameters, result, proc, rows, nil)
				require.ErrorIs(t, err, context.Canceled)
				require.Same(t, ctx.Err(), err, "return the original context error")
				matches := vector.MustFixedColNoTypeCheck[bool](result.GetResultVector())
				require.True(t, matches[0], "evaluation must begin before cancellation")
				if tc.mixed {
					require.True(t, matches[1], "the mixed-domain text matcher must begin before cancellation")
				}
				require.False(t, matches[rows-1], "a canceled batch must not finish successfully")
			})
		}
	}
}

func TestLikeFastPathBatchHonorsCancellation(t *testing.T) {
	for _, tc := range []struct{ name, pattern, value string }{
		{"empty", "", ""},
		{"any", "%", "a"},
		{"one rune", "_", "中"},
		{"one literal", "a", "a"},
		{"literal", "catalog", "catalog"},
		{"leading rune", "_tail", "中tail"},
		{"suffix", "%tail", "prefix_tail"},
		{"trailing rune", "head_", "head中"},
		{"prefix", "head%", "headtail"},
		{"contains", "%mid%", "prefixmidsuffix"},
		{"suffix rune", "%tail_", "prefix_tail中"},
		{"rune prefix", "_head%", "中headtail"},
		{"unanchored segments", "%a%b%", "xaYbZ"},
		{"anchored segments", "a%b%c", "aXbYc"},
	} {
		for _, insensitive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/insensitive=%v", tc.name, insensitive), func(t *testing.T) {
				proc := newLikeTestProcess(t)
				// The smallest batch crossing the 256-row cancellation interval.
				const rows = 257
				values := vector.NewVec(types.T_varchar.ToType())
				defer values.Free(proc.Mp())
				for range rows {
					require.NoError(t, vector.AppendBytes(values, []byte(tc.value), false, proc.Mp()))
				}
				patterns, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.pattern), rows, proc.Mp())
				require.NoError(t, err)
				defer patterns.Free(proc.Mp())
				result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(rows))
				ctx := newLikeCancelAfterChecksContext(t, 2)
				proc.Ctx = ctx
				op := newOpBuiltInRegexp()
				fn := op.likeFn
				if insensitive {
					fn = op.iLikeFn
				}
				err = fn([]*vector.Vector{values, patterns}, result, proc, rows, nil)
				require.ErrorIs(t, err, context.Canceled)
				require.Same(t, ctx.Err(), err)
				matches := vector.MustFixedColNoTypeCheck[bool](result.GetResultVector())
				require.True(t, matches[255], "the first interval must finish before cancellation")
				require.False(t, matches[256], "the second interval must observe cancellation")
			})
		}
	}
}

func TestLikeCancellationPreservesUnevaluatedRows(t *testing.T) {
	for _, insensitive := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, mode := range []string{"NULL values", "NULL pattern", "all skipped", "partial selection"} {
				t.Run(fmt.Sprintf("%s/insensitive=%v/escape=%v", mode, insensitive, explicit), func(t *testing.T) {
					proc := newLikeTestProcess(t)
					pattern := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a_%b"}, nil)
					if mode == "NULL pattern" {
						pattern = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true})
					}
					valueNulls := []bool{false, false, false}
					wantNulls := []bool{true, true, true}
					want := []bool{false, false, false}
					var selection *FunctionSelectList
					switch mode {
					case "NULL values":
						valueNulls = []bool{true, true, true}
					case "all skipped":
						selection = &FunctionSelectList{AnyNull: true, AllNull: true}
					case "partial selection":
						selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, false}}
						wantNulls = []bool{true, false, true}
						want[1] = true
					}
					inputs := []FunctionTestInput{
						NewFunctionTestInput(types.T_varchar.ToType(), []string{"aXXb", "aXXb", "aXXb"}, valueNulls), pattern,
					}
					if explicit {
						inputs = append(inputs, NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"!"}, nil))
					}
					ctx := newLikeCancelAfterChecksContext(t, 2)
					proc.Ctx = ctx
					op := newOpBuiltInRegexp()
					fn := op.likeFn
					if insensitive {
						fn = op.iLikeFn
					}
					tcc := NewFunctionTestCase(proc, inputs,
						NewFunctionTestResult(types.T_bool.ToType(), false, want, wantNulls), fn).WithSelectList(selection)
					defer tcc.Free()
					ok, info := tcc.Run()
					require.True(t, ok, info)
					require.NoError(t, ctx.Err(), "NULL and skipped rows must not invoke a regexp matcher")
					if mode != "partial selection" {
						require.Empty(t, op.regMap.mp, "unevaluated patterns must remain unprepared")
					}
				})
			}
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
