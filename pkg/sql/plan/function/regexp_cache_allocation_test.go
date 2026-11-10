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
	"regexp/syntax"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestRegexpCacheLazyAllocation(t *testing.T) {
	t.Run("unused", func(t *testing.T) {
		op := newOpBuiltInRegexp()
		require.Nil(t, op.regMap.mp)
		require.Nil(t, op.regMap.icu)
		require.Nil(t, op.regMap.mayMatchEmpty)
	})
	t.Run("like_fast_path", func(t *testing.T) {
		proc := newLikeTestProcess(t)
		op := newOpBuiltInRegexp()
		tc := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"catalog_columns", "other", "catalog"}, []bool{false, false, true}),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"catalog%"}, nil),
		}, NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, false}, []bool{false, false, true}), op.likeFn)
		t.Cleanup(tc.Free)
		ok, info := tc.Run()
		require.True(t, ok, info)
		require.Nil(t, op.regMap.mp)
		require.Nil(t, op.regMap.icu)
		require.Nil(t, op.regMap.mayMatchEmpty)
	})
	t.Run("re2_only", func(t *testing.T) {
		rs := newOpBuiltInRegexp().regMap
		matched, err := rs.regularMatchWithMode(`^ab$`, "ab", false)
		require.NoError(t, err)
		require.True(t, matched)
		first, err := rs.getRegularMatcher(`^ab$`)
		require.NoError(t, err)
		second, err := rs.getRegularMatcher(`^ab$`)
		require.NoError(t, err)
		require.Same(t, first, second)
		matched, err = rs.regularMatchWithMode(`^ab$`, "ac", false)
		require.NoError(t, err)
		require.False(t, matched)
		require.Len(t, rs.mp, 1)
		require.Len(t, rs.mayMatchEmpty, 1)
		require.Nil(t, rs.icu)
	})
	t.Run("icu_only", func(t *testing.T) {
		rs := newOpBuiltInRegexp().regMap
		before := regexp2ActiveSubjectBytes.Load()
		for _, tc := range []struct {
			subject string
			want    bool
		}{{"ab", true}, {"ac", false}} {
			matched, err := rs.regularMatchWithMode(`a(?=b)`, tc.subject, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, matched)
		}
		require.Len(t, rs.icu, 1)
		require.Nil(t, rs.mp)
		require.Nil(t, rs.mayMatchEmpty)
		require.Equal(t, before, regexp2ActiveSubjectBytes.Load())
	})
	t.Run("invalid_re2_does_not_publish", func(t *testing.T) {
		rs := newOpBuiltInRegexp().regMap
		_, err := rs.getRegularMatcher(`[`)
		var syntaxErr *syntax.Error
		require.ErrorAs(t, err, &syntaxErr)
		require.Equal(t, syntax.ErrMissingBracket, syntaxErr.Code)
		require.Nil(t, rs.mp)
		require.Nil(t, rs.icu)
		require.Nil(t, rs.mayMatchEmpty)
	})
}

// 与稳态匹配基准不同，每轮包含操作器创建，覆盖单次执行及 reset 后的新实例成本。
func BenchmarkRegexpOperatorCold(b *testing.B) {
	b.Run("like_prefix", func(b *testing.B) {
		proc := newLikeTestProcess(b)
		value, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("catalog_columns"), 1, proc.Mp())
		require.NoError(b, err)
		b.Cleanup(func() { value.Free(proc.Mp()) })
		pattern, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("catalog%"), 1, proc.Mp())
		require.NoError(b, err)
		b.Cleanup(func() { pattern.Free(proc.Mp()) })
		result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
		b.Cleanup(result.Free)
		parameters := []*vector.Vector{value, pattern}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := result.PreExtendAndReset(1); err != nil {
				b.Fatal(err)
			}
			if err := newOpBuiltInRegexp().likeFn(parameters, result, proc, 1, nil); err != nil {
				b.Fatal(err)
			}
			if !vector.GetFixedAtNoTypeCheck[bool](result.GetResultVector(), 0) {
				b.Fatal("LIKE result changed")
			}
		}
	})
	for _, tc := range []struct{ name, pattern string }{{"re2", `^ab$`}, {"icu", `a(?=b)`}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs := newOpBuiltInRegexp().regMap
				matched, err := rs.regularMatchWithMode(tc.pattern, "ab", false)
				if err != nil || !matched {
					b.Fatalf("match=%v, err=%v", matched, err)
				}
			}
		})
	}
}
