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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegexpRegularSubjectAdmission(t *testing.T) {
	for _, binary := range []bool{false, true} {
		for _, size := range []int{16384, 1 << 20} {
			name := "text"
			if binary {
				name = "binary"
			}
			if size > 16384 {
				name += "/large"
			} else {
				name += "/small"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				fc := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"a\n" + strings.Repeat("y", size) + "\r"}, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`^a\s`}, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"m"}, nil),
				}, NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil), newOpBuiltInRegexp().builtInRegexpLike)
				defer fc.Free()
				if binary {
					require.NoError(t, fc.parameters[0].SetRuntimeStringDomainAtWithMP(0, types.RuntimeStringBinary, proc.Mp()))
				}
				ok, info := fc.Run()
				require.True(t, ok, info)
			})
		}
	}
}

func BenchmarkRegexpPredicateModes(b *testing.B) {
	for _, tc := range []struct {
		name, subject string
		binary        bool
	}{
		{"short_text", "alpha", false},
		{"long_text", "alpha" + strings.Repeat("x", 32768), false},
		{"long_binary", "alpha" + strings.Repeat("x", 32768), true},
	} {
		for _, mode := range []string{"m", "mu", ""} {
			label := mode
			if label == "" {
				label = "default"
			}
			b.Run(tc.name+"/"+label, func(b *testing.B) {
				proc := testutil.NewProcess(b)
				subjects := make([]string, 256)
				want := make([]bool, 256)
				for i := range subjects {
					subjects[i] = tc.subject
					want[i] = true
				}
				inputs := []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), subjects, nil), NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`^alpha`}, nil)}
				if mode != "" {
					inputs = append(inputs, NewFunctionTestConstInput(types.T_varchar.ToType(), []string{mode}, nil))
				}
				fc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(types.T_bool.ToType(), false, want, nil), newOpBuiltInRegexp().builtInRegexpLike)
				defer fc.Free()
				if tc.binary {
					require.NoError(b, fc.parameters[0].SetRuntimeStringDomainWithMP(types.RuntimeStringBinary, proc.Mp()))
				}
				fc.Benchmark(b)
			})
		}
	}
}

func BenchmarkRegexpTerminalPrefixRejection(b *testing.B) {
	for _, pattern := range []string{`a.*b`, `a.*\z`} {
		b.Run(pattern, func(b *testing.B) {
			proc := testutil.NewProcess(b)
			subjects := make([]string, 256)
			want := make([]int64, len(subjects))
			for i := range subjects {
				subjects[i] = strings.Repeat("b", 300)
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), subjects, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{pattern}, nil),
			}, NewFunctionTestResult(types.T_int64.ToType(), false, want, nil), newOpBuiltInRegexp().builtInRegexpInstr)
			defer fc.Free()
			fc.Benchmark(b)
		})
	}
}

// Consuming across a wide boundary must retain captures, whole-subject anchors
// and occurrence offsets in every SQL regexp operation, even beyond ICU limits.
func TestRegexpRegularCrossLineOperations(t *testing.T) {
	for _, binary := range []bool{false, true} {
		for _, pattern := range []string{`^a\rb`, `^a\sb`, `(?s:^a.b)`, `^a[\s\S]+?b`, `\A(a\rb)`, `(?:^z|^a\rb)`} {
			op := newOpBuiltInRegexp()
			subject := "a\rb" + strings.Repeat("y", (1<<20)+1)
			got, err := op.regMap.regularLikeWithMode(pattern, subject, "m", binary)
			require.NoError(t, err, pattern)
			require.True(t, got, pattern)
			found, value, err := op.regMap.regularSubstrWithMatchType(pattern, subject, 1, 1, binary, "m")
			require.NoError(t, err, pattern)
			require.True(t, found, pattern)
			require.Equal(t, "a\rb", value, pattern)
			at, err := op.regMap.regularInstrWithMatchType(pattern, subject, 1, 1, 1, binary, "m")
			require.NoError(t, err, pattern)
			require.Equal(t, int64(4), at, pattern)
			replaced, err := op.regMap.regularReplaceWithMatchType(pattern, subject, "X", 1, 1, binary, "m")
			require.NoError(t, err, pattern)
			require.Equal(t, "X"+subject[3:], replaced, pattern)
		}
	}
	op := newOpBuiltInRegexp()
	got, err := op.regMap.regularReplaceWithMatchType(`(?m)^(a\rb)`, "x\ra\rb", "<$1>", 3, 1, false, "m")
	require.NoError(t, err)
	require.Equal(t, "x\r<a\rb>", got)
	// Consuming atoms keep Go semantics; wide mode changes assertions only.
	for _, mode := range []string{"m", "mu"} {
		got, err := op.regMap.regularLikeWithMode(`^a.b`, "a\rb", mode, false)
		require.NoError(t, err)
		require.True(t, got)
		got, err = op.regMap.regularLikeWithMode(`^a.b`, "a\nb", mode, false)
		require.NoError(t, err)
		require.False(t, got)
	}
}
