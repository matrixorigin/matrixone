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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestGeneralCIPrefixWeights(t *testing.T) {
	for _, tc := range []struct {
		value, prefix string
		want          bool
	}{
		{"Forest green", "foReSt", true}, {"foREst", "FOREST", true},
		{"forest", "forest x", false}, {"coral", "forest", false},
		{"fórêst", "FOREST", true}, {"ábaco", "ABA", true},
		{"ßeta", "S", true}, {"ßeta", "SS", false}, {"ıstanbul", "I", true},
		{"forest🙂", "forest", true}, {"fore\xffst", "forest", false},
		{"forest\xff", "forest", true}, {"\xffforest", "forest", false},
		{"", "F", false}, {"\x00x", "\x00", true},
	} {
		t.Run(tc.value+"/"+tc.prefix, func(t *testing.T) {
			require.Equal(t, tc.want, generalCIHasASCIIPrefix([]byte(tc.value), []byte(tc.prefix)))
		})
	}
}

func TestGeneralCIPrefixVectorEntryAndFallbacks(t *testing.T) {
	proc := newLikeTestProcess(t)
	typ := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8)
	for _, tc := range []struct {
		name, pattern string
		values        []string
		want          []bool
		masked        bool
	}{
		{"prefix", "foReSt%", []string{"forest", "fórêst", "forex", "forest\xff"}, []bool{true, true, false, true}, false},
		{"late Unicode", "forest%", []string{"foreßt", "foreST", "forest🙂", "fórest"}, []bool{true, true, true, true}, false},
		{"contains fallback", "%forest%", []string{"xforest", "coral", "forest", "xx"}, []bool{true, false, true, false}, false},
		{"underscore fallback", "f_rest%", []string{"forest", "fórest", "forx", "fXrest"}, []bool{true, true, false, true}, false},
		{"escaped fallback", `f\%rest%`, []string{"f%rest", "forest", "f%restx", "fXrest"}, []bool{true, false, true, false}, false},
		{"Unicode pattern fallback", "fórest%", []string{"forest", "fórêst", "coral", "FOREST"}, []bool{true, true, false, true}, false},
		{"masked fallback", "forest%", []string{"forest", "coral", "forest", "FOREST"}, []bool{false, false, true, false}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, constant := range []bool{false, true} {
				pattern := NewFunctionTestInput(typ, []string{tc.pattern, tc.pattern, tc.pattern, tc.pattern}, nil)
				if constant {
					pattern = NewFunctionTestConstInput(typ, []string{tc.pattern}, nil)
				}
				nulls := []bool{false, false, false, true}
				want := append([]bool(nil), tc.want...)
				want[3] = false
				wantNulls := append([]bool(nil), nulls...)
				var selection *FunctionSelectList
				if tc.masked {
					selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, true, false}}
					wantNulls = []bool{true, true, false, true}
				}
				fn := newOpBuiltInRegexp().likeFn
				tcc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(typ, tc.values, nulls), pattern},
					NewFunctionTestResult(types.T_bool.ToType(), false, want, wantNulls), fn).WithSelectList(selection)
				ok, info := tcc.Run()
				tcc.Free()
				require.True(t, ok, info)
			}
		})
	}
}
