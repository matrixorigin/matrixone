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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCollationKeyComparators(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	general := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetUTF8)
	generalCases := []struct {
		name  string
		left  []string
		right []string
		want  []bool
		nulls []bool
	}{
		{
			name:  "case and pad space",
			left:  []string{"Alpha", "Alpha ", "Alpha", "beta", ""},
			right: []string{"alpha", "alpha", "Alpha", "BETA", ""},
			want:  []bool{true, true, true, true, true},
			nulls: []bool{false, false, false, false, false},
		},
		{
			name:  "null propagates",
			left:  []string{"Alpha", "", ""},
			right: []string{"alpha", "Alpha", ""},
			want:  []bool{true, false, false},
			nulls: []bool{false, true, true},
		},
	}

	for _, tc := range generalCases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(general, tc.left, tc.nulls),
				NewFunctionTestInput(general, tc.right, tc.nulls),
			}
			expect := NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, tc.nulls)
			testCase := NewFunctionTestCase(proc, inputs, expect, CollationKeyEqual)
			ok, info := testCase.Run()
			require.True(t, ok, info)
		})
	}

	bin := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetUTF8MB4Bin)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(bin, []string{"Alpha", "Alpha ", "A"}, []bool{false, false, false}),
		NewFunctionTestInput(bin, []string{"alpha", "Alpha", "A "}, []bool{false, false, false}),
	}
	expect := NewFunctionTestResult(types.T_bool.ToType(), false,
		[]bool{false, true, true}, []bool{false, false, false})
	testCase := NewFunctionTestCase(proc, inputs, expect, CollationKeyNullSafeEqual)
	ok, info := testCase.Run()
	require.True(t, ok, info)
}

func TestCollationKeyComparatorRejectsLegacyDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	legacy := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetLegacy)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(legacy, []string{"Alpha"}, []bool{false}),
		NewFunctionTestInput(legacy, []string{"alpha"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_bool.ToType(), true, nil, nil)
	testCase := NewFunctionTestCase(proc, inputs, expect, CollationKeyEqual)
	ok, info := testCase.Run()
	require.True(t, ok, info)
}

func TestCollationKeyDomainRejectsUnsupportedTypes(t *testing.T) {
	accepted := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetUTF8)
	for _, typ := range []types.T{
		types.T_char,
		types.T_blob,
		types.T_binary,
		types.T_varbinary,
		types.T_json,
		types.T_int64,
	} {
		rejected := types.NewWithCharset(typ, 32, 0, types.CharsetUTF8)
		_, ok := collationKeyTextDomain(accepted, rejected)
		require.Falsef(t, ok, "type %s must remain on its conservative comparison path", typ)
	}

	otherCharset := types.NewWithCharset(types.T_text, 32, 0, types.CharsetUTF8MB4Bin)
	_, ok := collationKeyTextDomain(accepted, otherCharset)
	require.False(t, ok, "mixed comparison domains must fail closed")
}

func TestCollationKeyNullSafeHonorsSelectionMask(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	typ := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetUTF8)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(typ, []string{"Alpha", "beta"}, []bool{false, false}),
		NewFunctionTestInput(typ, []string{"alpha", "BETA"}, []bool{false, false}),
	}
	// SelectList marks row 1 as not selected.  The comparator must not run for
	// that row and the expression framework represents it as a NULL result.
	testCase := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, true}),
		CollationKeyNullSafeEqual).WithSelectList(&FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{true, false},
	})
	ok, info := testCase.Run()
	require.True(t, ok, info)
}

func TestCollationKeyNullSafeSelectionMaskConstAndStaleRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	typ := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetUTF8MB4Bin)
	t.Run("all rows masked", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(typ, []string{"Alpha", "beta"}, []bool{false, false}),
			NewFunctionTestInput(typ, []string{"Alpha", "BETA"}, []bool{false, false}),
		}
		testCase := NewFunctionTestCase(proc, inputs,
			NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false}, []bool{true, true}),
			CollationKeyNullSafeEqual).WithSelectList(&FunctionSelectList{
			AnyNull:    true,
			AllNull:    true,
			SelectList: []bool{false, false},
		})
		ok, info := testCase.Run()
		require.True(t, ok, info)
	})

	t.Run("constant left", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestConstInput(typ, []string{"Alpha"}, []bool{false}),
			NewFunctionTestInput(typ, []string{"Alpha", "alpha"}, []bool{false, false}),
		}
		testCase := NewFunctionTestCase(proc, inputs,
			NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false}, []bool{false, true}),
			CollationKeyNullSafeEqual).WithSelectList(&FunctionSelectList{
			AnyNull:    true,
			SelectList: []bool{true, false},
		})
		ok, info := testCase.Run()
		require.True(t, ok, info)
	})

	t.Run("stale tail is ignored", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(typ, []string{"Alpha", "Alpha"}, []bool{false, false}),
			NewFunctionTestInput(typ, []string{"alpha", "Alpha"}, []bool{false, false}),
		}
		testCase := NewFunctionTestCase(proc, inputs,
			NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true}, []bool{true, false}),
			CollationKeyNullSafeEqual).WithSelectList(&FunctionSelectList{
			AnyNull: true,
			// The third entry is from a previous, longer batch and must not
			// affect this two-row evaluation.
			SelectList: []bool{false, true, false},
		})
		ok, info := testCase.Run()
		require.True(t, ok, info)
	})
}
