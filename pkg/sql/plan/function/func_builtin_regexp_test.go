// Copyright 2022 Matrix Origin
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
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_BuiltIn_RegularInstr(t *testing.T) {
	op := newOpBuiltInRegexp()

	cs := []struct {
		pat       string
		str       string
		pos       int64
		ocr       int64
		retOption int8
		expected  int64
	}{
		{pat: "at", str: "Cat", pos: 1, ocr: 1, retOption: 0, expected: 2},
		{pat: "^at", str: "at", pos: 1, ocr: 1, retOption: 0, expected: 1},
		{pat: "Cat", str: "Cat Cat", pos: 2, ocr: 1, retOption: 0, expected: 5},
		{pat: "Cat", str: "Cat Cat", pos: 3, ocr: 1, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 1, retOption: 0, expected: 1},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 1, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 6, ocr: 1, retOption: 0, expected: 16},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 1, retOption: 0, expected: 1},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 2, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 3, retOption: 0, expected: 16},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 1, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 2, retOption: 0, expected: 16},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 3, retOption: 0, expected: 0},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 1, retOption: 1, expected: 4},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 2, retOption: 1, expected: 8},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 3, retOption: 1, expected: 19},
	}

	for i, c := range cs {
		v, err := op.regMap.regularInstr(c.pat, c.str, c.pos, c.ocr, c.retOption)
		require.NoError(t, err)
		require.Equal(t, c.expected, v, i)
	}

	_, err := op.regMap.regularInstr("at", "Cat", 100, 1, 0)
	require.True(t, err != nil)
}

func Test_BuiltIn_RegularLike(t *testing.T) {
	op := newOpBuiltInRegexp()

	cs := []struct {
		pat       string
		str       string
		matchType string
		expected  bool
	}{
		{pat: ".*", str: "Cat", matchType: "c", expected: true},
		{pat: "b+", str: "Cat", matchType: "c", expected: false},
		{pat: "^Ca", str: "Cat", matchType: "c", expected: true},
		{pat: "^Da", str: "Cat", matchType: "c", expected: false},
		{pat: "cat", str: "Cat", matchType: "", expected: false},
		{pat: "cat", str: "Cat", matchType: "i", expected: true},
		{pat: ".", str: "\n", matchType: "", expected: false},
		{pat: ".", str: "\n", matchType: "n", expected: true},
		{pat: "last$", str: "last\nday", matchType: "", expected: false},
		{pat: "last$", str: "last\nday", matchType: "m", expected: true},
		{pat: "abc", str: "ABC", matchType: "icicc", expected: false},
		{pat: "abc", str: "ABC", matchType: "ccici", expected: true},
	}

	for i, c := range cs {
		match, err := op.regMap.regularLike(c.pat, c.str, c.matchType)
		require.NoError(t, err, i)
		require.Equal(t, c.expected, match, i)
	}

}

func Test_BuiltIn_RegularReplace(t *testing.T) {
	op := newOpBuiltInRegexp()

	cs := []struct {
		pat      string
		str      string
		repl     string
		pos      int64
		ocr      int64
		expected string
	}{
		{pat: "[0-9]", str: "1abc2", repl: "#", pos: 1, ocr: 1, expected: "#abc2"},
		{pat: "[0-9]", str: "12abc", repl: "#", pos: 2, ocr: 1, expected: "1#abc"},
		{pat: "[0-9]", str: "01234abcde56789", repl: "#", pos: 1, ocr: 1, expected: "#1234abcde56789"},
		{pat: "[09]", str: "01234abcde56789", repl: "#", pos: 1, ocr: 1, expected: "#1234abcde56789"},
		{pat: "[0-9]", str: "abcdefg123456ABC", repl: "", pos: 4, ocr: 0, expected: "abcdefgABC"},
		{pat: "[0-9]", str: "abcDEfg123456ABC", repl: "", pos: 4, ocr: 0, expected: "abcDEfgABC"},
		{pat: "[0-9]", str: "abcDEfg123456ABC", repl: "", pos: 7, ocr: 0, expected: "abcDEfgABC"},
		{pat: "[0-9]", str: "abcDefg123456ABC", repl: "", pos: 10, ocr: 0, expected: "abcDefgABC"},
	}

	for i, c := range cs {
		val, err := op.regMap.regularReplace(c.pat, c.str, c.repl, c.pos, c.ocr)
		require.NoError(t, err, i)
		require.Equal(t, c.expected, val, i)
	}
}

func Test_BuiltIn_RegularSubstr(t *testing.T) {
	op := newOpBuiltInRegexp()

	cc := []struct {
		pat      string
		str      string
		pos      int64
		ocr      int64
		expected string
	}{
		{pat: "[a-z]+", str: "abc def ghi", pos: 1, ocr: 1, expected: "abc"},
		{pat: "[a-z]+", str: "abc def ghi", pos: 1, ocr: 3, expected: "ghi"},
		{pat: "[a-z]+", str: "java t point", pos: 2, ocr: 3, expected: "point"},
		{pat: "[a-z]+", str: "my sql function", pos: 1, ocr: 3, expected: "function"},
	}

	for i, c := range cc {
		match, val, err := op.regMap.regularSubstr(c.pat, c.str, c.pos, c.ocr)
		require.NoError(t, err, i)
		require.True(t, match, i)
		require.Equal(t, c.expected, val, i)
	}
}
