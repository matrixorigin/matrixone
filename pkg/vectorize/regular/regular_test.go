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

package regular

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Regular_Instr UT tests
func TestRegularInstrTest1(t *testing.T) {
	//Test values
	expr := "Cat"
	pat := "at"

	//Predefined Correct Values
	expected := int64(2)

	result, _ := RegularInstr(expr, pat, 1, 1, 0, "")
	require.Equal(t, expected, result)
}

func TestRegularInstrTest2(t *testing.T) {
	//Test values
	expr := "at"
	pat := "^at"

	//Predefined Correct Values
	expected := int64(1)

	result, _ := RegularInstr(expr, pat, 1, 1, 0, "")
	require.Equal(t, expected, result)
}

func TestRegularInstrTest3(t *testing.T) {
	//Test values
	expr := "Cat Cat"
	pat := "Cat"

	//Predefined Correct Values
	expected := int64(5)

	result, _ := RegularInstr(expr, pat, 2, 1, 0, "")
	require.Equal(t, expected, result)
}

func TestRegularInstrTest4(t *testing.T) {
	//Test values
	expr := "Cat Cat"
	pat := "Cat"

	//Predefined Correct Values
	expected := int64(5)

	result, _ := RegularInstr(expr, pat, 3, 1, 0, "")
	require.Equal(t, expected, result)
}

func TestRegularInstrTest5(t *testing.T) {
	//Test values
	expr := "Cat City is SO Cute!"
	pat := "C.t"

	//different pos
	pos := []int64{1, 2, 6}
	//Predefined Correct Values
	result := make([]int64, 3)
	expected := []int64{1, 5, 16}
	for i := range pos {
		result[i], _ = RegularInstr(expr, pat, pos[i], 1, 0, "")
	}

	require.Equal(t, expected, result)
}

func TestRegularInstrTest6(t *testing.T) {
	//Test values
	expr := "Cat City is SO Cute!"
	pat := "C.t"

	//different pos
	pos := []int64{1, 1, 1}
	occ := []int64{1, 2, 3}
	//Predefined Correct Values
	result := make([]int64, 3)
	expected := []int64{1, 5, 16}
	for i := range pos {
		result[i], _ = RegularInstr(expr, pat, pos[i], occ[i], 0, "")
	}

	require.Equal(t, expected, result)
}

func TestRegularInstrTest7(t *testing.T) {
	//Test values
	expr := "Cat City is SO Cute!"
	pat := "C.t"

	//different pos
	pos := []int64{2, 2, 2}
	occ := []int64{1, 2, 3}
	//Predefined Correct Values
	result := make([]int64, 3)
	expected := []int64{5, 16, 0}
	for i := range pos {
		result[i], _ = RegularInstr(expr, pat, pos[i], occ[i], 0, "")
	}

	require.Equal(t, expected, result)
}

func TestRegularInstrTest8(t *testing.T) {
	//Test values
	expr := "Cat City is SO Cute!"
	pat := "C.t"

	//different pos
	pos := []int64{1, 1, 1}
	occ := []int64{1, 2, 3}
	opt := []uint8{0, 0, 0}
	//Predefined Correct Values
	result := make([]int64, 3)
	expected := []int64{1, 5, 16}
	for i := range pos {
		result[i], _ = RegularInstr(expr, pat, pos[i], occ[i], opt[i], "")
	}

	require.Equal(t, expected, result)
}
func TestRegularInstrTest9(t *testing.T) {
	//Test values
	expr := "Cat City is SO Cute!"
	pat := "C.t"

	//different pos
	pos := []int64{1, 1, 1}
	occ := []int64{1, 2, 3}
	opt := []uint8{1, 1, 1}
	//Predefined Correct Values"
	result := make([]int64, 3)
	expected := []int64{4, 8, 19}
	for i := range pos {
		result[i], _ = RegularInstr(expr, pat, pos[i], occ[i], opt[i], "")
	}

	require.Equal(t, expected, result)
}

// Regular_Like UT tests
func TestRegularLikerTest1(t *testing.T) {
	//Test values
	expr := []string{"Cat", "Cat", "Cat", "Cat"}
	pat := []string{".*", "b+", "^Ca", "^Da"}

	//Predefined Correct Values
	expected := []bool{true, false, true, false}
	result := make([]bool, len(expr))
	for i := range expr {
		result[i], _ = RegularLike(expr[i], pat[i], "")
	}
	require.Equal(t, expected, result)
}

func TestRegularLikeTest2(t *testing.T) {
	// Test for case insensitive matching.
	expr := []string{"Cat"}
	pat := []string{"cat"}
	matchType := []string{"", "i"}

	expected := []bool{false, true}
	result := make([]bool, len(matchType))
	for i := range matchType {
		result[i], _ = RegularLike(expr[0], pat[0], matchType[i])
	}
	require.Equal(t, expected, result)
}

func TestRegularLikeTest3(t *testing.T) {
	// Test for . match line-terminator.
	expr := []string{"\n"}
	pat := []string{"."}
	matchType := []string{"", "n"}

	expected := []bool{false, true}
	result := make([]bool, len(matchType))
	for i := range matchType {
		result[i], _ = RegularLike(expr[0], pat[0], matchType[i])
	}
	require.Equal(t, expected, result)
}

func TestRegularLikeTest4(t *testing.T) {
	// Test for multi-line mode.
	expr := []string{"last\nday"}
	pat := []string{"last$"}
	matchType := []string{"", "m"}

	expected := []bool{false, true}
	result := make([]bool, len(matchType))
	for i := range matchType {
		result[i], _ = RegularLike(expr[0], pat[0], matchType[i])
	}
	require.Equal(t, expected, result)
}

func TestRegularLikeTest5(t *testing.T) {
	// Test for right-most rule.
	expr := []string{"ABC"}
	pat := []string{"abc"}
	matchType := []string{"icicc", "ccici"}

	expected := []bool{false, true}
	result := make([]bool, len(matchType))
	for i := range matchType {
		result[i], _ = RegularLike(expr[0], pat[0], matchType[i])
	}
	require.Equal(t, expected, result)
}

// Regular_Replace UT tests
func TestRegularReplaceTest1(t *testing.T) {
	//Test values
	expr := "1abc2"
	pat := "[0-9]"
	repl := "#"

	//Predefined Correct Values
	expected := "#abc2"
	result, _ := RegularReplace(expr, pat, repl, 1, 1, "")

	require.Equal(t, expected, result)
}

func TestRegularReplaceTest2(t *testing.T) {
	//Test values
	expr := "12abc"
	pat := "[0-9]"
	repl := "#"

	//Predefined Correct Values
	expected := "1#abc"
	result, _ := RegularReplace(expr, pat, repl, 2, 1, "")

	require.Equal(t, expected, result)
}

func TestRegularReplaceTest3(t *testing.T) {
	//Test values
	expr := []string{"01234abcde56789", "01234abcde56789"}
	pat := []string{"[0-9]", "[09]"}
	repl := []string{"#", "#"}

	//Predefined Correct Values
	expected := []string{"#1234abcde56789", "#1234abcde56789"}
	result := make([]string, len(expr))
	for i := range expr {
		result[i], _ = RegularReplace(expr[i], pat[i], repl[i], 1, 1, "")
	}
	require.Equal(t, expected, result)
}

func TestRegularReplaceTest4(t *testing.T) {
	//Test values
	expr := []string{"abcdefg123456ABC", "abcDEfg123456ABC", "abcDEfg123456ABC", "abcDefg123456ABC"}
	pat := []string{"[0-9]", "[0-9]", "[0-9]", "[0-9]"}
	repl := []string{"", "", "", ""}

	//Predefined Correct Values
	expected := []string{"abcdefgABC", "abcDEfgABC", "abcDEfgABC", "abcDefgABC"}
	pos := []int64{4, 4, 7, 10}
	result := make([]string, len(expr))
	for i := range expr {
		result[i], _ = RegularReplace(expr[i], pat[i], repl[i], pos[i], 0, "")
	}
	require.Equal(t, expected, result)
}

// Regular_Substr UT tests
func TestRegularSubstrTest1(t *testing.T) {
	//Test values
	expr := []string{"abc def ghi", "abc def ghi"}
	pat := []string{"[a-z]+", "[a-z]+"}
	pos := []int64{1, 1}
	occ := []int64{1, 3}

	//Predefined Correct Values
	expected := []string{"abc", "ghi"}
	result := make([]string, len(expr))
	for i := range expr {
		temp, _ := RegularSubstr(expr[i], pat[i], pos[i], occ[i], "")
		result[i] = temp[occ[i]-1]
	}
	require.Equal(t, expected, result)
}

func TestRegularSubstrTest2(t *testing.T) {
	//Test values
	expr := []string{"java t point", "my sql function"}
	pat := []string{"[a-z]+", "[a-z]+"}
	pos := []int64{2, 1}
	occ := []int64{3, 3}

	//Predefined Correct Values
	expected := []string{"point", "function"}
	result := make([]string, len(expr))
	for i := range expr {
		temp, _ := RegularSubstr(expr[i], pat[i], pos[i], occ[i], "")
		result[i] = temp[occ[i]-1]
	}
	require.Equal(t, expected, result)
}
