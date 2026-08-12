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

// Boolean query-tree printer — ported verbatim from classic fulltext
// (pkg/fulltext/fulltext_test.go TestPatternBoolean / TestPatternPhrase).
// BooleanTreeString reuses fulltext.PatternToString, so the printed S-expression
// is identical to classic.
package fulltext2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBooleanTreeString(t *testing.T) {
	cases := []struct {
		pattern string
		expect  string
	}{
		{"Ma'trix Origin", "(text 0 ma'trix) (text 1 origin)"},
		{"Matrix Origin", "(text 0 matrix) (text 1 origin)"},
		{"+Matrix Origin", "(+ (text 0 matrix)) (text 1 origin)"},
		{"+Matrix -Origin", "(+ (text 0 matrix)) (- (text 1 origin))"},
		{"Matrix ~Origin", "(text 0 matrix) (~ (text 1 origin))"},
		{"Matrix +(<Origin >One)", "(+ (group (< (text 0 origin)) (> (text 1 one)))) (text 2 matrix)"},
		{"+Matrix +Origin", "(join 0 (+ (text 0 matrix)) (+ (text 0 origin)))"},
		{`"Matrix origin"`, "(phrase (text 0 matrix) (text 1 origin))"},
		{"Matrix Origin*", "(text 0 matrix) (* 1 origin*)"},
		{"+Matrix +(Origin (One Two))", "(+ (text 0 matrix)) (+ (group (text 1 origin) (group (text 2 one) (text 3 two))))"},
		{"+读写汉字 -学中文", "(+ (text 0 读写汉字)) (- (text 1 学中文))"},
		{"+读书会 +提效 +社群 +案例 +运营", "(join 0 (+ (text 0 读书会)) (+ (text 0 提效)) (+ (text 0 社群)) (+ (text 0 案例)) (+ (text 0 运营)))"},
	}
	for _, c := range cases {
		got, err := BooleanTreeString(c.pattern)
		require.NoError(t, err, c.pattern)
		require.Equal(t, c.expect, got, "pattern %q", c.pattern)
	}
}

func TestBooleanTreeStringWithPosition(t *testing.T) {
	cases := []struct {
		pattern string
		expect  string
	}{
		{`"Ma'trix     Origin"`, "(phrase (text 0 0 ma'trix) (text 1 12 origin))"},
		{`"Matrix Origin"`, "(phrase (text 0 0 matrix) (text 1 7 origin))"},
		{`"Matrix"`, "(phrase (text 0 0 matrix))"},
		{`"    Matrix     "`, "(phrase (text 0 0 matrix))"},
		{`"Matrix     Origin"`, "(phrase (text 0 0 matrix) (text 1 11 origin))"},
		{`"  你好嗎? Hello World  在一起  Happy  再见  "`,
			"(phrase (text 0 0 你好嗎?) (text 1 11 hello) (text 2 17 world) (text 3 24 在一起) (text 4 35 happy) (text 5 42 再见))"},
	}
	for _, c := range cases {
		got, err := BooleanTreeStringWithPosition(c.pattern)
		require.NoError(t, err, c.pattern)
		require.Equal(t, c.expect, got, "pattern %q", c.pattern)
	}
}

// Ported from classic TestPatternNL: NL-mode ngram/word expansion with positions
// (short tokens → * prefix, CJK → 3-grams, mixed CJK/latin).
func TestNLTreeStringWithPosition(t *testing.T) {
	cases := []struct {
		pattern string
		expect  string
	}{
		{"Ma'trix Origin", "(* 0 0 ma*) (text 1 3 trix) (text 2 8 origin)"},
		{"Matrix Origin", "(text 0 0 matrix) (text 1 7 origin)"},
		{"读写汉字 学中文", "(text 0 0 读写汉) (text 1 3 写汉字) (text 2 13 学中文)"},
		{"读写", "(* 0 0 读写*)"},
		{"肥胖的原因都是因为摄入脂肪多导致的吗",
			"(text 0 0 肥胖的) (text 1 9 原因都) (text 2 18 是因为) (text 3 27 摄入脂) (text 4 36 肪多导) (text 5 45 致的吗)"},
		{"肥胖的原因都是因为摄入fat多导致的吗",
			"(text 0 0 肥胖的) (text 1 9 原因都) (text 2 18 是因为) (text 3 24 为摄入) (text 4 33 fat) (text 5 36 多导致) (text 6 42 致的吗)"},
	}
	for _, c := range cases {
		got, err := NLTreeStringWithPosition(c.pattern)
		require.NoError(t, err, c.pattern)
		require.Equal(t, c.expect, got, "pattern %q", c.pattern)
	}
}

// Ported from classic TestPatternFail: malformed boolean queries error.
func TestBooleanTreeStringFail(t *testing.T) {
	for _, p := range []string{
		"Matrix Origin( ", "(+Matrix Origin", "++Matrix -Origin",
		"Matrix ~~Origin", "Matrix +(<(+Origin -apple) >One)", "+Matrix --Origin",
	} {
		_, err := BooleanTreeString(p)
		require.Error(t, err, "pattern %q should fail", p)
	}
}

// Ported from classic TestPatternNLFail: malformed NL queries error.
func TestNLTreeStringFail(t *testing.T) {
	for _, p := range []string{"+[[[", "+''"} {
		_, err := NLTreeString(p)
		require.Error(t, err, "pattern %q should fail", p)
	}
}
