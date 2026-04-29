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

package mysql

import (
	"context"
	"testing"
)

func TestIssue24235IntervalParenthesizedModulo(t *testing.T) {
	sqls := []string{
		"SELECT DATE_SUB(CURRENT_DATE, INTERVAL (DAYOFWEEK(CURRENT_DATE) + 5) % 7 DAY)",
		"SELECT DATE_SUB(CURRENT_DATE, INTERVAL (1 + 2) % 3 DAY)",
		"SELECT DATE_SUB(CURRENT_DATE, INTERVAL (DAYOFWEEK(CURRENT_DATE) + 5) % 7 + 7 DAY)",
		"SELECT DATE_ADD(CURRENT_DATE, INTERVAL (1 + 2) % 3 - 1 DAY)",
		"SELECT DATE_SUB(CURRENT_DATE, INTERVAL /*comment*/ (1 + 2) % 3 DAY)",
		"SELECT DATE_SUB(CURRENT_DATE, INTERVAL (1 + 2) /*comment*/ % 3 DAY)",
		"SELECT DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)",
		"SELECT DATE_SUB(CURRENT_DATE, INTERVAL 7 % 3 DAY)",
	}
	for _, sql := range sqls {
		if _, err := ParseOne(context.Background(), sql, 1); err != nil {
			t.Fatalf("ParseOne(%q) failed: %v", sql, err)
		}
	}
}

func TestNormalizeIntervalParenthesizedModulo(t *testing.T) {
	sql := "SELECT 'INTERVAL (1 + 2) % 3 DAY', DATE_SUB(CURRENT_DATE, INTERVAL (1 + (2)) % 3 DAY)"
	want := "SELECT 'INTERVAL (1 + 2) % 3 DAY', DATE_SUB(CURRENT_DATE, INTERVAL +(1 + (2)) % 3 DAY)"
	if got := normalizeIntervalParenthesizedModulo(sql); got != want {
		t.Fatalf("normalizeIntervalParenthesizedModulo() = %q, want %q", got, want)
	}
}

func TestNormalizeIntervalParenthesizedModuloWithComments(t *testing.T) {
	sql := "SELECT DATE_SUB(CURRENT_DATE, INTERVAL /*a*/ (1 + 2) /*b*/ % 3 DAY)"
	want := "SELECT DATE_SUB(CURRENT_DATE, INTERVAL /*a*/ +(1 + 2) /*b*/ % 3 DAY)"
	if got := normalizeIntervalParenthesizedModulo(sql); got != want {
		t.Fatalf("normalizeIntervalParenthesizedModulo() with comments = %q, want %q", got, want)
	}
}

func TestNormalizeIntervalParenthesizedModuloNoChange(t *testing.T) {
	sql := "SELECT DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)"
	if got := normalizeIntervalParenthesizedModulo(sql); got != sql {
		t.Fatalf("normalizeIntervalParenthesizedModulo() = %q, want original %q", got, sql)
	}
}
