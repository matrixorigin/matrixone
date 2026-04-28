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
