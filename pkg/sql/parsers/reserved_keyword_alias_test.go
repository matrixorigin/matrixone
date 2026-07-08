// Copyright 2021 Matrix Origin
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

package parsers

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// TestReservedKeywordAlias verifies that reserved keywords can be used
// as column aliases when preceded by AS (issue #24410).
func TestReservedKeywordAlias(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"AS current_time", "SELECT NOW() AS current_time"},
		{"AS rows", "SELECT 1 AS rows"},
		{"AS rows with GROUP BY", "SELECT pump, COUNT(*) AS rows FROM t GROUP BY pump"},
		{"AS current_timestamp", "SELECT NOW() AS current_timestamp"},
		{"AS localtime", "SELECT NOW() AS localtime"},
		{"AS localtimestamp", "SELECT NOW() AS localtimestamp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseOne(context.Background(), dialect.MYSQL, tt.sql, 1)
			if err != nil {
				t.Errorf("unexpected parse error for %q: %v", tt.sql, err)
			}
		})
	}
}

// TestReservedKeywordRegression verifies that reserved keywords still work
// in their original contexts after the alias fix.
func TestReservedKeywordRegression(t *testing.T) {
	tests := []string{
		"SELECT CURRENT_TIME()",
		"SELECT CURRENT_TIMESTAMP()",
		"SELECT ROW_NUMBER() OVER (ORDER BY 1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			if err != nil {
				t.Errorf("unexpected parse error for %q: %v", sql, err)
			}
		})
	}
}
