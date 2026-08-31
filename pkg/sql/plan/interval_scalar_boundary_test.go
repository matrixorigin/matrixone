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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestBuildPlanRejectsStandaloneIntervalAtScalarBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		clause string
	}{
		{
			name:   "projection",
			sql:    "select interval 1 day",
			clause: "SELECT list",
		},
		{
			name:   "group key",
			sql:    "select 1 from select_test.bind_select group by interval 1 day",
			clause: "GROUP BY",
		},
		{
			name:   "sort key",
			sql:    "select a from select_test.bind_select order by interval 1 day",
			clause: "ORDER BY",
		},
		{
			name:   "window partition key",
			sql:    "select row_number() over (partition by interval 1 day) from select_test.bind_select",
			clause: "window PARTITION BY",
		},
		{
			name:   "window sort key",
			sql:    "select row_number() over (order by interval 1 day) from select_test.bind_select",
			clause: "ORDER BY",
		},
		{
			name:   "grouping set sort key",
			sql:    "select a from select_test.bind_select group by grouping sets ((a)) order by interval 1 day",
			clause: "ORDER BY",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
			require.ErrorContains(t, err, "standalone INTERVAL expression in "+test.clause)
		})
	}
}

func TestBuildPlanAllowsConsumedIntervalExpressions(t *testing.T) {
	tests := []string{
		"select date_add('2026-01-01', interval 1 day)",
		"select date_add('2026-01-01', interval a day) from select_test.bind_select group by date_add('2026-01-01', interval a day)",
		"select row_number() over (partition by date_add('2026-01-01', interval a day)) from select_test.bind_select",
		"select row_number() over (order by date_add('2026-01-01', interval a day)) from select_test.bind_select",
		"select sum(a) over (order by cast('2026-01-01' as timestamp) range between interval 1 day preceding and current row) from select_test.bind_select",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
		})
	}
}
