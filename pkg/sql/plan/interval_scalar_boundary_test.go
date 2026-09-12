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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestBindFuncExprImplRejectsNestedStandaloneInterval(t *testing.T) {
	ctx := context.Background()
	nested := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Args: []*planpb.Expr{MakeIntervalExpr(1, "day")},
		}},
	}

	_, err := BindFuncExprImplByPlanExpr(ctx, "not", []*planpb.Expr{nested})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
	require.ErrorContains(t, err, "standalone INTERVAL expression in NOT argument")

	_, err = BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
		MakeIntervalExpr(1, "day"),
		MakeIntervalExpr(2, "day"),
	})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg), err)
	require.ErrorContains(t, err, "invalid argument operator +, bad value [INTERVAL INTERVAL]")
}

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
			name:   "generic scalar argument",
			sql:    "select interval 1 day is null",
			clause: "ISNULL argument",
		},
		{
			name:   "unknown function cannot reach udf resolution",
			sql:    "select missing_interval_udf(interval 1 day)",
			clause: "MISSING_INTERVAL_UDF argument",
		},
		{
			name:   "aggregate argument",
			sql:    "select count(interval 1 day) from select_test.bind_select",
			clause: "COUNT argument",
		},
		{
			name:   "aggregate nested tuple argument",
			sql:    "select count(distinct (interval 1 day, a)) from select_test.bind_select",
			clause: "COUNT argument",
		},
		{
			name:   "where predicate",
			sql:    "select a from select_test.bind_select where interval 1 day",
			clause: "predicate",
		},
		{
			name:   "having predicate",
			sql:    "select count(*) from select_test.bind_select having interval 1 day",
			clause: "predicate",
		},
		{
			name:   "subquery comparison predicate",
			sql:    "select a from select_test.bind_select where interval 1 day in (select a from select_test.bind_select)",
			clause: "IN argument",
		},
		{
			name:   "not in subquery comparison predicate",
			sql:    "select a from select_test.bind_select where interval 1 day not in (select a from select_test.bind_select)",
			clause: "NOT_IN argument",
		},
		{
			name:   "quantified subquery comparison predicate",
			sql:    "select a from select_test.bind_select where interval 1 day = any (select a from select_test.bind_select)",
			clause: "= argument",
		},
		{
			name:   "join predicate",
			sql:    "select l.a from select_test.bind_select l join select_test.bind_select r on interval 1 day",
			clause: "predicate",
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
		"select date_add('2026-01-01', interval 1 day) is null",
		"select '2026-01-01' + interval 1 day",
		"select cast(20260515 as int) + interval 1 day",
		"select interval 1 day + cast(20260515 as int)",
		"select count(date_add('2026-01-01', interval 1 day)) from select_test.bind_select",
		"select interval(23, 1, 15, 17)",
		"select a from select_test.bind_select where date_add('2026-01-01', interval 1 day) in (select '2026-01-02' from select_test.bind_select)",
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

func TestBuildPlanPreservesInvalidIntervalFunctionDiagnostics(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "integer plus sub-day interval",
			sql:  "select cast(20260515 as int) + interval 1 hour",
			want: "invalid argument operator +, bad value [INT INTERVAL]",
		},
		{
			name: "integer minus sub-day interval",
			sql:  "select cast(20260515 as int) - interval 30 minute",
			want: "invalid argument operator -, bad value [INT INTERVAL]",
		},
		{
			name: "reversed sub-day interval",
			sql:  "select interval 1 second + cast(20260515 as int)",
			want: "invalid argument operator +, bad value [INTERVAL INT]",
		},
		{
			name: "uuid v4 interval",
			sql:  "select uuid_v4(interval 1 minute)",
			want: "invalid argument function uuid_v4, bad value [INTERVAL]",
		},
		{
			name: "greatest mixed json date interval",
			sql:  `select greatest(json_extract('"2020-01-02"', '$'), cast('2020-01-01' as date), interval 1 day)`,
			want: "invalid argument function greatest, bad value [JSON DATE INTERVAL]",
		},
		{
			name: "greatest intervals",
			sql:  "select greatest(interval 1 day, interval 2 day)",
			want: "invalid argument function greatest, bad value [INTERVAL INTERVAL]",
		},
		{
			name: "greatest interval and date",
			sql:  "select greatest(interval 1 day, cast('2020-01-01' as date))",
			want: "invalid argument function greatest, bad value [INTERVAL DATE]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg), err)
			require.ErrorContains(t, err, test.want)
		})
	}
}
