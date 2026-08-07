// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package plan

import (
	"context"
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func findAggregateByName(query *planpb.Query, name string) *planpb.Function {
	for _, node := range query.Nodes {
		for _, expr := range node.AggList {
			if fn := expr.GetF(); fn != nil && strings.EqualFold(fn.Func.GetObjName(), name) {
				return fn
			}
		}
	}
	return nil
}

func TestBuildOrderedSetAggregates(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	for _, tc := range []struct {
		name      string
		sql       string
		desc      byte
		wantOrder bool
	}{
		{name: "continuous", sql: "select percentile_cont(0.5) within group (order by a) from select_test.bind_select"},
		{name: "discrete descending", sql: "select percentile_disc(0.5) within group (order by a desc) from select_test.bind_select", desc: 1},
		{name: "group concat within group", sql: "select group_concat(a) within group (order by b desc) from select_test.bind_select", wantOrder: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			queryPlan, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)

			name := "percentile_cont"
			if strings.Contains(tc.sql, "percentile_disc") {
				name = "percentile_disc"
			} else if strings.Contains(tc.sql, "group_concat") {
				name = "group_concat"
			}
			fn := findAggregateByName(queryPlan.GetQuery(), name)
			require.NotNil(t, fn)
			if tc.wantOrder {
				require.NotEqual(t, planpb.AggregateConfigType_AGG_CONFIG_NONE, fn.AggConfigType)
				require.NotEmpty(t, fn.AggConfig)
				return
			}
			require.Len(t, fn.Args, 2)
			require.Equal(t, tc.desc, fn.AggConfig[0])
		})
	}
}

func TestBuildOrderedSetPercentileRejectsNonConstant(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select percentile_cont(b) within group (order by a) from select_test.bind_select", 1)
	require.NoError(t, err)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "percentile argument of percentile_cont must be a non-null constant")
}

func TestBuildOrderedSetPercentileRejectsWindowForm(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select percentile_cont(0.5) within group (order by a) over () from select_test.bind_select", 1)
	require.NoError(t, err)
	_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.ErrorContains(t, err, "ordered-set percentile window functions")
}
