// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"maps"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldProducerProvenance(t *testing.T) {
	for _, tc := range []struct {
		name, sql     string
		valueFirst    bool
		first, want   types.T
		textCondition bool
	}{
		{"LAG marker", "select field(case when ? then lag(?,0) over () else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"LEAD marker", "select field(case when ? then lead(?,0) over () else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"LAG NULL default", "select field(case when ? then lag(?,0,NULL) over () else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"LEAD NULL default", "select field(case when ? then lead(?,0,NULL) over () else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"LAG NULL default text condition", "select field(case when ? then lag(?,0,NULL) over () else null end,'A','a')", false, types.T_any, types.T_blob, true},
		{"LAG text default", "select field(case when ? then lag(?,0,'x') over () else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"LAG binary default", "select field(case when ? then lag(?,0,_binary 'x') over () else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"LAG compound text default", "select field(case when ? then lag(coalesce(?,null),0,'x') over () else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"LAG compound", "select field(case when ? then lag(coalesce(?,null),0) over () else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"LEAD compound", "select field(case when ? then lead(coalesce(?,null),0) over () else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"CONCAT marker", "select field(case when ? then concat(?) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT compound", "select field(case when ? then concat(coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"CONCAT literal peer", "select field(case when ? then concat(?,'') else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT compound literal peer", "select field(case when ? then concat(coalesce(?,null),'') else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT_WS skips NULL compound", "select field(case when ? then concat_ws('',coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT_WS LOWER NULL", "select field(case when ? then concat_ws('',lower(coalesce(?,NULL))) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT_WS LOWER NULL text condition", "select field(case when ? then concat_ws('',lower(coalesce(?,NULL))) else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS REVERSE NULL", "select field(case when ? then concat_ws('',reverse(coalesce(?,NULL))) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT_WS CONCAT NULL", "select field(case when ? then concat_ws('',concat(coalesce(?,NULL))) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT_WS nested NULL", "select field(case when ? then concat_ws('',lower(reverse(coalesce(?,NULL)))) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT_WS LOWER text first", "select field(case when ? then concat_ws('',lower(coalesce(?,NULL))) else null end,'A','a')", false, types.T_varchar, types.T_text, true},
		{"CONCAT_WS LOWER binary first", "select field(case when ? then concat_ws('',lower(coalesce(?,NULL))) else null end,'A','a')", false, types.T_blob, types.T_text, true},
		{"CONCAT_WS COALESCE real text peer", "select field(case when ? then concat_ws('',coalesce(lower(coalesce(?,NULL)),'a')) else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS CONCAT real binary peer", "select field(case when ? then concat_ws('',concat(coalesce(?,NULL),_binary '')) else null end,'A','a')", false, types.T_any, types.T_blob, true},
		{"CONCAT_WS scalar inferred BLOB", "select field(case when ? then concat_ws('',(select lower(coalesce(?,NULL)) limit 1)) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT_WS scalar declaration", "select field(case when ? then concat_ws('',(select concat(coalesce(?,NULL)) limit 1)) else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS LAG inferred BLOB", "select field(case when ? then concat_ws('',lag(?,0,NULL) over ()) else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS ELT inferred", "select field(case when ? then concat_ws('',elt(1,coalesce(?,null))) else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS LTRIM inferred", "select field(case when ? then concat_ws('',ltrim(coalesce(?,null))) else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS ELT explicit binary", "select field(case when ? then concat_ws('',elt(1,coalesce(?,null),_binary '')) else null end,'A','a')", false, types.T_any, types.T_blob, true},
		{"CONCAT_WS inherent UNHEX binary", "select field(case when ? then concat_ws('',unhex(coalesce(?,null))) else null end,'A','a')", false, types.T_any, types.T_blob, true},
		{"CONCAT_WS UNION inferred", "select field(case when ? then concat_ws('',(select x from (select lower(coalesce(?,null)) x union all select null) d limit 1)) else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS UNION explicit binary", "select field(case when ? then concat_ws('',(select x from (select lower(coalesce(?,null)) x union all select _binary '') d limit 1)) else null end,'A','a')", false, types.T_any, types.T_blob, true},
		{"CONCAT_WS separator inferred", "select field(case when ? then concat_ws(lower(coalesce(?,null)),'a') else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS ELT separator inferred", "select field(case when ? then concat_ws(elt(1,coalesce(?,null)),'a') else null end,'A','a')", false, types.T_any, types.T_text, true},
		{"CONCAT_WS binary separator", "select field(case when ? then concat_ws(_binary '',lower(coalesce(?,NULL))) else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"CONCAT_WS explicit binary NULL", "select field(case when ? then concat_ws('',cast(NULL as binary),?) else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"LOWER compound", "select field(case when ? then lower(coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"REVERSE compound", "select field(case when ? then reverse(coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"direct ANY_VALUE marker", "select field(case when ? then any_value(?) else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"scalar ANY_VALUE marker", "select field(case when ? then (select any_value(?) limit 1) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"derived ANY_VALUE marker", "select field(case when ? then x else null end,'A','a') from (select any_value(?) x limit 1) d", false, types.T_any, types.T_text, false},
		{"ELT ANY_VALUE marker", "select field(case when ? then elt(1,any_value(?)) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"NULLIF numeric peer marker", "select field(case when ? then nullif(?,1) else null end,'A','a')", false, types.T_any, types.T_int64, false},
		{"ANY_VALUE NULLIF numeric peer", "select field(case when ? then any_value(nullif(?,1)) else null end,'A','a')", false, types.T_any, types.T_int64, false},
		{"NULLIF numeric peer compound", "select field(case when ? then nullif(coalesce(?,null),1) else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"ANY_VALUE NULLIF marker", "select field(case when ? then any_value(nullif(?,'x')) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"COALESCE ANY_VALUE marker", "select field(case when ? then coalesce(any_value(?),null) else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"direct ANY_VALUE compound", "select field(case when ? then any_value(coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"scalar ANY_VALUE compound", "select field(case when ? then (select any_value(coalesce(?,null)) limit 1) else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"scalar MAX marker", "select field(case when ? then (select max(?) limit 1) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"scalar MIN marker", "select field(case when ? then (select min(?) limit 1) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"scalar MAX compound", "select field(case when ? then (select max(coalesce(?,null)) limit 1) else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"UNION marker", "select field(case when ? then x else null end,'A','a') from (select ? x union all select null) d", false, types.T_any, types.T_text, false},
		{"UNION compound", "select field(case when ? then x else null end,'A','a') from (select coalesce(?,null) x union all select null) d", false, types.T_any, types.T_any, false},
		{"ELT literal NULL peer", "select field(case when ? then elt(1,?,null) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"ELT literal NULL before marker", "select field(case when ? then elt(2,null,?) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"ELT compound and literal NULL", "select field(case when ? then elt(1,coalesce(?,null),null) else null end,'A','a')", false, types.T_varchar, types.T_text, false},
		{"ELT compound", "select field(case when ? then elt(1,coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"ELT compound declaration", "select field(case when ? then elt(1,coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_blob, true},
		{"scalar compound", "select field(case when ? then (select coalesce(?,null) limit 1) else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"scalar marker", "select field(case when ? then (select ? limit 1) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"derived marker", "select field(case when ? then x else null end,'A','a') from (select ? x limit 1) d", false, types.T_any, types.T_text, false},
		{"CTE marker", "with d as (select ? x) select field(case when ? then x else null end,'A','a') from d", true, types.T_any, types.T_text, false},
		{"ELT text", "select field(case when ? then elt(1,?) else null end,'A','a')", false, types.T_varchar, types.T_text, false},
		{"ELT binary", "select field(case when ? then elt(1,?) else null end,'A','a')", false, types.T_blob, types.T_blob, false},
		{"NULLIF null marker", "select field(case when ? then nullif(?,'x') else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"NULLIF null composite", "select field(case when ? then nullif(coalesce(?,null),'x') else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"ELT null", "select field(case when ? then elt(1,?) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"ELT declaration without reprepare", "select field(case when ? then elt(1,?) else null end,'A','a')", false, types.T_blob, types.T_text, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			var snapshot *PreparedFieldTypes
			for _, oid := range []types.T{tc.first, types.T_varchar, types.T_blob} {
				var value any = "a"
				if oid == types.T_any {
					value = nil
				}
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
				values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}}
				if tc.textCondition {
					bindings[0].Type = types.T_varchar.ToType()
					values[0] = ParamValue{Value: "1", SourceType: bindings[0].Type, HasSourceType: true}
				}
				if tc.valueFirst {
					bindings[0].Type, bindings[1].Type = bindings[1].Type, bindings[0].Type
					values[0], values[1] = values[1], values[0]
				}
				ctx.SetContext(WithPreparedFieldTypes(context.Background(), snapshot))
				bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
				require.NoError(t, err)
				require.NotNil(t, bound.FieldTypes)
				require.Len(t, bound.FieldTypes.cases, 1)
				require.Equal(t, tc.want, bound.FieldTypes.cases[0].result.Oid)
				if snapshot != nil {
					require.Same(t, snapshot, bound.FieldTypes)
				}
				snapshot = bound.FieldTypes
			}
		})
	}
}

func TestPreparedFieldWindowDefaultParameters(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want types.T
	}{
		{"lag(?,0,?) over ()", types.T_text},
		{"lead(?,0,?) over ()", types.T_text},
		{"lag(?,1,?) over ()", types.T_text},
		{"lead(?,1,?) over ()", types.T_text},
		{"lag(coalesce(?,null),0,?) over ()", types.T_any},
		{"lag(?,0,coalesce(?,null)) over ()", types.T_any},
		{"lag(?,0,cast(? as binary)) over ()", types.T_blob},
	} {
		for _, textCondition := range []bool{false, true} {
			conditionName := "/numeric"
			if textCondition {
				conditionName = "/text"
			}
			t.Run(tc.expr+conditionName, func(t *testing.T) {
				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
					"select field(case when ? then "+tc.expr+" else null end,'A','a')", 1)
				require.NoError(t, err)
				defer stmt.Free()
				var snapshot *PreparedFieldTypes
				for _, oid := range []types.T{types.T_any, types.T_varchar, types.T_blob} {
					condition, value := types.T_int64.ToType(), any("a")
					var c any = int64(1)
					if textCondition {
						condition, c = types.T_varchar.ToType(), "1"
					}
					if oid == types.T_any {
						value = nil
					}
					bindings := []PreparedSourceBinding{{Position: 0, Type: condition}, {Position: 1, Type: oid.ToType()}, {Position: 2, Type: oid.ToType()}}
					values := []any{ParamValue{Value: c, SourceType: condition, HasSourceType: true}, ParamValue{Value: value, SourceType: oid.ToType(), HasSourceType: true}, ParamValue{Value: value, SourceType: oid.ToType(), HasSourceType: true}}
					ctx.SetContext(WithPreparedFieldTypes(context.Background(), snapshot))
					bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
					require.NoError(t, err)
					require.NotNil(t, bound.FieldTypes)
					require.Len(t, bound.FieldTypes.cases, 1)
					want := tc.want
					if textCondition && want == types.T_any {
						want = types.T_blob
					}
					require.Equal(t, want, bound.FieldTypes.cases[0].result.Oid)
					if snapshot != nil {
						require.Same(t, snapshot, bound.FieldTypes)
					}
					snapshot = bound.FieldTypes
				}
			})
		}
	}
}

func TestPreparedFieldRetainedProvenance(t *testing.T) {
	for _, tc := range []struct {
		expr          string
		peer          bool
		binary        bool
		inferredFact  bool
		allowNullFact bool
	}{
		{"case when ? then concat_ws('',nullif(?,lower(coalesce(?,null)))) else null end", true, false, false, false},
		{"case when ? then concat_ws('',nullif(?,cast(? as binary(1)))) else null end", true, true, false, false},
		{"case when ? then concat_ws('',nullif(coalesce(?,null),lower(coalesce(?,null)))) else null end", true, false, false, false},
		{"concat_ws('',case when ? then lower(coalesce(?,null)) else null end)", false, false, true, false},
		{"concat_ws('',case when ? then lower(coalesce(?,null)) else _binary '' end)", false, true, false, false},
		{"concat_ws('',case when ? then (case when true then lower(coalesce(?,null)) else null end) else null end)", false, false, true, false},
		{"concat_ws('',case when ? then elt(1,coalesce(?,null)) else null end)", false, false, true, false},
		{"concat_ws('',elt(1,case when ? then lower(coalesce(?,null)) else null end))", false, false, true, false},
		{"concat_ws('',elt(1,case when ? then ? else null end))", false, false, true, true},
		{"concat_ws('',ltrim(case when ? then ? else null end))", false, false, true, true},
		{"concat_ws('',ltrim(case when ? then lower(coalesce(?,null)) else null end))", false, false, true, false},
		{"concat_ws('',rtrim(case when ? then lower(coalesce(?,null)) else null end))", false, false, true, false},
		{"concat_ws('',elt(1,ltrim(case when ? then lower(coalesce(?,null)) else null end)))", false, false, true, false},
		{"concat_ws(elt(1,case when ? then lower(coalesce(?,null)) else null end),'a')", false, false, true, false},
		{"concat_ws('',elt(1,case when ? then lower(coalesce(?,null)) else null end,_binary ''))", false, true, true, false},
		{"concat_ws('',elt(1,case when ? then lower(coalesce(?,null)) else _binary '' end))", false, true, false, false},
		{"concat_ws('',unhex(case when ? then lower(coalesce(?,null)) else null end))", false, true, true, false},
		{"concat_ws('',(select elt(1,x) from (select case when ? then lower(coalesce(?,null)) else null end x limit 1) d))", false, false, true, false},
		{"concat_ws('',elt(1,(select x from (select case when ? then lower(coalesce(?,null)) else null end x union all select '') d where x<>'' limit 1)))", false, false, true, false},
		{"concat_ws('',elt(1,(select x from (select case when ? then lower(coalesce(?,null)) else null end x union all select null) d where x<>'' limit 1)))", false, false, true, false},
		{"greatest(case when ? then lower(coalesce(?,null)) else null end,'')", false, false, true, false},
		{"greatest('',case when ? then lower(coalesce(?,null)) else null end)", false, false, true, false},
		{"least(case when ? then lower(coalesce(?,null)) else null end,'z')", false, false, true, false},
		{"least('z',case when ? then lower(coalesce(?,null)) else null end)", false, false, true, false},
		{"greatest(case when ? then lower(coalesce(?,null)) else null end,_binary '')", false, true, true, false},
		{"least(case when ? then lower(coalesce(?,null)) else null end,_binary 'z')", false, true, true, false},
		{"greatest(case when ? then lower(coalesce(?,null)) else _binary '' end,'')", false, true, false, false},
		{"least(case when ? then lower(coalesce(?,null)) else _binary '' end,'z')", false, true, false, false},
		{"greatest(case when ? then ? else null end,'')", false, false, true, true},
		{"least(case when ? then ? else null end,'z')", false, false, true, true},
		{"greatest(case when ? then lower(coalesce(?,null)) else null end,1)", false, false, true, false},
		{"least(case when ? then lower(coalesce(?,null)) else null end,1)", false, false, true, false},
		{"greatest(elt(1,case when ? then lower(coalesce(?,null)) else null end),'')", false, false, true, false},
		{"least(elt(1,case when ? then lower(coalesce(?,null)) else null end),'z')", false, false, true, false},
		{"concat_ws('',greatest(case when ? then lower(coalesce(?,null)) else null end,''))", false, false, true, false},
		{"concat_ws('',least(case when ? then lower(coalesce(?,null)) else null end,'z'))", false, false, true, false},
		{"(select x from (select case when ? then lower(coalesce(?,null)) else null end x union all select '') d where x<>'' limit 1)", false, false, true, false},
		{"(select x from (select case when ? then lower(coalesce(?,null)) else null end x union select '') d where x<>'' limit 1)", false, false, true, false},
		{"(select x from (select case when ? then lower(coalesce(?,null)) else null end x union all select null) d where x<>'' limit 1)", false, true, true, false},
		{"(select x from (select case when ? then lower(coalesce(?,null)) else null end x union all select _binary '') d where x<>'' limit 1)", false, true, true, false},
		{"(select x from (select case when ? then lower(coalesce(?,null)) else _binary '' end x union all select '') d where x<>'' limit 1)", false, true, false, false},
		{"ifnull(case when ? then lower(coalesce(?,null)) else null end,'')", false, false, true, false},
		{"ifnull(null,case when ? then lower(coalesce(?,null)) else null end)", false, true, true, false},
		{"ifnull(case when ? then lower(coalesce(?,null)) else null end,null)", false, true, true, false},
		{"ifnull(case when ? then lower(coalesce(?,null)) else null end,_binary '')", false, true, true, false},
		{"concat_ws('',ifnull(case when ? then lower(coalesce(?,null)) else null end,''))", false, false, true, false},
		{"ifnull(case when ? then lower(coalesce(?,null)) else _binary '' end,'')", false, true, false, false},
		{"coalesce(case when ? then lower(coalesce(?,null)) else null end,'')", false, false, true, false},
		{"coalesce(null,case when ? then lower(coalesce(?,null)) else null end,'')", false, false, true, false},
		{"concat(case when ? then lower(coalesce(?,null)) else null end,'')", false, false, true, false},
		{"concat('',case when ? then lower(coalesce(?,null)) else null end)", false, false, true, false},
		{"if(1,case when ? then lower(coalesce(?,null)) else null end,'')", false, false, true, false},
		{"coalesce(case when ? then lower(coalesce(?,null)) else null end,null)", false, true, true, false},
		{"concat(case when ? then lower(coalesce(?,null)) else null end)", false, true, true, false},
		{"coalesce(case when ? then lower(coalesce(?,null)) else null end,_binary '')", false, true, true, false},
		{"concat(case when ? then lower(coalesce(?,null)) else null end,_binary '')", false, true, true, false},
	} {
		for _, textCondition := range []bool{false, true} {
			name := "/numeric"
			if textCondition {
				name = "/text"
			}
			t.Run(tc.expr+name, func(t *testing.T) {
				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select field("+tc.expr+",'A','a')", 1)
				require.NoError(t, err)
				defer stmt.Free()
				var snapshot *PreparedFieldTypes
				for _, oid := range []types.T{types.T_any, types.T_varchar, types.T_blob} {
					condition, value, peer := types.T_int64.ToType(), any("a"), any("x")
					var c any = int64(1)
					if textCondition {
						condition, c = types.T_varchar.ToType(), "1"
					}
					if oid == types.T_any {
						value, peer = nil, nil
					}
					bindings := []PreparedSourceBinding{{Position: 0, Type: condition}, {Position: 1, Type: oid.ToType()}}
					values := []any{ParamValue{Value: c, SourceType: condition, HasSourceType: true}, ParamValue{Value: value, SourceType: oid.ToType(), HasSourceType: true}}
					if tc.peer {
						bindings = append(bindings, PreparedSourceBinding{Position: 2, Type: oid.ToType()})
						values = append(values, ParamValue{Value: peer, SourceType: oid.ToType(), HasSourceType: true})
					}
					prior := snapshot
					var facts map[int]preparedFieldCaseType
					if prior != nil {
						facts = maps.Clone(prior.cases)
					}
					ctx.SetContext(WithPreparedFieldTypes(context.Background(), snapshot))
					bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
					require.NoError(t, err)
					require.NotNil(t, bound.FieldTypes)
					if prior != nil {
						require.Same(t, prior, bound.FieldTypes)
						require.Equal(t, facts, prior.cases, "binding must not mutate published facts")
					}
					snapshot = bound.FieldTypes
					for _, fact := range snapshot.cases {
						// A neutral NULL CASE fact acquires its BLOB declaration at
						// the argument boundary, without mutating the stored fact.
						if tc.allowNullFact && fact.result.Oid == types.T_any {
							require.False(t, fact.inferredBinary)
						} else {
							require.Equal(t, tc.inferredFact, fact.inferredBinary)
						}
					}
					if oid != types.T_any {
						field := findPlanFunctionExpr(bound.Plan, "field")
						require.NotNil(t, field)
						require.Equal(t, tc.binary, types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])) == types.StringDomainBinary)
					}
				}
			})
		}
	}
}

func TestPreparedFieldStringConsumerWithoutHistory(t *testing.T) {
	for _, expr := range []string{
		"lower(case when ? then lower(coalesce(?,null)) else null end)",
		"lower(ifnull(case when ? then lower(coalesce(?,null)) else null end,''))",
		"lower(case when ? then case when true then lower(coalesce(?,null)) else null end else null end)",
	} {
		for _, oid := range []types.T{types.T_any, types.T_varchar, types.T_blob} {
			t.Run(expr+"/"+oid.String(), func(t *testing.T) {
				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				ctx.SetContext(WithPreparedFieldTypes(context.Background(), nil))
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select "+expr, 1)
				require.NoError(t, err)
				defer stmt.Free()
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
				var value any = "a"
				if oid == types.T_any {
					value = nil
				}
				values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}}
				bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
				require.NoError(t, err)
				require.Nil(t, bound.FieldTypes, "reading a string declaration must not acquire FIELD history")
			})
		}
	}
}

func TestPreparedConditionalDeclarationWithoutHistory(t *testing.T) {
	for _, value := range []string{"n_name", "lpad(n_name,3,'z')"} {
		for _, boundary := range []string{"limit 1", "having x is not null"} {
			t.Run(value+"/"+boundary, func(t *testing.T) {
				proc := newPlanTestProcess(t)
				params := vector.NewVec(types.T_text.ToType())
				t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
				require.NoError(t, vector.AppendBytes(params, []byte("1"), false, proc.Mp()))
				proc.SetPrepareParams(params)
				ctx := NewMockCompilerContext(false, proc)
				ctx.SetContext(WithPreparedFieldTypes(context.Background(), nil))
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
					"select reverse(x) from (select case when ? then "+value+" else _binary '' end x from nation "+boundary+") d", 1)
				require.NoError(t, err)
				defer stmt.Free()
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}}
				values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}}
				for range 2 {
					bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
					require.NoError(t, err)
					require.Nil(t, bound.FieldTypes, "the condition alone does not require result-type history")
					reverse := findPlanFunctionExpr(bound.Plan, "reverse")
					require.NotNil(t, reverse)
					arg := reverse.GetF().Args[0]
					for source := preparedFieldPrivateCastSource(arg, 0); source != nil; source = preparedFieldPrivateCastSource(arg, 0) {
						arg = source
					}
					require.False(t, isCastOverload(arg, 1), "a text declaration must not consume a binary transport")
				}
			})
		}
	}
}

func TestPreparedFieldSetResolvedDeclaration(t *testing.T) {
	// A concrete first binding can need no retained CASE facts. Identity
	// relational nodes must still expose its real branch declaration.
	for _, peer := range []string{"''", "null"} {
		for _, order := range []string{"", " order by x"} {
			for _, oid := range []types.T{types.T_varchar, types.T_blob} {
				t.Run(peer+order+"/"+oid.String(), func(t *testing.T) {
					sql := "select field(concat_ws('',elt(1,(select x from (select case when ? then lower(coalesce(?,null)) else null end x union all select " + peer + ") d where x<>''" + order + " limit 1))),'A','a')"
					ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
					stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
					require.NoError(t, err)
					defer stmt.Free()
					bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
					values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: "a", SourceType: bindings[1].Type, HasSourceType: true}}
					bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
					require.NoError(t, err)
					require.Nil(t, bound.FieldTypes)
					field := findPlanFunctionExpr(bound.Plan, "field")
					require.NotNil(t, field)
					got := types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0]))
					require.Equal(t, oid == types.T_blob, got == types.StringDomainBinary)
				})
			}
		}
	}
}

func TestPreparedFieldProvenanceReprepare(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select field(concat_ws('',case when ? then lower(coalesce(?,null)) else null end),'A','a')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	var snapshot *PreparedFieldTypes
	for i := range 2 {
		condition, source := types.T_varchar, types.T_any
		var c, value any = "1", nil
		if i == 1 {
			condition, source, c, value = types.T_int64, types.T_blob, int64(1), "a"
		}
		bindings := []PreparedSourceBinding{{Position: 0, Type: condition.ToType()}, {Position: 1, Type: source.ToType()}}
		values := []any{ParamValue{Value: c, SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}}
		ctx.SetContext(WithPreparedFieldTypes(context.Background(), snapshot))
		bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
		require.NoError(t, err)
		require.NotNil(t, bound.FieldTypes)
		require.Len(t, bound.FieldTypes.cases, 1)
		require.Equal(t, i == 0, bound.FieldTypes.cases[0].inferredBinary)
		if snapshot != nil {
			require.NotSame(t, snapshot, bound.FieldTypes)
			require.Equal(t, snapshot.cases[0].result, bound.FieldTypes.cases[0].result, "provenance can change without changing the declaration type")
			require.Greater(t, bound.FieldTypes.Revision(), snapshot.Revision())
			require.True(t, snapshot.cases[0].inferredBinary, "reprepare must not mutate the old declaration provenance")
		}
		snapshot = bound.FieldTypes
	}
}

func TestPreparedFieldNullIfReprepare(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select field(case when ? then nullif(?,'x') else null end,'A','a')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	var snapshot *PreparedFieldTypes
	for i, condition := range []types.T{types.T_varchar, types.T_int64} {
		source := types.T_varchar
		var c any = "1"
		if i == 1 {
			source = types.T_blob
			c = int64(1)
		}
		bindings := []PreparedSourceBinding{{Position: 0, Type: condition.ToType()}, {Position: 1, Type: source.ToType()}}
		values := []any{ParamValue{Value: c, SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: "a", SourceType: bindings[1].Type, HasSourceType: true}}
		ctx.SetContext(WithPreparedFieldTypes(context.Background(), snapshot))
		bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
		require.NoError(t, err)
		require.NotNil(t, bound.FieldTypes)
		want := types.T_text
		if i == 1 {
			want = types.T_blob
			require.Greater(t, bound.FieldTypes.Revision(), snapshot.Revision())
		}
		require.Equal(t, want, bound.FieldTypes.cases[0].result.Oid)
		snapshot = bound.FieldTypes
	}
}
