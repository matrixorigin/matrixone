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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		{"LAG binary default", "select field(case when ? then lag(?,0,_binary 'x') over () else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"LAG compound text default", "select field(case when ? then lag(coalesce(?,null),0,'x') over () else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"LAG compound", "select field(case when ? then lag(coalesce(?,null),0) over () else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"LEAD compound", "select field(case when ? then lead(coalesce(?,null),0) over () else null end,'A','a')", false, types.T_any, types.T_any, false},
		{"CONCAT marker", "select field(case when ? then concat(?) else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT compound", "select field(case when ? then concat(coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"CONCAT literal peer", "select field(case when ? then concat(?,'') else null end,'A','a')", false, types.T_any, types.T_text, false},
		{"CONCAT compound literal peer", "select field(case when ? then concat(coalesce(?,null),'') else null end,'A','a')", false, types.T_any, types.T_blob, false},
		{"CONCAT_WS skips NULL compound", "select field(case when ? then concat_ws('',coalesce(?,null)) else null end,'A','a')", false, types.T_any, types.T_text, false},
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
