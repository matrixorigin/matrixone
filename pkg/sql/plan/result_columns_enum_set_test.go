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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEnumSetResultPresentation(t *testing.T) {
	expressionKinds := map[string]pb.MySQLSpecialResultType_Kind{
		"min max":                  pb.MySQLSpecialResultType_AGGREGATE,
		"empty aggregate":          pb.MySQLSpecialResultType_AGGREGATE,
		"aggregate subquery input": pb.MySQLSpecialResultType_AGGREGATE,
		"scalar subquery":          pb.MySQLSpecialResultType_SCALAR_SUBQUERY,
		"empty subquery":           pb.MySQLSpecialResultType_SCALAR_SUBQUERY,
		"correlated subquery":      pb.MySQLSpecialResultType_SCALAR_SUBQUERY,
	}
	for _, tc := range []struct {
		name string
		sql  string
		want []types.T
	}{
		{"direct", "select e, s, v from enum_order_t", []types.T{types.T_enum, types.T_uint64, types.T_any}},
		{"ordinary first", "select v, e, s from enum_order_t", []types.T{types.T_any, types.T_enum, types.T_uint64}},
		{"reordered aliases", "select s as x, v, e as y, s from enum_order_t", []types.T{types.T_uint64, types.T_any, types.T_enum, types.T_uint64}},
		{"empty", "select e, s from enum_order_t where false", []types.T{types.T_enum, types.T_uint64}},
		{"distinct", "select distinct e, s from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
		{"group key", "select e, s from enum_order_t group by e, s", []types.T{types.T_enum, types.T_uint64}},
		{"group by primary key", "select e, s from enum_order_t group by id", []types.T{types.T_enum, types.T_uint64}},
		{"min max", "select min(e), max(s) from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
		{"scalar subquery", "select (select e from enum_order_t limit 1), (select s from enum_order_t limit 1)", []types.T{types.T_enum, types.T_uint64}},
		{"empty aggregate", "select min(e), max(s) from enum_order_t where false", []types.T{types.T_enum, types.T_uint64}},
		{"empty subquery", "select (select e from enum_order_t where false), (select s from enum_order_t where false)", []types.T{types.T_enum, types.T_uint64}},
		{"correlated subquery", "select (select b.e from enum_order_t b where b.id=a.id), (select b.s from enum_order_t b where b.id=a.id) from enum_order_t a", []types.T{types.T_enum, types.T_uint64}},
		{"no from", "select (select e), (select s) from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
		{"aggregate subquery input", "select min((select b.e from enum_order_t b where b.id=a.id)), max(s) from enum_order_t a", []types.T{types.T_enum, types.T_uint64}},
		{"derived aggregate", "select e, s from (select min(e) e, max(s) s from enum_order_t) d", []types.T{types.T_enum, types.T_uint64}},
		{"derived grouped column", "select e, s from (select e, s from enum_order_t group by id) d", []types.T{types.T_enum, types.T_uint64}},
		{"derived scalar", "select e, s from (select (select e from enum_order_t limit 1) e, (select s from enum_order_t limit 1) s) d", []types.T{types.T_any, types.T_any}},
		{"aggregate expressions", "select min(concat(e)), concat(max(s)) from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"window min max", "select min(e) over (), max(s) over () from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"aggregate union", "select min(e), max(s) from enum_order_t union all select min(e), max(s) from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"derived limit", "select s, e from (select e, s from enum_order_t limit 2) d", []types.T{types.T_uint64, types.T_enum}},
		{"cte", "with c as (select e, s from enum_order_t) select s, e from c", []types.T{types.T_uint64, types.T_enum}},
		{"outer join", "select b.s, a.e from enum_order_t a left join enum_order_t b on a.id=b.id", []types.T{types.T_uint64, types.T_enum}},
		{"string expressions", "select concat(e), cast(s as char), coalesce(e, v) from enum_order_t", []types.T{types.T_any, types.T_any, types.T_any}},
		{"union", "select e, s from enum_order_t union all select e, s from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"derived union", "select e, s from (select e, s from enum_order_t union all select e, s from enum_order_t) d", []types.T{types.T_any, types.T_any}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMySQLSpecialOrderMock(t)
			mock.ctxt.tables["enum_order_t"].Cols[0].Typ.NotNullable = true
			mock.ctxt.tables["enum_order_t"].Pkey = &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
			mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			snapshot := proto.Clone(p)
			before, err := p.Marshal()
			require.NoError(t, err)
			cols := GetResultColumnsFromPlan(p)
			require.Len(t, cols, len(tc.want))
			hasSpecial := false
			for i, want := range tc.want {
				if want == types.T_any {
					require.Nil(t, cols[i].MysqlSpecialResultType)
					continue
				}
				hasSpecial = true
				require.NotNil(t, cols[i].MysqlSpecialResultType)
				require.Equal(t, int32(want), cols[i].MysqlSpecialResultType.Typ.Id)
				require.NotEmpty(t, cols[i].MysqlSpecialResultType.Typ.Enumvalues)
				require.Equal(t, expressionKinds[tc.name], cols[i].MysqlSpecialResultType.Kind)
				require.Equal(t, int32(types.T_varchar), cols[i].Typ.Id, "execution and result storage still hold labels")
				require.Equal(t, cols[i].MysqlSpecialResultType, DeepCopyColDef(cols[i]).MysqlSpecialResultType)
				require.NotSame(t, cols[i].MysqlSpecialResultType, DeepCopyColDef(cols[i]).MysqlSpecialResultType)
			}
			if hasSpecial {
				require.Len(t, p.GetQuery().MysqlSpecialResultTypes, len(tc.want))
			} else {
				require.Empty(t, p.GetQuery().MysqlSpecialResultTypes, "ordinary results do not allocate presentation metadata")
			}
			cloned := GetResultColumnsFromPlan(DeepCopyPlan(p))
			require.Len(t, cloned, len(cols))
			for i := range cols {
				require.Equal(t, cols[i].Typ, cloned[i].Typ)
				require.Equal(t, cols[i].MysqlSpecialResultType, cloned[i].MysqlSpecialResultType)
			}
			var decoded pb.Plan
			require.NoError(t, decoded.Unmarshal(before))
			require.Equal(t, cols, GetResultColumnsFromPlan(&decoded))
			require.True(t, proto.Equal(snapshot, p), "extracting metadata must not mutate a cached plan")
		})
	}
}

func TestEnumSetPresentationDoesNotRestoreSchemaProvenance(t *testing.T) {
	domain := pb.Type{Id: int32(types.T_enum), Enumvalues: "a,bb"}
	display, err := makeEnumOrSetDisplayValue(t.Context(), GetColExpr(domain, 10, 0))
	require.NoError(t, err)
	minimum, err := BindFuncExprImplByPlanExpr(t.Context(), "min", []*pb.Expr{DeepCopyExpr(display)})
	require.NoError(t, err)
	ctx := NewBindContext(nil, nil)
	ctx.projectTag, ctx.aggregateTag = 20, 30
	ctx.headings = []string{"e", "minimum"}
	ctx.projects = []*pb.Expr{display, GetColExpr(minimum.Typ, ctx.aggregateTag, 0)}
	ctx.aggregates = []*pb.Expr{minimum}
	ctx.bindingByTag[10] = &Binding{outputColumnProvenance: []OutputColumnProvenance{{
		State:  ProvenanceSingleSource,
		Source: &SourceColumn{Metadata: SourceColumnMetadata{Typ: domain}},
	}}}
	require.Equal(t, ProvenanceSingleSource, ctx.outputColumnProvenanceForProject(0).State)
	mock := newMySQLSpecialOrderMock(t)
	builder := &QueryBuilder{compCtx: &mock.ctxt}
	builder.captureMySQLSpecialBlockResultTypes(ctx)
	ctx.projects[0] = builder.wrapBareColRefsInAnyValue(ctx.projects[0], ctx)
	for i, kind := range []pb.MySQLSpecialResultType_Kind{pb.MySQLSpecialResultType_COLUMN, pb.MySQLSpecialResultType_AGGREGATE} {
		provenance := ctx.outputColumnProvenanceForProject(int32(i))
		require.Equal(t, ProvenanceNone, provenance.State)
		require.Nil(t, provenance.Source, "result presentation must not fabricate a schema source")
		require.NotNil(t, provenance.MySQLSpecialResultType)
		require.Equal(t, kind, provenance.MySQLSpecialResultType.Kind)
	}
	ctx.clearOutputColumnProvenance()
	require.Nil(t, ctx.outputColumnProvenanceForProject(0).MySQLSpecialResultType)
}
