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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestEnumSetResultMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want []types.T
	}{
		{"direct", "select e, s, v from enum_order_t", []types.T{types.T_enum, types.T_uint64, types.T_any}},
		{"output alignment", "select v, s as x, e as y, s from enum_order_t", []types.T{types.T_any, types.T_uint64, types.T_enum, types.T_uint64}},
		{"empty", "select e, s from enum_order_t where false", []types.T{types.T_enum, types.T_uint64}},
		{"distinct", "select distinct e, s from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
		{"group keys", "select e, s from enum_order_t group by e, s", []types.T{types.T_enum, types.T_uint64}},
		{"primary key group", "select e, s from enum_order_t group by id", []types.T{types.T_enum, types.T_uint64}},
		{"derived primary key group", "select e, s from (select e, s from enum_order_t group by id) d", []types.T{types.T_enum, types.T_uint64}},
		{"grouped field union", "select e, s from (select e, s from enum_order_t group by id) d union all select e,s from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"ordered limit", "select e, s from enum_order_t order by id limit 2", []types.T{types.T_enum, types.T_uint64}},
		{"derived limit", "select s, e from (select e, s from enum_order_t limit 2) d", []types.T{types.T_uint64, types.T_enum}},
		{"cte", "with c as (select e, s from enum_order_t) select s, e from c", []types.T{types.T_uint64, types.T_enum}},
		{"outer join", "select b.s, a.e from enum_order_t a left join enum_order_t b on a.id=b.id", []types.T{types.T_uint64, types.T_enum}},
		{"string expressions", "select concat(e), cast(s as char), coalesce(e, v) from enum_order_t", []types.T{types.T_any, types.T_any, types.T_any}},
		{"derived expression", "select e, s from (select concat(e) as e, cast(s as char) as s from enum_order_t limit 2) d", []types.T{types.T_any, types.T_any}},
		{"union", "select e, s from enum_order_t union all select e, s from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"derived union", "select e, s from (select e, s from enum_order_t union select e, s from enum_order_t) d", []types.T{types.T_any, types.T_any}},
		// These expressions do not retain a declared field identity. Their
		// MySQL expression/phase-specific presentation is a separate contract.
		{"aggregates", "select min(e), max(s) from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"windows", "select min(e) over (), max(s) over () from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"scalar", "select (select e from enum_order_t limit 1)", []types.T{types.T_any}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newEnumSetResultMock(t)
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			snapshot := proto.Clone(p)
			cols := GetResultColumnsFromPlan(p)
			require.Len(t, cols, len(tc.want))
			hasDeclared := false
			for i, want := range tc.want {
				if want == types.T_any {
					require.Nil(t, cols[i].EnumSetResultType)
					continue
				}
				hasDeclared = true
				source := cols[i].EnumSetResultType
				require.NotNil(t, source)
				require.Equal(t, int32(want), source.Id)
				require.NotEmpty(t, source.Enumvalues)
				require.Equal(t, int32(types.T_varchar), cols[i].Typ.Id, "execution still produces labels")
				copy := DeepCopyColDef(cols[i])
				require.Equal(t, source, copy.EnumSetResultType)
				require.NotSame(t, source, copy.EnumSetResultType)
				copy.EnumSetResultType.Enumvalues = "changed"
				require.NotEqual(t, source.Enumvalues, copy.EnumSetResultType.Enumvalues)
			}
			require.Equal(t, hasDeclared, len(p.GetQuery().EnumSetResultTypes) > 0, "no sidecar for ordinary results")
			cloned := GetResultColumnsFromPlan(DeepCopyPlan(p))
			require.Len(t, cloned, len(cols))
			for i := range cols {
				require.Equal(t, cols[i].Typ, cloned[i].Typ)
				require.Equal(t, cols[i].EnumSetResultType, cloned[i].EnumSetResultType)
			}
			data, err := p.Marshal()
			require.NoError(t, err)
			var decoded pb.Plan
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, cols, GetResultColumnsFromPlan(&decoded))
			require.True(t, proto.Equal(snapshot, p), "metadata extraction must not mutate the cached plan")
		})
	}
}

func newEnumSetResultMock(t *testing.T) *MockOptimizer {
	mock := newMySQLSpecialOrderMock(t)
	mock.ctxt.tables["enum_order_t"].Cols[0].Typ.NotNullable = true
	mock.ctxt.tables["enum_order_t"].Pkey = &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	return mock
}

func TestEnumSetResultMetadataDoesNotRestoreSchemaSource(t *testing.T) {
	mock := newEnumSetResultMock(t)
	stmts, err := mysql.Parse(t.Context(), "select e,s from enum_order_t group by id", 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	builder := NewQueryBuilder(pb.Query_SELECT, &mock.ctxt, false, true)
	ctx := NewBindContext(builder, nil)
	_, err = builder.bindSelect(stmts[0].(*tree.Select), ctx, true)
	require.NoError(t, err)
	for i := range 2 {
		provenance := ctx.outputColumnProvenanceForProject(int32(i))
		require.Equal(t, ProvenanceNone, provenance.State)
		require.Nil(t, provenance.Source)
		require.NotNil(t, provenance.EnumSetResultType)
		require.Nil(t, mysqlSpecialTypeFromProvenance(provenance), "schema must not consume result-only identity")
	}
	ctx.clearOutputColumnProvenance()
	require.Nil(t, enumSetResultTypeFromProvenance(ctx.outputColumnProvenanceForProject(0)), "UNION terminates field identity")
}
