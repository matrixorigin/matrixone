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
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestEnumSetResultPresentation(t *testing.T) {
	expressionKinds := map[string]pb.MySQLSpecialResultType_Kind{
		"min max":                   pb.MySQLSpecialResultType_AGGREGATE,
		"empty aggregate":           pb.MySQLSpecialResultType_AGGREGATE,
		"aggregate subquery input":  pb.MySQLSpecialResultType_AGGREGATE,
		"scalar subquery":           pb.MySQLSpecialResultType_SCALAR_SUBQUERY,
		"empty subquery":            pb.MySQLSpecialResultType_SCALAR_SUBQUERY,
		"correlated subquery":       pb.MySQLSpecialResultType_SCALAR_SUBQUERY,
		"rollup columns":            pb.MySQLSpecialResultType_ROLLUP,
		"rollup aggregate":          pb.MySQLSpecialResultType_ROLLUP,
		"window min max":            pb.MySQLSpecialResultType_PREPARE_ONLY,
		"distinct rollup":           pb.MySQLSpecialResultType_PREPARE_ONLY,
		"derived rollup":            pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"rollup user union":         pb.MySQLSpecialResultType_ROLLUP,
		"derived window":            pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"widthonly aggregate":       pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"widthonly scalar":          pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"widthonly window":          pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"widthonly collapsed union": pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"widthonly union":           pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"widthonly rollup":          pb.MySQLSpecialResultType_VARCHAR_LABEL,
		"collapsed rollup union":    pb.MySQLSpecialResultType_ROLLUP,
		"noncollapsed rollup union": pb.MySQLSpecialResultType_ROLLUP,
	}
	for _, tc := range []struct {
		name string
		sql  string
		want []types.T
	}{
		{"widthonly aggregate", "select min(e),max(s) from (select e,s from enum_order_t group by e,s with rollup) d", []types.T{types.T_enum, types.T_uint64}},
		{"widthonly scalar", "select (select e from (select e,s from enum_order_t group by e,s with rollup) d limit 1), (select s from (select e,s from enum_order_t group by e,s with rollup) d limit 1)", []types.T{types.T_enum, types.T_uint64}},
		{"widthonly window", "select min(e) over(),max(s) over() from (select e,s from enum_order_t group by e,s with rollup) d", []types.T{types.T_enum, types.T_uint64}},
		{"widthonly collapsed union", "select e,s from (select e,s from enum_order_t group by e,s with rollup) d union select e,s from (select e,s from enum_order_t group by e,s with rollup) d", []types.T{types.T_enum, types.T_uint64}},
		{"widthonly union", "select e,s from (select e,s from enum_order_t group by e,s with rollup) d union all select e,s from enum_order_t group by e,s with rollup", []types.T{types.T_enum, types.T_uint64}},
		{"widthonly rollup", "select e,s from (select e,s from enum_order_t group by e,s with rollup) d group by e,s with rollup", []types.T{types.T_enum, types.T_uint64}},
		{"collapsed rollup union", "select e,s from enum_order_t group by e,s with rollup union select e,s from enum_order_t group by e,s with rollup", []types.T{types.T_enum, types.T_uint64}},
		{"noncollapsed rollup union", "select e,s from enum_order_t group by e,s with rollup union select e,s from enum_order_t where true group by e,s with rollup", []types.T{types.T_enum, types.T_uint64}},
		{"direct", "select e, s, v from enum_order_t", []types.T{types.T_enum, types.T_uint64, types.T_any}},
		{"ordinary first", "select v, e, s from enum_order_t", []types.T{types.T_any, types.T_enum, types.T_uint64}},
		{"reordered aliases", "select s as x, v, e as y, s from enum_order_t", []types.T{types.T_uint64, types.T_any, types.T_enum, types.T_uint64}},
		{"empty", "select e, s from enum_order_t where false", []types.T{types.T_enum, types.T_uint64}},
		{"distinct", "select distinct e, s from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
		{"group key", "select e, s from enum_order_t group by e, s", []types.T{types.T_enum, types.T_uint64}},
		{"rollup columns", "select e, s from enum_order_t group by e, s with rollup", []types.T{types.T_enum, types.T_uint64}},
		{"distinct rollup", "select distinct e, s from enum_order_t group by e, s with rollup", []types.T{types.T_enum, types.T_uint64}},
		{"derived rollup", "select e, s from (select e, s from enum_order_t group by e, s with rollup) d", []types.T{types.T_enum, types.T_uint64}},
		{"derived window", "select e, s from (select min(e) over () e, max(s) over () s from enum_order_t) d", []types.T{types.T_enum, types.T_uint64}},
		{"rollup user union", "select e, s from enum_order_t group by e, s with rollup union all select e, s from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
		{"rollup aggregate", "select min(e), max(s) from enum_order_t group by id with rollup", []types.T{types.T_enum, types.T_uint64}},
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
		{"window min max", "select min(e) over (), max(s) over () from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
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

func TestEnumSetWindowMaterializationPlan(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		elided bool
	}{
		{"select min(e) over(),max(s) over() from enum_order_t limit 0", true},
		{"select min(e) over(),max(s) over() from enum_order_t where false", true},
		{"select distinct e,s from enum_order_t where false group by e,s with rollup", true},
		{"select distinct e,s from enum_order_t group by e,s with rollup limit 0", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id>100", false},
		{"select min(min(e)) over(),max(max(s)) over() from enum_order_t where false", true},
		{"select min(min(e)) over(),max(max(s)) over() from enum_order_t", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=1)b on a.id=b.id where a.id=coalesce(b.id,2)", false},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a join (select id from enum_order_t where id=1)b on a.id=b.id where a.id=coalesce(b.id,2)", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=1)b on a.id=b.id where a.id=1", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 and exists(select 1 from enum_order_t b where b.id=enum_order_t.id)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where exists(select 1 from enum_order_t b where b.id=enum_order_t.id)", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 and exists(select 1 from enum_order_t b where b.id>=enum_order_t.id)", false},
		{"select min(e)over(),max(s)over() from enum_order_t where not(not(id=1))", true},
		{"select min(e)over(),max(s)over() from enum_order_t where not(id<>1)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where not(id=1 or null)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where not(id=1 or false)", false},
		{"select min(e)over(),max(s)over() from enum_order_t where not(id<>1 and null)", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=1)b on a.id=b.id where a.id=b.id", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=1)b on true where a.id=coalesce(b.id,2)", false},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=100)b on true where a.id=coalesce(b.id,2)", false},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=1)b on a.id=b.id where b.id is not null and a.id=coalesce(b.id,2)", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=1)b on a.id=b.id where b.id is not null or a.id=2", false},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t where id=1)b on a.id=b.id where not(b.id is null) and a.id=coalesce(b.id,2)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where not(not(id between 1.2 and 1.1))", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id>=1.2 and id<=1.1", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id between 1.2 and 1.1", true},
		{"select min(e)over(),max(s)over() from enum_order_t where 1.2<=id and 1.1>=id", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id>1.2 and id<1.1", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id>=2.0 and id<=1.0", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id>2.0 and id<1.0", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id>=cast(2.0 as decimal(10,1)) and id<=1", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id>=2 and id<=1.1", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id>=2.2 and id<=1.2", true},
		{"select min(e)over(),max(s)over() from enum_order_t where 2<=id and 1>=id", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id>2 and id<1", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id>1 and id<2", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id>2.5 and id<2.5", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id>=1 and id<1", true},
		{"select min(e)over(),max(s)over() from enum_order_t where not(id<2 or id>1)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where not(id<1.2 or id>1.1)", false},
		{"select min(e)over(),max(s)over() from enum_order_t where (id between 1.2 and 1.1) or id=1", false},
		{"select min(e)over(),max(s)over() from enum_order_t where not(id between 1.2 and 1.1)", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id between 2.5 and 1.5", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id between 1.5 and 2.5", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id between -1.5 and -2.5", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id between 2.50 and 1.500", true},
		{"select min(e)over(),max(s)over() from (select id,e,s from enum_order_t limit 2)d where id=(select min(id) from enum_order_t)", false},
		{"select min(e)over(),max(s)over() from (select id,e,s from enum_order_t limit 2)d where id=1", false},
		{"select min(e)over(),max(s)over() from (select id,e,s from enum_order_t)d where id=(select min(id) from enum_order_t)", true},
		{"select min(e)over(),max(s)over() from (select id,e,s from enum_order_t limit 1)d where id=(select min(id) from enum_order_t)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 and exists(select count(*) from enum_order_t b where b.id=enum_order_t.id group by b.e)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 and exists(select count(*) from enum_order_t b where b.id>=enum_order_t.id group by b.e)", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 and exists(select 1 from enum_order_t b where b.id=(select min(id) from enum_order_t))", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 and exists(select count(*) from enum_order_t b where b.id=(select min(id) from enum_order_t) group by b.e)", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a join (select id from enum_order_t limit 1)b on a.id=b.id", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a join (select id from enum_order_t limit 1)b on true where a.id=b.id", true},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a join (select id from enum_order_t limit 2)b on a.id=b.id", false},
		{"select min(a.e)over(),max(a.s)over() from enum_order_t a left join (select id from enum_order_t limit 1)b on a.id=b.id", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 or false", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 or id=1", true},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 or id=2", false},
		{"select min(e)over(),max(s)over() from enum_order_t where id=1 or null", true},
		{"select min(e)over(),max(s)over() from enum_order_t where (id=1 or false) and exists(select 1 from enum_order_t b where b.id=(select min(id) from enum_order_t))", true},
		{"select min(e) over(),max(s) over() from enum_order_t where cast(id as signed)=(select count(*) from enum_order_t where id<=1)", false},
		{"select min(e) over(),max(s) over() from enum_order_t where cast(id as signed)=1", false},
		{"select min(e) over(),max(s) over() from enum_order_t where id=(select count(*) from enum_order_t where id<=1)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and not exists(select 1 from (select id from enum_order_t where id=100 limit 1)d)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and not exists(select 1 from (select id from enum_order_t where id=1 limit 1)d)", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and exists(select count(*) from enum_order_t r where r.id>=l.id)", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and not exists(select count(*) from enum_order_t r where r.id>=l.id)", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and exists(select count(*) from enum_order_t r where r.id>=l.id having count(*)>0)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=(select min(id) from enum_order_t) and id=(select min(id) from enum_order_t)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=coalesce((select min(id) from enum_order_t),1)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=(select min(id) from enum_order_t where id=(select min(id) from enum_order_t))", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=(select min(id) from enum_order_t) and id=coalesce((select min(id) from enum_order_t),1)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id between 2 and 1", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id between null and 1", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id between 1 and null", true},
		{"select min(e) over(),max(s) over() from enum_composite_t where id=(select min(id) from enum_order_t) and u=(select min(id) from enum_order_t)", true},
		{"select min(e) over(),max(s) over() from enum_composite_t where id=(select min(id) from enum_order_t)", false},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and exists(select 1 from (select id from enum_order_t where id=1 limit 1)d)", false},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and exists(select count(*) from enum_order_t)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and exists(select count(*) from enum_order_t where false)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and not exists(select count(*) from enum_order_t)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and exists(select count(*) from enum_order_t group by id)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=1 and exists(select 1 from (select count(*) from enum_order_t)d)", false},
		{"select min(e) over(),max(s) over() from enum_order_t where id=(select min(id) from enum_order_t)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=(select count(*) from enum_order_t)", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id=(select id from enum_order_t where id=1)", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=(select min(r.id) from enum_order_t r where r.id>=l.id)", false},
		{"select min(e) over(partition by id),max(s) over(partition by id) from enum_order_t where id=1", true},
		{"select min(e) over(partition by id),max(s) over(partition by id) from enum_order_t where false", true},
		{"select min(e) over(partition by id),max(s) over(partition by id) from enum_order_t", false},
		{"select min(min(e)) over(),max(max(s)) over() from enum_order_t where id=1 group by id", true},
		{"select min(min(e)) over(),max(max(s)) over() from enum_order_t where false group by id", true},
		{"select min(min(e)) over(),max(max(s)) over() from enum_order_t group by id", false},
		{"select min(e) over(),max(s) over(),(select count(*) from enum_order_t) from enum_order_t where id=1", true},
		{"select min(e) over(),max(s) over(),(select count(*) from enum_order_t) from enum_order_t limit 1", false},
		{"select min(e) over(),max(s) over() from (select e,s,row_number() over(order by id) n from enum_order_t order by id limit 1)d where n>0", true},
		{"select min(e) over(),max(s) over() from (select e,s,row_number() over(order by id) n from enum_order_t order by id limit 2)d where n>0", false},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and exists(select 1 from enum_order_t r where r.id=1 and r.id=l.id)", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and not exists(select 1 from enum_order_t r where r.id=100)", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and not exists(select 1 from enum_order_t r where r.id=1)", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and not exists(select 1 from enum_order_t r where r.id=1 and r.id=l.id)", false},
		{"select min(e) over(),max(s) over() from enum_order_t where id between 1 and 1", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id between 1 and 2", false},
		{"select min(e) over(),max(s) over() from enum_order_t where id<=>1", true},
		{"select min(e) over(),max(s) over() from enum_order_t where u<=>1", true},
		{"select min(e) over(),max(s) over() from enum_order_t where u<=>null", false},
		{"select min(e) over(),max(s) over() from enum_order_t where id=100", true},
		{"select min(e) over(),max(s) over() from enum_order_t where id<2", false},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and exists(select 1 from enum_order_t r where r.id%2=l.id)", false},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l where l.id=1 and not exists(select 1 from enum_order_t r where r.id%2=l.id)", false},
		{"select min(e) over(),max(s) over() from (select e,s from enum_order_t limit 1) d", true},
		{"select min(e) over(),max(s) over() from (select e,s from enum_order_t limit 2) d", false},
		{"select min(e) over(),max(s) over() from enum_order_t limit 1", false},
		{"select min(e) over(),max(s) over() from enum_order_t where null", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l join enum_order_t r on false", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l left join enum_order_t r on false", false},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l left join enum_order_t r on false where l.id=1", true},
		{"select min(l.e) over(),max(l.s) over() from enum_order_t l right join enum_order_t r on false", false},
		{"select distinct e,s from enum_order_t where id=1 group by e,s with rollup", false},
		{"select min(min(e)) over(),max(max(s)) over() from enum_order_t where false having count(*)>0", true},
		{"select min(e) over(),max(s) over() from (select e,s from enum_order_t group by e,s with rollup) d limit 0", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := newMySQLSpecialOrderMock(t)
			mock.ctxt.tables["enum_order_t"].Pkey = &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
			mock.ctxt.tables["enum_order_t"].Cols[0].Typ.Id = int32(types.T_int32)
			mock.ctxt.tables["enum_order_t"].Cols = append(mock.ctxt.tables["enum_order_t"].Cols, &pb.ColDef{Name: "u", Typ: pb.Type{Id: int32(types.T_int64)}})
			mock.ctxt.tables["enum_order_t"].Indexes = []*pb.IndexDef{{IndexName: "u_key", Unique: true, Parts: []string{"u"}}}
			composite := DeepCopyTableDef(mock.ctxt.tables["enum_order_t"], true)
			composite.Name = "enum_composite_t"
			composite.Pkey = &pb.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"id", "u"}}
			composite.Indexes = nil
			mock.ctxt.tables[composite.Name] = composite
			mock.ctxt.objects[composite.Name] = &pb.ObjectRef{ObjName: composite.Name, Obj: 99071}
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			snapshot := proto.Clone(p)
			cols := GetResultColumnsFromPlan(p)
			require.GreaterOrEqual(t, len(cols), 2)
			for index, def := range cols {
				if index >= 2 {
					require.Nil(t, def.MysqlSpecialResultType)
					continue
				}
				require.NotNil(t, def.MysqlSpecialResultType)
				require.Equal(t, tc.elided, def.MysqlSpecialResultType.MaterializationElided)
				cloned := DeepCopyColDef(def)
				require.Equal(t, def.MysqlSpecialResultType, cloned.MysqlSpecialResultType)
				data, err := def.Marshal()
				require.NoError(t, err)
				var restored pb.ColDef
				require.NoError(t, restored.Unmarshal(data))
				require.Equal(t, def.MysqlSpecialResultType, restored.MysqlSpecialResultType)
			}
			require.True(t, proto.Equal(snapshot, p), "metadata extraction must not mutate plan maps")
		})
	}
}

func TestEnumSetParameterizedEmptyPublication(t *testing.T) {
	stmt, err := mysql.ParseOne(t.Context(), "select min(e) over(),max(s) over() from enum_order_t limit ?", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := newMySQLSpecialOrderMock(t)
	for _, source := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint64} {
		t.Run(source.String(), func(t *testing.T) {
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{{Position: 0, Type: source.ToType()}}, []any{ParamValue{Value: "0", IsBinaryProtocol: true, PrepareParamKind: vector.PrepareParamInteger}})
			require.NoError(t, err)
			snapshot := proto.Clone(bound.Plan)
			for _, count := range []any{int64(0), int64(1), nil, false, true, int64(0)} {
				for _, col := range GetResultColumnsFromPlanWithParams(bound.Plan, []any{count}) {
					require.Equal(t, count == nil || count == false || count == int64(0), col.MysqlSpecialResultType.MaterializationElided)
				}
				require.True(t, proto.Equal(snapshot, bound.Plan), "execution parameters must not specialize shared metadata")
				for _, col := range GetResultColumnsFromPlan(bound.Plan) {
					require.False(t, col.MysqlSpecialResultType.MaterializationElided, "unbound PREPARE remains value-independent")
				}
			}
		})
	}
}

func TestEnumSetNullSafeKeyParameters(t *testing.T) {
	for _, key := range []string{"id", "u"} {
		stmt, err := mysql.ParseOne(t.Context(), "select min(e)over(),max(s)over() from enum_order_t where "+key+"<=>?", 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		mock := newMySQLSpecialOrderMock(t)
		table := mock.ctxt.tables["enum_order_t"]
		table.Pkey = &pb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
		table.Cols = append(table.Cols, &pb.ColDef{Name: "u", Typ: pb.Type{Id: int32(types.T_int64)}})
		table.Indexes = []*pb.IndexDef{{IndexName: "u_key", Unique: true, Parts: []string{"u"}}}
		proc := mock.ctxt.GetProcess()
		params := vector.NewVec(types.T_int32.ToType())
		t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
		require.NoError(t, vector.AppendFixed(params, int32(1), false, proc.Mp()))
		proc.SetPrepareParams(params)
		bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{{Position: 0, Type: types.T_int32.ToType()}}, []any{int32(1)})
		require.NoError(t, err)
		if key == "u" {
			require.True(t, bound.ValueDependent, "parameter nullness must not enter the type-only plan cache")
		}
		snapshot := proto.Clone(bound.Plan)
		for _, value := range []any{int32(1), int32(2), int32(1)} {
			for _, col := range GetResultColumnsFromPlanWithParams(bound.Plan, []any{value}) {
				require.True(t, col.MysqlSpecialResultType.MaterializationElided)
			}
			require.True(t, proto.Equal(snapshot, bound.Plan))
		}
	}
}

func TestEnumSetBoundRangeMaterialization(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []any
		empty  bool
	}{
		{"reversed", []any{int32(2), int32(1)}, true},
		{"null_lower", []any{nil, int32(1)}, true},
		{"null_upper", []any{int32(1), nil}, true},
		{"range", []any{int32(1), int32(2)}, false},
		{"equal", []any{int32(1), int32(1)}, false},
		{"decimal_reversed", []any{ParamValue{Value: "2.5", HasRuntimeType: true, RuntimeType: types.Type{Oid: types.T_decimal64, Width: 18, Scale: 1}, IsBinaryProtocol: true}, ParamValue{Value: "1.5", HasRuntimeType: true, RuntimeType: types.Type{Oid: types.T_decimal64, Width: 18, Scale: 1}, IsBinaryProtocol: true}}, true},
		{"decimal_range", []any{ParamValue{Value: "1.5", HasRuntimeType: true, RuntimeType: types.Type{Oid: types.T_decimal64, Width: 18, Scale: 1}, IsBinaryProtocol: true}, ParamValue{Value: "2.5", HasRuntimeType: true, RuntimeType: types.Type{Oid: types.T_decimal64, Width: 18, Scale: 1}, IsBinaryProtocol: true}}, false},
		{"sql_decimal_text", []any{ParamValue{Value: "2.5", HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal64, Width: 2, Scale: 1}}, ParamValue{Value: "1.5", HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal64, Width: 2, Scale: 1}}}, true},
		{"sql_decimal_native", []any{ParamValue{Value: types.Decimal64(25), HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal64, Width: 2, Scale: 1}}, ParamValue{Value: types.Decimal64(15), HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal64, Width: 2, Scale: 1}}}, true},
		{"sql_decimal_128", []any{ParamValue{Value: types.Decimal128{B0_63: 25}, HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal128, Width: 38, Scale: 1}}, ParamValue{Value: types.Decimal128{B0_63: 15}, HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal128, Width: 38, Scale: 1}}}, true},
		{"sql_decimal_256", []any{ParamValue{Value: types.Decimal256{B0_63: 25}, HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal256, Width: 76, Scale: 1}}, ParamValue{Value: types.Decimal256{B0_63: 15}, HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal256, Width: 76, Scale: 1}}}, true},
		{"sql_decimal_runtime_string", []any{ParamValue{Value: "2.5", HasRuntimeType: true, RuntimeType: types.T_varchar.ToType(), HasSourceType: true, SourceType: types.T_decimal64.ToType()}, ParamValue{Value: "1.5", HasRuntimeType: true, RuntimeType: types.T_varchar.ToType(), HasSourceType: true, SourceType: types.T_decimal64.ToType()}}, false},
		{"sql_decimal_range", []any{ParamValue{Value: types.Decimal64(15), HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal64, Width: 2, Scale: 1}}, ParamValue{Value: types.Decimal64(25), HasSourceType: true, SourceType: types.Type{Oid: types.T_decimal64, Width: 2, Scale: 1}}}, false},
		{"sql_decimal_string_source", []any{ParamValue{Value: "2.5", HasSourceType: true, SourceType: types.T_varchar.ToType()}, ParamValue{Value: "1.5", HasSourceType: true, SourceType: types.T_varchar.ToType()}}, false},
		{"sql_integer_transport", []any{ParamValue{Value: int64(2)}, ParamValue{Value: int64(1)}}, true},
		{"binary_integer_text", []any{ParamValue{Value: "2", HasRuntimeType: true, RuntimeType: types.T_int32.ToType(), IsBinaryProtocol: true}, ParamValue{Value: "1", HasRuntimeType: true, RuntimeType: types.T_int32.ToType(), IsBinaryProtocol: true}}, true},
		{"binary_null", []any{ParamValue{IsBinaryProtocol: true}, ParamValue{Value: "1", HasRuntimeType: true, RuntimeType: types.T_int32.ToType(), IsBinaryProtocol: true}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), "select min(e)over(),max(s)over() from enum_order_t where id between ? and ?", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			mock := newMySQLSpecialOrderMock(t)
			mock.ctxt.tables["enum_order_t"].Cols[0].Typ.Id = int32(types.T_int32)
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
			for _, value := range tc.values {
				if typed, ok := value.(ParamValue); ok {
					value = typed.Value
				}
				require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(value)), value == nil, proc.Mp()))
			}
			proc.SetPrepareParams(params)
			bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int32.ToType()}, {Position: 1, Type: types.T_int32.ToType()}}
			if tc.name == "decimal_reversed" || tc.name == "decimal_range" {
				for i := range bindings {
					bindings[i].Type = types.Type{Oid: types.T_decimal64, Width: 18, Scale: 1}
				}
			}
			if tc.name == "sql_integer_transport" || strings.HasPrefix(tc.name, "sql_decimal_") {
				for i := range bindings {
					bindings[i].Type = types.T_text.ToType()
				}
			}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, tc.values)
			require.NoError(t, err)
			if tc.empty {
				require.True(t, bound.ValueDependent, "a bound-only rejection cannot enter the type-only plan cache")
			}
			snapshot := proto.Clone(bound.Plan)
			for _, col := range GetResultColumnsFromPlanWithParams(bound.Plan, tc.values) {
				require.NotNil(t, col.MysqlSpecialResultType)
				require.Equal(t, tc.empty, col.MysqlSpecialResultType.MaterializationElided)
			}
			require.True(t, proto.Equal(snapshot, bound.Plan))
		})
	}
}

func TestEnumSetPaginationTransportMaterialization(t *testing.T) {
	stmt, err := mysql.ParseOne(t.Context(), "select min(e) over(),max(s) over() from enum_order_t limit ?", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	mock := newMySQLSpecialOrderMock(t)
	for _, tc := range []struct {
		name   string
		source types.T
		value  any
		kind   vector.PrepareParamKind
		elided bool
	}{
		{"null", types.T_any, nil, vector.PrepareParamNone, true},
		{"false", types.T_bool, false, vector.PrepareParamBoolean, true},
		{"true", types.T_bool, true, vector.PrepareParamBoolean, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := []any{ParamValue{Value: tc.value, IsBinaryProtocol: true, PrepareParamKind: tc.kind}}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
				[]PreparedSourceBinding{{Position: 0, Type: tc.source.ToType()}}, values)
			require.NoError(t, err)
			snapshot := proto.Clone(bound.Plan)
			for _, col := range GetResultColumnsFromPlanWithParams(bound.Plan, values) {
				require.Equal(t, tc.elided, col.MysqlSpecialResultType.MaterializationElided)
			}
			require.True(t, proto.Equal(snapshot, bound.Plan))
		})
	}
}

func TestEnumSetRollupResultPresentationPlanModes(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	old, had := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables("optimizer_hints", old)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	})
	for _, hint := range []string{"rollupSort=1", "rollupSort=2"} {
		t.Run(hint, func(t *testing.T) {
			rt.SetGlobalVariables("optimizer_hints", hint)
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t,
				"select min(e), max(s) from enum_order_t group by id with rollup")
			require.NoError(t, err)
			require.Equal(t, hint == "rollupSort=1", planHasSortRollup(p.GetQuery()))
			for _, column := range GetResultColumnsFromPlan(p) {
				require.NotNil(t, column.MysqlSpecialResultType)
				require.Equal(t, pb.MySQLSpecialResultType_ROLLUP, column.MysqlSpecialResultType.Kind)
			}
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
