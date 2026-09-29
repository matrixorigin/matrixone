// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestLocalCTEOuterReferencesExecutablePlan(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "split where constrains recursive producer domain",
			sql: `select p.n_nationkey from tpch.nation p where not exists (
				with recursive ancestors as (
					select a.* from tpch.nation a where a.n_nationkey=p.n_regionkey
					union all
					select a.* from tpch.nation a join ancestors
						on ancestors.n_regionkey=a.n_nationkey
				) select * from ancestors where n_name='inactive'
			) and p.n_name='active'`,
		},
		{
			name: "split where constrains throwing producer",
			sql: `select p.n_nationkey from tpch.nation p where p.n_nationkey=2 and
				(with q(n) as (select abs(p.n_regionkey)) select n from q)>0`,
		},
		{
			name: "split where constrains throwing consumer",
			sql: `select p.n_nationkey from tpch.nation p where p.n_nationkey=2 and
				(with q(n) as (select p.n_regionkey) select abs(n) from q)>0`,
		},
		{
			name: "split where conjuncts reversed",
			sql: `select p.n_nationkey from tpch.nation p where
				(with q(n) as (select abs(p.n_regionkey)) select n from q)>0 and p.n_nationkey=2`,
		},
		{
			name: "consumer where constrains producer domain",
			sql: `select p.n_nationkey, (with q(n) as (select abs(p.n_regionkey))
				select n from q where p.n_nationkey=2) from tpch.nation p`,
		},
		{
			name: "consumer where constrains consumer domain",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_regionkey)
				select abs(n) from q where p.n_nationkey=2) from tpch.nation p`,
		},
		{
			name: "distinct consumer demand preserves identity",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_regionkey)
				select abs(n) from (select distinct n from q) d where p.n_nationkey=2)
				from tpch.nation p`,
		},
		{
			name: "paginated consumer demand preserves filter order",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_regionkey)
				select abs(n) from (select n from q limit 1) d where p.n_nationkey=2 or n>0)
				from tpch.nation p`,
		},
		{
			name: "distinct consumer text physical keys preserve identity",
			sql: `select p.n_nationkey, (with q(n) as
				(select cast(2 as char(5)) from tpch.nation a where p.n_nationkey>0)
				select cast(n as signed) from (select distinct n from q) d where p.n_nationkey>0)
				from tpch.nation p`,
		},
		{
			name: "mixed consumer predicate stays below projection",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_regionkey)
				select abs(n) from q where p.n_nationkey=2 or n>0) from tpch.nation p`,
		},
		{
			name: "ordinary count having user variable",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*)=@having_limit)
				from tpch.nation p`,
		},
		{
			name: "ordinary count having system variable",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*)=@@session.auto_increment_increment)
				from tpch.nation p`,
		},
		{
			name: "ordinary count having outer reference",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*)=p.n_regionkey)
				from tpch.nation p`,
		},
		{
			name: "ordinary count having multiple conjuncts",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*)>0 and count(*)<2)
				from tpch.nation p`,
		},
		{
			name: "ordinary count having in list",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*) in (0,1))
				from tpch.nation p`,
		},
		{
			name: "local count in empty group",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_nationkey where p.n_nationkey<2)
				select count(*) in (0,1) from q) from tpch.nation p`,
		},
		{
			name: "local count case empty group",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_nationkey where p.n_nationkey<2)
				select case when count(*) in (0,1) then 7 else 8 end from q) from tpch.nation p`,
		},
		{
			name: "local count having multiple conjuncts",
			sql: `select p.n_nationkey, (with q(n) as
				(select p.n_nationkey from tpch.nation a where a.n_nationkey=p.n_nationkey)
				select count(*) from q having count(*)>0 and count(*)<2) from tpch.nation p`,
		},
		{
			name: "split where safe consumer",
			sql: `select p.n_nationkey from tpch.nation p where p.n_nationkey=2 and
				(with q(n) as (select p.n_regionkey) select n from q)>0`,
		},
		{
			name: "unguarded producer cast on filtered domain",
			sql: `select (with q(n) as (select cast(p.n_name as signed)) select n from q)
				from tpch.nation p where p.n_nationkey=2`,
		},
		{
			name: "paginated safe local producer",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_regionkey) select n from q)
				from tpch.nation p order by p.n_nationkey desc limit 1`,
		},
		{
			name: "outer join on safe local producer",
			sql: `select p.n_nationkey, b.n_nationkey from tpch.nation p
				left join tpch.nation b on case when p.n_nationkey=1 then false else
				(with q(n) as (select p.n_regionkey) select n from q)>0 end`,
		},
		{
			name: "unguarded producer abs on filtered domain",
			sql: `select (with q(n) as (select abs(p.n_regionkey)) select n from q)
				from tpch.nation p where p.n_nationkey=2`,
		},
		{
			name: "safe producer cast",
			sql: `select p.n_nationkey, (with q(n) as
				(select cast(p.n_nationkey as signed)) select n from q) from tpch.nation p`,
		},
		{
			name: "count expression having limit one",
			sql: `select p.n_nationkey, (select count(*)+1 from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*)=1 limit 1)
				from tpch.nation p`,
		},
		{
			name: "count expression multiply limit one",
			sql: `select p.n_nationkey, (select count(*)*2 from tpch.nation a
				where a.n_nationkey=p.n_nationkey limit 1) from tpch.nation p`,
		},
		{
			name: "two counts with order",
			sql: `select p.n_nationkey, (select count(*)+count(a.n_nationkey) from tpch.nation a
				where a.n_nationkey=p.n_nationkey order by count(*)) from tpch.nation p`,
		},
		{
			name: "local count expression having limit one",
			sql: `select p.n_nationkey, (with q(n) as
				(select p.n_nationkey from tpch.nation a where a.n_nationkey=p.n_nationkey)
				select count(*)+1 from q having count(*)=1 limit 1) from tpch.nation p`,
		},
		{
			name: "local two counts with order",
			sql: `select p.n_nationkey, (with q(n) as
				(select p.n_nationkey from tpch.nation a where a.n_nationkey=p.n_nationkey)
				select count(*)+count(n) from q order by count(*)) from tpch.nation p`,
		},
		{
			name: "bit or offset deletes scalar result",
			sql: `select p.n_nationkey, (select bit_or(a.n_nationkey) from tpch.nation a
				where a.n_nationkey=p.n_nationkey limit 1 offset 1) from tpch.nation p`,
		},
		{
			name: "bit and offset deletes scalar result",
			sql: `select p.n_nationkey, (select bit_and(a.n_nationkey) from tpch.nation a
				where a.n_nationkey=p.n_nationkey limit 1 offset 1) from tpch.nation p`,
		},
		{
			name: "ordinary count having limit zero",
			sql: `select p.n_nationkey, (select count(*)+1 from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*)>=0 limit 0)
				from tpch.nation p`,
		},
		{
			name: "ordinary count having offset deletes result",
			sql: `select p.n_nationkey, (select count(*)+1 from tpch.nation a
				where a.n_nationkey=p.n_nationkey having count(*)>=0 limit 1 offset 1)
				from tpch.nation p`,
		},
		{
			name: "ordinary grouped having control",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey group by a.n_nationkey having count(*)=1)
				from tpch.nation p`,
		},
		{
			name: "ordinary count order limit one",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey and a.n_nationkey<2 order by count(*) limit 1)
				from tpch.nation p`,
		},
		{
			name: "ordinary count order wrapper",
			sql: `select p.n_nationkey, (select count(*) from tpch.nation a
				where a.n_nationkey=p.n_nationkey and a.n_nationkey<2 order by count(*))
				from tpch.nation p`,
		},
		{
			name: "local count order wrapper",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_regionkey from tpch.nation a
				where a.n_nationkey=p.n_nationkey and a.n_nationkey<2)
				select count(*) from q order by count(*)) from tpch.nation p`,
		},
		{
			name: "ordinary scalar control",
			sql:  `select p.n_nationkey, (select p.n_regionkey) from tpch.nation p`,
		},
		{
			name: "noncorrelated recursive control",
			sql: `select p.n_nationkey, (with recursive r(n) as (
				select 3 union all select n-1 from r where n>1
			) select count(*) from r) from tpch.nation p`,
		},
		{
			name: "local scalar projection",
			sql: `select p.n_nationkey,
				(with q(n) as (select p.n_regionkey) select n from q)
				from tpch.nation p`,
		},
		{
			name: "recursive seed projection",
			sql: `select p.n_nationkey, (with recursive r(n) as (
				select p.n_regionkey union all select n-1 from r where n>1
			) select count(*) from r) from tpch.nation p`,
		},
		{
			name: "filtered outer domain",
			sql: `select (with recursive r(n) as (
				select p.n_regionkey union all select n-1 from r where n>1
			) select count(*) from r) from tpch.nation p where p.n_nationkey=99`,
		},
		{
			name: "recursive member parameter",
			sql: `select p.n_nationkey, (with recursive r(n) as (
				select 1 union all select n+1 from r where n<p.n_regionkey
			) select count(*) from r) from tpch.nation p`,
		},
		{
			name: "recursive distinct identity",
			sql: `select p.n_nationkey, (with recursive r(n) as (
				select p.n_regionkey union distinct select n from r where n is not null
			) select count(*) from r) from tpch.nation p`,
		},
		{
			name: "explicit group count empty input",
			sql: `select (with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey=-1)
				select count(*) from q group by n) from tpch.nation p`,
		},
		{
			name: "explicit group count with offset",
			sql: `select (with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey=-1)
				select count(*) from q group by n limit 1 offset 1) from tpch.nation p`,
		},
		{
			name: "consumer window per outer row",
			sql: `select (with q(n) as (select p.n_regionkey)
				select row_number() over (order by n) from q) from tpch.nation p`,
		},
		{
			name: "count having consumer",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(*) from q having count(*)=0) from tpch.nation p`,
		},
		{
			name: "union all consumer",
			sql: `select p.n_nationkey from tpch.nation p where exists (
				with q(n) as (select p.n_regionkey)
				select n from q union all select n from q)`,
		},
		{
			name: "multiple consumers",
			sql: `select (with q(n) as (select p.n_regionkey)
				select a.n+b.n from q a join q b on a.n=b.n) from tpch.nation p`,
		},
		{
			name: "multiple outer parameters",
			sql: `select (with recursive r(n) as (
				select p.n_regionkey+p.n_nationkey
				union all select n-1 from r where n>p.n_regionkey
			) select count(*) from r) from tpch.nation p`,
		},
		{
			name: "chained producers",
			sql: `select (with q(n) as (select p.n_regionkey),
				r(n) as (select n+1 from q) select n from r) from tpch.nation p`,
		},
		{
			name: "correlated join branches",
			sql: `select (with q(n) as (select a.n_nationkey+b.n_nationkey
				from (select n_nationkey from tpch.nation where n_regionkey=p.n_regionkey) a
				join (select n_nationkey from tpch.nation where n_regionkey=p.n_regionkey) b
				on a.n_nationkey=b.n_nationkey) select n from q limit 1) from tpch.nation p`,
		},
		{
			name: "recursive ancestor predicate without skipped conjunct",
			sql: `select p.n_nationkey from tpch.nation p where not exists (
				with recursive ancestors as (
					select a.* from tpch.nation a where a.n_nationkey=p.n_regionkey
					union all
					select a.* from tpch.nation a join ancestors
						on ancestors.n_regionkey=a.n_nationkey
				) select * from ancestors where n_name='inactive'
			)`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			assertReachablePlanHasNoCorrelatedExpr(t, query)
		})
	}
}

func TestLocalCTEDomainAdmissionIsAtomic(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, false)
	ctx := NewBindContext(builder, nil)
	rowType := &planpb.Type{Id: int32(types.T_Rowid), NotNullable: true}
	valueType := &planpb.Type{Id: int32(types.T_int32)}
	outerTag := builder.genNewBindTag()
	outerID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{outerTag}}, ctx)
	ctx.bindings = []*Binding{{tag: outerTag, nodeId: outerID,
		cols: []string{catalog.Row_ID, "v"}, colIsHidden: []bool{true, false}, types: []*planpb.Type{rowType, valueType}}}
	makeProducer := func(limited bool) int32 {
		leaf := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, ctx)
		project := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{leaf},
			BindingTags: []int32{builder.genNewBindTag()}, ProjectList: []*planpb.Expr{{
				Typ: *valueType, Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{RelPos: outerTag, ColPos: 1, Depth: 1}},
			}}}
		if limited {
			project.Limit = makePlan2Uint64ConstExprWithType(1)
		}
		return builder.appendNode(project, ctx)
	}
	good, bad := makeProducer(false), makeProducer(true)
	root := builder.appendNode(&planpb.Node{NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{good, bad}}, ctx)
	builder.localCTERoots = map[int32]bool{good: true, bad: true}
	before, err := builder.qry.Marshal()
	require.NoError(t, err)
	_, err = builder.parameterizeLocalCTEs(outerID, root, ctx, planpb.SubqueryRef_SCALAR, false)
	require.ErrorContains(t, err, "producer contains pagination")
	after, err := builder.qry.Marshal()
	require.NoError(t, err)
	require.Equal(t, before, after, "rejecting a later producer must not publish the earlier rewrite")
}

func TestPreparedLocalCTEOuterReferences(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, `
		select (with recursive r(n) as (
			select p.n_regionkey union all select n-1 from r where n>1
		) select count(*) from r) from tpch.nation p where p.n_nationkey=?`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, true)
	require.NoError(t, err)
	assertReachablePlanHasNoCorrelatedExpr(t, logicPlan.GetQuery())
}

func TestLocalCTEGuardedLiteralProof(t *testing.T) {
	lit := func(value int64) *planpb.Literal {
		return &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}}
	}
	require.True(t, localCTEIntegerLiteralFits(lit(2), types.T_int32))
	require.False(t, localCTEIntegerLiteralFits(lit(2147483648), types.T_int32))
	require.False(t, localCTEIntegerLiteralFits(lit(-1), types.T_uint32))
	require.False(t, localCTEIntegerLiteralFits(&planpb.Literal{
		Value: &planpb.Literal_U64Val{U64Val: ^uint64(0)},
	}, types.T_int32))
	require.True(t, localCTEStringLiteralFits(&planpb.Literal{
		Value: &planpb.Literal_Sval{Sval: "active"},
	}, planpb.Type{Id: int32(types.T_varchar), Width: 20}))
	require.False(t, localCTEStringLiteralFits(lit(42), planpb.Type{Id: int32(types.T_varchar), Width: 20}))
}

func TestLocalCTEOuterReferencesRejectUnsafeDomains(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "non equality having must not lose filter",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(n) from q where n<=p.n_nationkey having count(n)=0)
				from tpch.nation p`,
		},
		{
			name: "exists empty aggregate is one row",
			sql: `select p.n_nationkey from tpch.nation p where exists (
				with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey=-1)
				select count(*) from q)`,
		},
		{
			name: "in empty aggregate is one row",
			sql: `select p.n_nationkey from tpch.nation p where 0 in (
				with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey=-1)
				select count(*) from q)`,
		},
		{
			name: "union empty aggregate result",
			sql: `select p.n_nationkey from tpch.nation p where exists (
				with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey=-1)
				select count(*) from q union all select count(*) from q)`,
		},
		{
			name: "nested empty aggregates",
			sql: `select (with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey=-1)
				select sum(c) from (select count(*) as c from q) s)
				from tpch.nation p`,
		},
		{
			name: "window predicate before row numbering",
			sql: `select (with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey in (1,2))
				select row_number() over (order by n desc) from q where n<=p.n_nationkey)
				from tpch.nation p`,
		},
		{
			name: "count pagination without having limit zero",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(*) from q limit 0) from tpch.nation p`,
		},
		{
			name: "count pagination without having offset",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(*) from q limit 1 offset 1) from tpch.nation p`,
		},
		{
			name: "grouped count window cannot use count fallback",
			sql: `select (with q(n) as (select p.n_regionkey)
				select row_number() over (order by count(*)) from q group by n)
				from tpch.nation p`,
		},
		{
			name: "join on outer reference must not reach executor",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(*) from q join tpch.nation b
				on n=b.n_regionkey and n=p.n_regionkey) from tpch.nation p`,
		},
		{
			name: "window count fallback without having",
			sql: `select (with q(n) as (select p.n_regionkey)
				select row_number() over (order by count(*)) from q) from tpch.nation p`,
		},
		{
			name: "having with limit zero",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(*) from q having count(*)=0 limit 0) from tpch.nation p`,
		},
		{
			name: "having with offset",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(*) from q having count(*)=0 limit 1 offset 1) from tpch.nation p`,
		},
		{
			name: "having in union branches",
			sql: `select p.n_nationkey from tpch.nation p where exists (
				with q(n) as (select p.n_regionkey from tpch.nation a where a.n_nationkey=-1)
				select count(*) from q having count(*)=0
				union all select count(*) from q having count(*)=0)`,
		},
		{
			name: "outer join consumer preserves left rows",
			sql: `select (with q(n) as (select p.n_regionkey)
				select count(*) from tpch.nation a left join q on a.n_regionkey=q.n)
				from tpch.nation p`,
		},
		{
			name: "having result is not count",
			sql: `select (with q(n) as (select p.n_regionkey)
				select row_number() over (order by count(*)) from q having count(*)=1) from tpch.nation p`,
		},
		{
			name: "non scalar count having",
			sql: `select p.n_nationkey from tpch.nation p where exists (
				with q(n) as (select p.n_regionkey)
				select count(*) from q having count(*)=0)`,
		},
		{
			name: "one union arm has no identity",
			sql: `select p.n_nationkey from tpch.nation p where exists (
				with q(n) as (select p.n_regionkey)
				select n from q union all select 7)`,
		},
		{
			name: "outer join multiplicity",
			sql: `select (with q(n) as (select p.n_regionkey) select n from q)
				from tpch.nation p join tpch.nation a on a.n_regionkey=p.n_regionkey`,
		},
		{
			name: "recursive limit",
			sql: `select (with recursive r(n) as (
				select p.n_regionkey union all select n-1 from r where n>1 limit 2
			) select count(*) from r) from tpch.nation p`,
		},
		{
			name: "explicit values expression executor",
			sql: `select (with q(n) as (select p.n_regionkey+v.x from (values row(rand())) v(x))
				select n from q) from tpch.nation p`,
		},
		{
			name: "guarded recursive operator can fail on skipped partition",
			sql: `select p.n_nationkey, case when p.n_nationkey=2 then
				(with recursive r(n) as (
					select p.n_nationkey union all select n from r where n=1
				) select count(*) from r) else 0 end from tpch.nation p`,
		},
		{
			name: "guarded recursive member arithmetic needs totality proof",
			sql: `select p.n_nationkey, case when p.n_nationkey=1 then 0 else
				(with recursive r(n) as (
					select p.n_regionkey union all select n-1 from r where n>1
				) select count(*) from r) end from tpch.nation p`,
		},
		{
			name: "guarded chained producer arithmetic needs totality proof",
			sql: `select case when p.n_nationkey=1 then 0 else
				(with q(n) as (select p.n_regionkey),
				r(n) as (select n+1 from q) select n from r) end
				from tpch.nation p`,
		},
		{
			name: "throwing consumer abs in inactive case",
			sql: `select p.n_nationkey, case when p.n_nationkey=1 then 0 else
				(with q(n) as (select p.n_regionkey) select abs(n) from q) end
				from tpch.nation p`,
		},
		{
			name: "throwing consumer arithmetic in inactive case",
			sql: `select p.n_nationkey, case when p.n_nationkey=1 then 0 else
				(with q(n) as (select p.n_regionkey) select n-1 from q) end
				from tpch.nation p`,
		},
		{
			name: "outer join on conditional producer abs",
			sql: `select p.n_nationkey, b.n_nationkey from tpch.nation p
				left join tpch.nation b on case when p.n_nationkey=1 then false else
				(with q(n) as (select abs(p.n_regionkey)) select n from q)>0 end`,
		},
		{
			name: "outer join on conditional consumer abs",
			sql: `select p.n_nationkey, b.n_nationkey from tpch.nation p
				left join tpch.nation b on case when p.n_nationkey=1 then false else
				(with q(n) as (select p.n_regionkey) select abs(n) from q)>0 end`,
		},
		{
			name: "outer limit skips throwing producer",
			sql: `select p.n_nationkey, (with q(n) as (select abs(p.n_regionkey)) select n from q)
				from tpch.nation p order by p.n_nationkey desc limit 1`,
		},
		{
			name: "outer offset skips throwing consumer",
			sql: `select p.n_nationkey, (with q(n) as (select p.n_regionkey) select abs(n) from q)
				from tpch.nation p order by p.n_nationkey limit 1 offset 1`,
		},
		{
			name: "throwing producer abs in inactive case",
			sql: `select p.n_nationkey, case when p.n_nationkey=1 then 0 else
				(with q(n) as (select abs(p.n_regionkey)) select n from q) end
				from tpch.nation p`,
		},
		{
			name: "throwing producer cast in inactive case",
			sql: `select p.n_nationkey, case when p.n_nationkey=1 then 0 else
				(with q(n) as (select cast(p.n_name as signed)) select n from q) end
				from tpch.nation p`,
		},
		{
			name: "throwing producer cast in active case",
			sql: `select p.n_nationkey, case when p.n_nationkey=1 then
				(with q(n) as (select cast(p.n_name as signed)) select n from q) else 0 end
				from tpch.nation p`,
		},
		{
			name: "volatile producer",
			sql: `select (with q(n) as (select p.n_regionkey+rand()) select n from q)
				from tpch.nation p`,
		},
		{
			name: "producer pagination",
			sql: `select (with q(n) as (select p.n_regionkey from tpch.nation limit 1)
				select n from q) from tpch.nation p`,
		},
		{
			name: "producer grouping",
			sql: `select (with q(n) as (select p.n_regionkey+count(*) from tpch.nation)
				select n from q) from tpch.nation p`,
		},
		{
			name: "producer outer join",
			sql: `select (with q(n) as (select p.n_regionkey from tpch.nation a
				left join tpch.nation b on a.n_nationkey=b.n_nationkey)
				select n from q) from tpch.nation p`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.ErrorContains(t, err, "correlated local CTE")
		})
	}
}
