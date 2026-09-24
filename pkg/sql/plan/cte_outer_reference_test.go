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
			name: "recursive ancestor predicate",
			sql: `select p.n_nationkey from tpch.nation p where not exists (
				with recursive ancestors as (
					select a.* from tpch.nation a where a.n_nationkey=p.n_regionkey
					union all
					select a.* from tpch.nation a join ancestors
						on ancestors.n_regionkey=a.n_nationkey
				) select * from ancestors where n_name='inactive'
			) and p.n_name='active'`,
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
	_, err = builder.parameterizeLocalCTEs(outerID, root, ctx)
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

func TestLocalCTEOuterReferencesRejectUnsafeDomains(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
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
