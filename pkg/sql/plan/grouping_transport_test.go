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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGroupingBinaryTransport(t *testing.T) {
	for _, tc := range []struct {
		sql string
		typ types.T
	}{{"binary(4)", types.T_binary}, {"varbinary(4)", types.T_varbinary}} {
		t.Run(tc.sql, func(t *testing.T) {
			opt := NewMockOptimizer(false)
			opt.ctxt.tables["nation"] = DeepCopyTableDef(opt.ctxt.tables["nation"], true)
			for _, col := range opt.ctxt.tables["nation"].Cols {
				if col.Name == "n_name" {
					col.Typ = plan.Type{Id: int32(tc.typ), Width: 4, NotNullable: true}
				}
			}
			p, err := buildSingleStmt(opt, t,
				"select n_name k,grouping(n_name),count(*) from nation group by n_name with rollup")
			require.NoError(t, err)
			q := p.GetQuery()
			for _, n := range q.Nodes {
				if n.NodeType == plan.Node_UNION_ALL {
					require.Equal(t, int32(tc.typ), n.ProjectList[0].Typ.Id,
						"internal transport must not insert a bitmap-losing BLOB cast")
				}
			}
			p, err = buildSingleStmt(opt, t, "select n_name from nation union all select n_name from nation")
			require.NoError(t, err)
			q = p.GetQuery()
			require.Equal(t, int32(types.T_blob), q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0].Typ.Id,
				"public SQL set operations retain their existing common-type rule")
		})
	}
}

func TestGroupingSampleOmittedKeys(t *testing.T) {
	p, err := buildSingleStmt(NewMockOptimizer(false), t,
		"select n_regionkey,grouping(n_regionkey),sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup")
	require.NoError(t, err)
	q := p.GetQuery()
	omitted := 0
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_SAMPLE && len(n.GroupBy) == 1 && n.GroupBy[0].GetLit().GetIsnull() {
			require.False(t, n.GroupBy[0].Typ.NotNullable)
			omitted++
		}
	}
	require.Equal(t, 1, omitted, "the subtotal must sample one global group, not the detail groups again")
	require.False(t, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0].Typ.NotNullable)
}

func TestGroupingWindowConsumerNullability(t *testing.T) {
	for _, fn := range []string{"first_value", "last_value"} {
		t.Run(fn, func(t *testing.T) {
			p, err := buildSingleStmt(NewMockOptimizer(false), t,
				"select "+fn+"(n_nationkey) over(order by n_nationkey) f from nation group by n_nationkey with rollup")
			require.NoError(t, err)
			q := p.GetQuery()
			for _, n := range q.Nodes {
				if n.NodeType == plan.Node_WINDOW {
					w := n.WinSpecList[0]
					require.False(t, w.GetW().WindowFunc.GetF().Args[0].Typ.NotNullable)
					require.False(t, w.Typ.NotNullable)
				}
			}
			require.False(t, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0].Typ.NotNullable)
		})
	}
}

func TestGroupingWindowPartitionOnlyKey(t *testing.T) {
	p, err := buildSingleStmt(NewMockOptimizer(false), t,
		"select first_value(n_nationkey) over(partition by n_regionkey order by n_nationkey) f from nation group by n_nationkey,n_regionkey with rollup")
	require.NoError(t, err, "partition-only keys can be pruned before the WINDOW input")
	q := p.GetQuery()
	require.False(t, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0].Typ.NotNullable)
}

func TestGroupingDistinctCompositeOrder(t *testing.T) {
	for _, order := range []string{"(n_nationkey,n_regionkey)", "(n_nationkey+1,n_regionkey)"} {
		_, err := buildSingleStmt(NewMockOptimizer(false), t,
			"select distinct n_nationkey from nation group by n_nationkey,n_regionkey with rollup order by "+order)
		require.Error(t, err, "every nested column must survive DISTINCT")
	}
}

func TestGroupingTransportOnlyConsumers(t *testing.T) {
	for _, tc := range []struct {
		query string
		cols  int
	}{
		{"select sum(c) from (select count(*) c from nation group by n_nationkey,n_regionkey with rollup) t", 1},
		{"select count(*) from nation group by n_nationkey,n_regionkey with rollup order by n_regionkey", 2},
		{"select first_value(n_nationkey) over(order by n_nationkey) from nation group by n_nationkey,n_regionkey with rollup", 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			p, err := buildSingleStmt(NewMockOptimizer(false), t, tc.query)
			require.NoError(t, err)
			found := 0
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == plan.Node_UNION_ALL {
					require.Len(t, n.ProjectList, tc.cols, "only actual consumers justify transport columns")
					found++
				}
			}
			require.Positive(t, found)
		})
	}
}

func TestGroupingSampleDistinctOrderExpression(t *testing.T) {
	for _, prefix := range []string{"", "create table ordered_sample as "} {
		_, err := buildSingleStmt(NewMockOptimizer(false), t, prefix+
			"select distinct n_regionkey,sample(n_nationkey,100 percent) chosen from nation group by grouping sets ((n_regionkey),(n_regionkey)) order by chosen+1")
		require.NoError(t, err, "a sort expression built from selected sample output is legal after DISTINCT")
	}
}
