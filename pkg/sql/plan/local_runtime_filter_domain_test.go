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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func localDomainTestQuery() *planpb.Query {
	typ := planpb.Type{Id: int32(types.T_int64)}
	scan := func(id int32, rows float64) *planpb.Node {
		return &planpb.Node{NodeId: id, NodeType: planpb.Node_TABLE_SCAN,
			ProjectList: []*planpb.Expr{GetColExpr(typ, 0, 0), GetColExpr(typ, 0, 1)},
			TableDef:    &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "key", Typ: typ}, {Name: "other", Typ: typ}}},
			Stats:       &planpb.Stats{Outcnt: rows, TableCnt: rows}}
	}
	probe, source, fact := scan(0, 10000), scan(1, 1000), scan(3, 100000)
	source.Stats.Outcnt = 4
	source.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "key", Names: []string{"key"}}
	source.FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
	lower := &planpb.Node{NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{0, 1}, OnList: []*planpb.Expr{makeRuntimeFilterTestEq(typ, 0, 1, 0, 0)},
		ProjectList: []*planpb.Expr{GetColExpr(typ, 0, 0), GetColExpr(typ, 0, 1), GetColExpr(typ, 1, 0)}}
	upper := &planpb.Node{NodeId: 4, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{3, 2}, OnList: []*planpb.Expr{
			makeRuntimeFilterTestEq(typ, 0, 1, 0, 2),
			makeRuntimeFilterTestEq(typ, 0, 1, 1, 1),
		}, ProjectList: []*planpb.Expr{GetColExpr(typ, 0, 0)}}
	return &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{4}, Nodes: []*planpb.Node{probe, source, lower, fact, upper}}
}

func TestLocalRuntimeFilterDomainCompositeLineage(t *testing.T) {
	narrow := types.New(types.T_decimal64, 1, 0)
	fallible, err := makePlan2CastExpr(context.Background(),
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 0, 1), makePlan2Type(&narrow))
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		change  func(*planpb.Query)
		targets []int32
	}{
		{name: "necessary component reaches both scans", targets: []int32{0, 3}},
		{name: "commuted equality", targets: []int32{0, 3}, change: func(q *planpb.Query) {
			a := q.Nodes[4].OnList[0].GetF().Args
			a[0], a[1] = a[1], a[0]
		}},
		{name: "limit is a barrier", targets: []int32{0}, change: func(q *planpb.Query) {
			q.Nodes[3].Limit = makePlan2Int64ConstExprWithType(1)
		}},
		{name: "outer join", change: func(q *planpb.Query) { q.Nodes[4].JoinType = planpb.Node_LEFT }},
		{name: "source wait", change: func(q *planpb.Query) {
			q.Nodes[1].RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{{Tag: 1}}
		}},
		{name: "already filtered target keeps its reader path", targets: []int32{0}, change: func(q *planpb.Query) {
			q.Nodes[3].FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{name: "source unselective", change: func(q *planpb.Query) { q.Nodes[1].Stats.Outcnt = 1000 }},
		{name: "computed scan projection cannot be skipped", change: func(q *planpb.Query) {
			q.Nodes[3].ProjectList = append(q.Nodes[3].ProjectList, DeepCopyExpr(fallible))
		}},
		{name: "computed join projection cannot be skipped", change: func(q *planpb.Query) {
			q.Nodes[2].ProjectList = append(q.Nodes[2].ProjectList, DeepCopyExpr(fallible))
		}},
		{name: "estimate above ten percent waits for actual cardinality", targets: []int32{0, 3}, change: func(q *planpb.Query) { q.Nodes[1].Stats.Outcnt = 200 }},
		{name: "source not single primary key", change: func(q *planpb.Query) {
			q.Nodes[1].TableDef.Pkey.Names = []string{"key", "other"}
		}},
		{name: "incompatible integer", change: func(q *planpb.Query) {
			q.Nodes[4].OnList[0].GetF().Args[0].Typ.Id = int32(types.T_uint64)
		}},
		{name: "shared source", change: func(q *planpb.Query) { q.Nodes[4].Children[0] = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := localDomainTestQuery()
			if tc.change != nil {
				tc.change(q)
			}
			before := proto.Clone(q).(*planpb.Query)
			domains := LocalRuntimeFilterDomains(q, func(*planpb.Node, string) float64 { return 1000 })
			if len(tc.targets) == 0 {
				require.Empty(t, domains)
			} else {
				require.Len(t, domains, 1)
				require.Equal(t, int32(1), domains[0].SourceScanNodeId)
				require.Equal(t, "key", domains[0].SourceColumnName)
				targets := make([]int32, 0, len(domains[0].Targets))
				for _, target := range domains[0].Targets {
					targets = append(targets, target.ScanNodeId)
				}
				require.Equal(t, tc.targets, targets)
			}
			require.True(t, proto.Equal(before, q), "analysis does not change joins, estimates, placement or existing wait specs")
		})
	}
}

func TestLocalRuntimeFilterDomainStatsAndCopy(t *testing.T) {
	q := localDomainTestQuery()
	require.Empty(t, LocalRuntimeFilterDomains(q, func(*planpb.Node, string) float64 { return -1 }))
	q.LocalRuntimeFilterDomains = LocalRuntimeFilterDomains(q, func(*planpb.Node, string) float64 { return 1000 })
	require.Len(t, q.LocalRuntimeFilterDomains, 1)
	copy := DeepCopyQuery(q)
	require.True(t, proto.Equal(q, copy))
	copy.LocalRuntimeFilterDomains[0].Targets[0].ColumnName = "changed"
	copy.LocalRuntimeFilterDomains[0].Targets[0].ProbeNdv = 1
	require.Equal(t, "key", q.LocalRuntimeFilterDomains[0].Targets[0].ColumnName)
	require.Equal(t, float64(1000), q.LocalRuntimeFilterDomains[0].Targets[0].ProbeNdv)
	require.Empty(t, ValidLocalRuntimeFilterDomains(copy))
	bytes, err := q.Marshal()
	require.NoError(t, err)
	var decoded planpb.Query
	require.NoError(t, decoded.Unmarshal(bytes))
	require.True(t, proto.Equal(q, &decoded))
}

func TestLocalRuntimeFilterDomainParsedQuery(t *testing.T) {
	stmt, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, `
		select l.l_orderkey from lineitem l
		join partsupp ps on l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey
		join part p on ps.ps_partkey = p.p_partkey
		where p.p_name like '%green%'`)
	require.NoError(t, err)
	q := stmt.GetQuery()
	for _, n := range q.Nodes {
		if n.NodeType != planpb.Node_TABLE_SCAN {
			continue
		}
		if n.Stats == nil {
			n.Stats = &planpb.Stats{}
		}
		switch n.TableDef.Name {
		case "part":
			n.Stats.TableCnt, n.Stats.Outcnt = 20000000, 4000000
		case "partsupp":
			n.Stats.TableCnt, n.Stats.Outcnt = 80000000, 80000000
		case "lineitem":
			n.Stats.TableCnt, n.Stats.Outcnt = 600000000, 600000000
		}
	}
	domains := LocalRuntimeFilterDomains(q, func(*planpb.Node, string) float64 { return 20000000 })
	require.NotEmpty(t, domains, "real finalized SQL lineage must expose the selective domain")
	q.LocalRuntimeFilterDomains = domains
	(&BaseOptimizer{}).pruneUsedNodes(q)
	domains = ValidLocalRuntimeFilterDomains(q)
	require.NotEmpty(t, domains, "annotations must follow the production optimizer's final numbering")
	var targets []string
	for _, domain := range domains {
		if domain.SourceColumnName != "p_partkey" {
			continue
		}
		for _, target := range domain.Targets {
			targets = append(targets, q.Nodes[target.ScanNodeId].TableDef.Name)
		}
	}
	require.ElementsMatch(t, []string{"lineitem", "partsupp"}, targets)
}

func TestLocalRuntimeFilterDomainCompactionIdentity(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "live endpoints keep identity", true: "removed endpoint cannot alias a reused ID"}[invalid], func(t *testing.T) {
			q := localDomainTestQuery()
			q.LocalRuntimeFilterDomains = LocalRuntimeFilterDomains(q, func(*planpb.Node, string) float64 { return 1000 })
			source := q.Nodes[1]
			if invalid {
				dead := DeepCopyNode(source)
				dead.NodeId = 0 // this number will be owned by a different live node
				q.Nodes = append(q.Nodes, dead)
				q.LocalRuntimeFilterDomains[0].SourceScanNodeId = int32(len(q.Nodes) - 1)
			}
			(&BaseOptimizer{}).pruneUsedNodes(q)
			if invalid {
				require.Empty(t, q.LocalRuntimeFilterDomains)
			} else {
				require.Len(t, q.LocalRuntimeFilterDomains, 1)
				id := q.LocalRuntimeFilterDomains[0].SourceScanNodeId
				require.Same(t, source, q.Nodes[id])
				require.NotEqual(t, int32(1), id, "fixture must exercise an actual numbering change")
				require.Len(t, ValidLocalRuntimeFilterDomains(q), 1)
			}
		})
	}
}
