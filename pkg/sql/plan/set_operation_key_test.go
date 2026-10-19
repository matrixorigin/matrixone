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
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSetOperationFloatPhysicalKeys(t *testing.T) {
	for _, op := range []string{"union", "intersect", "intersect all", "except", "except all", "union all"} {
		t.Run(op, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"select cast(-0e0 as float) f,cast('a' as varchar(4)) s "+op+" select cast(0e0 as float),cast('a ' as char(4))")
			require.NoError(t, err)
			found := false
			for _, n := range p.GetQuery().Nodes {
				if !isSetOperation(n.NodeType) {
					continue
				}
				found = true
				if n.NodeType == planpb.Node_UNION_ALL {
					require.Empty(t, n.PhysicalEqualityKeyList)
					continue
				}
				require.Len(t, n.ProjectList, 2)
				require.Equal(t, int32(types.T_float32), n.ProjectList[0].Typ.Id)
				require.Len(t, n.PhysicalEqualityKeyList, 2)
				require.Equal(t, "set_operation_float_key", n.PhysicalEqualityKeyList[0].GetF().Func.ObjName)
				require.Equal(t, int32(types.T_uint64), n.PhysicalEqualityKeyList[0].Typ.Id)
				visible := n.ProjectList[0].GetCol()
				physical := n.PhysicalEqualityKeyList[0].GetF().Args[0].GetCol()
				require.Equal(t, visible.RelPos, physical.RelPos)
				require.Equal(t, visible.ColPos, physical.ColPos)
				require.Equal(t, "cast", n.PhysicalEqualityKeyList[1].GetF().Func.ObjName)
			}
			require.True(t, found)
		})
	}
}

func TestPreparedSetOperationFloatKeysFollowOutputDomain(t *testing.T) {
	for _, kind := range []planpb.Node_NodeType{planpb.Node_UNION, planpb.Node_INTERSECT, planpb.Node_INTERSECT_ALL, planpb.Node_MINUS, planpb.Node_MINUS_ALL} {
		t.Run(kind.String(), func(t *testing.T) {
			node := &planpb.Node{NodeType: kind}
			for _, oid := range []types.T{types.T_int64, types.T_float64, types.T_int64, types.T_float32, types.T_varchar} {
				node.ProjectList = []*planpb.Expr{GetColExpr(planpb.Type{Id: int32(oid), Scale: 2}, 0, 0)}
				require.NoError(t, refreshPreparedSetOperationPhysicalKeys(t.Context(), node, nil))
				features, err := planpb.RequiredRemoteExpressionFeatures(node)
				require.NoError(t, err)
				require.Equal(t, oid == types.T_float32 || oid == types.T_float64, features.SetOperationFloatKeys)
				if oid != types.T_float32 && oid != types.T_float64 {
					require.Empty(t, node.PhysicalEqualityKeyList)
					continue
				}
				require.Len(t, node.PhysicalEqualityKeyList, 1)
				key := node.PhysicalEqualityKeyList[0]
				require.Equal(t, "set_operation_float_key", key.GetF().Func.ObjName)
				require.Equal(t, node.ProjectList[0].Typ, key.GetF().Args[0].Typ)
				require.NotSame(t, node.ProjectList[0], key.GetF().Args[0])
			}
		})
	}
	_, err := buildSetOperationPhysicalKeys(t.Context(), []*planpb.Expr{nil}, []bool{false})
	require.Error(t, err)
	_, err = buildSetOperationPhysicalKeys(t.Context(), []*planpb.Expr{}, []bool{true})
	require.Error(t, err)
}

func TestIdenticalFloatUnionKeepsSetEquality(t *testing.T) {
	for _, query := range []string{
		"select cast(-0e0 as double) union select cast(-0e0 as double)",
		"(select cast(-0e0 as double)) union (select cast(-0e0 as double))",
	} {
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
		require.NoError(t, err)
		found := false
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType == planpb.Node_UNION {
				found = true
				require.Len(t, node.Children, 2)
				require.Len(t, node.PhysicalEqualityKeyList, 1)
				require.Equal(t, "set_operation_float_key", node.PhysicalEqualityKeyList[0].GetF().Func.ObjName)
			}
		}
		require.True(t, found, "textual equality must not rewrite UNION to ordinary DISTINCT")
	}
}

func TestCorrelatedUnionExportsIdentity(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"exists", `SELECT 2 FROM NATION o WHERE EXISTS
			((SELECT 1 FROM NATION i WHERE i.N_NATIONKEY=o.N_NATIONKEY)
			UNION (SELECT 1 FROM NATION i WHERE i.N_NATIONKEY=o.N_NATIONKEY))`},
		{"membership", `SELECT o.N_NATIONKEY IN
			(SELECT i.N_NATIONKEY FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY
			UNION SELECT i.N_NATIONKEY FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY) FROM NATION o`},
		{"aggregate", `SELECT o.N_NATIONKEY IN
			(SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY GROUP BY i.N_NAME
			UNION SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY GROUP BY i.N_NAME) FROM NATION o`},
		{"float-output", `SELECT o.N_NATIONKEY FROM NATION o WHERE EXISTS
			(SELECT CAST(-0e0 AS DOUBLE) FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY
			UNION SELECT CAST(0e0 AS DOUBLE) FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY)`},
		{"different-arms", `SELECT 1 FROM NATION o WHERE EXISTS
			(SELECT 1 FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY
			UNION SELECT 2 FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.query)
			require.NoError(t, err)
			q := p.GetQuery()
			found := false
			for _, n := range q.Nodes {
				if n.NodeType != planpb.Node_UNION_ALL {
					continue
				}
				found = true
				require.Len(t, n.ProjectList, 1, "unused existential payload is pruned; identity must remain")
				for _, child := range n.Children {
					require.Len(t, q.Nodes[child].ProjectList, len(n.ProjectList))
				}
				require.Empty(t, n.PhysicalEqualityKeyList, "existence must retain all candidates")
			}
			require.Equal(t, tc.name == "float-output" || tc.name == "different-arms", found)
			assertReachablePlanHasNoCorrelatedExpr(t, q)
		})
	}
}

func TestCorrelatedUnionRejectsMismatchedIdentity(t *testing.T) {
	for _, query := range []string{
		`SELECT 1 FROM NATION o WHERE EXISTS (SELECT 1 FROM NATION i WHERE i.N_NATIONKEY=o.N_NATIONKEY UNION SELECT 1 FROM NATION i)`,
		`SELECT 1 FROM NATION o WHERE EXISTS (SELECT 1 FROM NATION i WHERE i.N_NATIONKEY=o.N_NATIONKEY UNION SELECT 1 FROM NATION i WHERE i.N_NATIONKEY=o.N_REGIONKEY)`,
	} {
		_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
		require.ErrorContains(t, err, "correlated UNION", query)
	}
}

func TestIdenticalNonFloatUnionRetainsSafePath(t *testing.T) {
	for _, predicate := range []string{
		"i.N_NATIONKEY=o.N_NATIONKEY AND i.N_REGIONKEY=o.N_REGIONKEY",
		"i.N_NATIONKEY>o.N_NATIONKEY",
	} {
		query := "SELECT 1 FROM NATION o WHERE EXISTS (SELECT 1 FROM NATION i WHERE " + predicate +
			" UNION SELECT 1 FROM NATION i WHERE " + predicate + ")"
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
		require.NoError(t, err, query)
		for _, n := range p.GetQuery().Nodes {
			require.NotEqual(t, planpb.Node_UNION, n.NodeType)
		}
		assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
	}
}

func TestIdenticalVolatileUnionKeepsBothArms(t *testing.T) {
	for _, query := range []string{
		"select cast(rand() as int) union select cast(rand() as int)",
		"select 1 where rand()>0.5 union select 1 where rand()>0.5",
		"(select rand() limit 1) union (select rand() limit 1)",
		"select max(rand()) from NATION union select max(rand()) from NATION",
	} {
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
		require.NoError(t, err)
		found := false
		for _, n := range p.GetQuery().Nodes {
			found = found || n.NodeType == planpb.Node_UNION
		}
		require.True(t, found, "volatile evaluation counts must be preserved")
	}
}

func TestPreparedUnionKeepsScalarFloatIdentity(t *testing.T) {
	for _, oid := range []types.T{types.T_float32, types.T_float64} {
		t.Run(oid.String(), func(t *testing.T) {
			outputType := planpb.Type{Id: int32(oid)}
			node := &planpb.Node{
				NodeType: planpb.Node_UNION,
				ProjectList: []*planpb.Expr{
					GetColExpr(outputType, 0, 0), GetColExpr(outputType, 0, 1),
				},
				PhysicalEqualityKeyList: []*planpb.Expr{
					GetColExpr(planpb.Type{Id: int32(types.T_any)}, 0, 0),
					GetColExpr(outputType, 0, 1),
				},
			}
			for range 2 {
				require.NoError(t, refreshPreparedSetOperationPhysicalKeys(t.Context(), node, nil))
				features, err := planpb.RequiredRemoteExpressionFeatures(node)
				require.NoError(t, err)
				require.Equal(t, oid == types.T_float32 || oid == types.T_float64, features.SetOperationFloatKeys)
				require.Len(t, node.PhysicalEqualityKeyList, 2)
				require.Equal(t, "set_operation_float_key", node.PhysicalEqualityKeyList[0].GetF().Func.ObjName,
					"visible ANY specializing to float must retain signed-zero set membership")
				require.Equal(t, node.ProjectList[1].GetCol(), node.PhysicalEqualityKeyList[1].GetCol(),
					"hidden identity must keep scalar float equality across refreshes")
				require.NotSame(t, node.ProjectList[1], node.PhysicalEqualityKeyList[1])
			}
			node.PhysicalEqualityKeyList[0] = GetColExpr(outputType, 0, 1)
			require.NoError(t, refreshPreparedSetOperationPhysicalKeys(t.Context(), node, nil))
			features, err := planpb.RequiredRemoteExpressionFeatures(node)
			require.NoError(t, err)
			require.Equal(t, oid == types.T_float32 || oid == types.T_float64, features.SetOperationFloatKeys)
			require.NotNil(t, node.PhysicalEqualityKeyList[0].GetF(), "a different slot is not an identity marker")
		})
	}
}

func TestRetainedUnionCorrelationShapes(t *testing.T) {
	for _, predicate := range []string{
		"i.N_NATIONKEY=o.N_NATIONKEY AND i.N_REGIONKEY=o.N_REGIONKEY",
		"i.N_NATIONKEY>o.N_NATIONKEY",
	} {
		query := "SELECT 1 FROM NATION o WHERE EXISTS (SELECT CAST(i.N_REGIONKEY AS DOUBLE) FROM NATION i WHERE " + predicate +
			" UNION SELECT CAST(i.N_REGIONKEY AS DOUBLE) FROM NATION i WHERE " + predicate + ")"
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
		require.NoError(t, err, query)
		assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
		found := false
		for _, n := range p.GetQuery().Nodes {
			if n.NodeType != planpb.Node_UNION_ALL {
				continue
			}
			found = true
			if predicate == "i.N_NATIONKEY>o.N_NATIONKEY" {
				require.Len(t, n.ProjectList, 1)
			} else {
				require.Len(t, n.ProjectList, 2)
			}
			require.Empty(t, n.PhysicalEqualityKeyList)
			for _, child := range n.Children {
				require.Len(t, p.GetQuery().Nodes[child].ProjectList, len(n.ProjectList))
			}
		}
		require.True(t, found)
	}
}

func TestKeyedSetPushesUnchangedIntegerPredicate(t *testing.T) {
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"SELECT k,d FROM (SELECT N_NATIONKEY k,CAST(N_REGIONKEY AS DOUBLE) d FROM NATION UNION SELECT N_NATIONKEY k,CAST(N_REGIONKEY AS DOUBLE) d FROM NATION) u WHERE k=1")
	require.NoError(t, err)
	scans := 0
	for _, n := range p.GetQuery().Nodes {
		if n.NodeType == planpb.Node_TABLE_SCAN {
			scans++
			require.NotEmpty(t, n.FilterList, "integer predicate must enter each branch scan")
		}
	}
	require.Equal(t, 2, scans)
}

func TestKeyedSetKeepsUnsafePredicateAbove(t *testing.T) {
	for _, predicate := range []string{"d>0", "k=1 AND d>0", "k=1 AND rand()>0.5"} {
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
			"SELECT k,d FROM (SELECT N_NATIONKEY k,CAST(N_REGIONKEY AS DOUBLE) d FROM NATION UNION SELECT N_NATIONKEY k,CAST(N_REGIONKEY AS DOUBLE) d FROM NATION) u WHERE "+predicate)
		require.NoError(t, err)
		q := p.GetQuery()
		scans := 0
		residual := false
		for _, n := range q.Nodes {
			if n.NodeType == planpb.Node_TABLE_SCAN {
				scans++
				if predicate == "d>0" {
					require.Empty(t, n.FilterList)
				} else {
					require.Len(t, n.FilterList, 1)
				}
				for _, filter := range n.FilterList {
					require.False(t, ContainsVolatileFunction(filter))
				}
			}
			if n.NodeType == planpb.Node_FILTER && len(n.FilterList) > 0 {
				residual = true
			}
		}
		require.Equal(t, 2, scans)
		require.True(t, residual, "unsafe conjunct must remain above the set operation")
	}
}

func TestSetKeyInvarianceRejectsInvalidSlots(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_int32)}
	output := GetColExpr(typ, 1, 0)
	node := &planpb.Node{BindingTags: []int32{2}, ProjectList: []*planpb.Expr{output}, PhysicalEqualityKeyList: []*planpb.Expr{DeepCopyExpr(output)}}
	filter := GetColExpr(typ, 2, 0)
	require.True(t, setFilterPreservesEquality(filter, node))
	for _, invalid := range []*planpb.Expr{GetColExpr(typ, 3, 0), GetColExpr(typ, 2, -1), GetColExpr(typ, 2, 1)} {
		require.False(t, setFilterPreservesEquality(invalid, node))
	}
	node.PhysicalEqualityKeyList[0] = GetColExpr(typ, 1, 1)
	require.False(t, setFilterPreservesEquality(filter, node), "a key for another slot is not equality proof")
	node.PhysicalEqualityKeyList = nil
	require.False(t, setFilterPreservesEquality(filter, node), "a missing key is not equality proof")
}

func TestExistentialUnionPredicateTemplates(t *testing.T) {
	for _, predicate := range []string{
		"i.N_NATIONKEY=o.N_NATIONKEY OR i.N_REGIONKEY=o.N_REGIONKEY",
		"i.N_NATIONKEY=o.N_NATIONKEY AND o.N_REGIONKEY>0",
		"o.N_REGIONKEY>0",
		"i.N_NATIONKEY<=>o.N_NATIONKEY",
		"CAST(i.N_REGIONKEY AS DOUBLE)>CAST(o.N_REGIONKEY AS DOUBLE)",
	} {
		arm := "SELECT CAST(i.N_REGIONKEY AS DOUBLE) FROM NATION i WHERE " + predicate
		query := "SELECT 1 FROM NATION o WHERE EXISTS (" + arm + " UNION " + arm + ")"
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
		require.NoError(t, err, query)
		assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
		found := false
		for _, node := range p.GetQuery().Nodes {
			require.NotEqual(t, planpb.Node_UNION, node.NodeType, "existential UNION island must retain every candidate")
			if node.NodeType == planpb.Node_UNION_ALL {
				found = true
				require.Empty(t, node.PhysicalEqualityKeyList)
				for _, child := range node.Children {
					require.Len(t, p.GetQuery().Nodes[child].ProjectList, len(node.ProjectList))
				}
			}
		}
		require.True(t, found)

	}
}

func TestExistentialUnionStopsAtObservableConsumers(t *testing.T) {
	for _, tc := range []struct {
		name             string
		kind             planpb.Node_NodeType
		limited, demoted bool
	}{
		{"project", planpb.Node_PROJECT, false, true},
		{"sort", planpb.Node_SORT, false, true},
		{"aggregate", planpb.Node_AGG, false, false},
		{"filter", planpb.Node_FILTER, false, false},
		{"window", planpb.Node_WINDOW, false, false},
		{"pagination", planpb.Node_PROJECT, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := NewMockCompilerContext(true, newPlanTestProcess(t))
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, false)
			typ := planpb.Type{Id: int32(types.T_float64)}
			set := &planpb.Node{NodeType: planpb.Node_UNION, Children: []int32{1, 3}, BindingTags: []int32{30}, ProjectList: []*planpb.Expr{GetColExpr(typ, 10, 0)}, PhysicalEqualityKeyList: []*planpb.Expr{GetColExpr(typ, 10, 0)}}
			root := &planpb.Node{NodeType: tc.kind, Children: []int32{4}, BindingTags: []int32{40}, ProjectList: []*planpb.Expr{GetColExpr(typ, 30, 0)}}
			if tc.limited {
				root.Limit = makePlan2Uint64ConstExprWithType(1)
			}
			if tc.kind == planpb.Node_FILTER {
				root.FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
			}
			builder.qry.Nodes = []*planpb.Node{
				{NodeType: planpb.Node_VALUE_SCAN, BindingTags: []int32{1}, ProjectList: []*planpb.Expr{GetColExpr(typ, 1, 0)}},
				{NodeType: planpb.Node_PROJECT, Children: []int32{0}, BindingTags: []int32{10}, ProjectList: []*planpb.Expr{GetColExpr(typ, 1, 0)}},
				{NodeType: planpb.Node_VALUE_SCAN, BindingTags: []int32{2}, ProjectList: []*planpb.Expr{GetColExpr(typ, 2, 0)}},
				{NodeType: planpb.Node_PROJECT, Children: []int32{2}, BindingTags: []int32{20}, ProjectList: []*planpb.Expr{GetColExpr(typ, 2, 0)}},
				set, root,
			}
			crossed := false
			_, _, err := builder.pullupCorrelatedPredicates(5, NewBindContext(builder, nil), planpb.SubqueryRef_EXISTS, true, true, &crossed)
			require.NoError(t, err)
			if tc.demoted {
				require.Equal(t, planpb.Node_UNION_ALL, set.NodeType)
				require.Empty(t, set.PhysicalEqualityKeyList)
			} else {
				require.Equal(t, planpb.Node_UNION, set.NodeType)
				require.Len(t, set.PhysicalEqualityKeyList, 1)
			}
		})
	}
}

func TestExistentialUnionKeepsBranchLocalVolatileProducers(t *testing.T) {
	arm := "SELECT CAST(-0e0 AS DOUBLE) FROM NATION i WHERE i.N_NATIONKEY+rand()>o.N_NATIONKEY"
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "SELECT 1 FROM NATION o WHERE EXISTS ("+arm+" UNION "+arm+")")
	require.NoError(t, err)
	visited := make(map[int32]bool)
	randomCalls := 0
	var visit func(int32)
	visit = func(id int32) {
		if visited[id] {
			return
		}
		visited[id] = true
		node := p.GetQuery().Nodes[id]
		for _, exprs := range [][]*planpb.Expr{node.ProjectList, node.FilterList, node.OnList} {
			for _, expr := range exprs {
				require.NoError(t, planpb.VisitExprTree(expr, func(e *planpb.Expr) error {
					if f := e.GetF(); f != nil && f.Func.ObjName == "rand" {
						randomCalls++
					}
					return nil
				}))
			}
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	for _, step := range p.GetQuery().Steps {
		visit(step)
	}
	require.Equal(t, 2, randomCalls, "independent branch-local evaluations must remain; no canonical deduplication or template sharing")

}

func TestScalarSetReaggregationAdmission(t *testing.T) {
	for _, kind := range []string{"identity", "computed", "dropped-member", "limit", "volatile"} {
		t.Run(kind, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, false)
			typ := planpb.Type{Id: int32(types.T_float64)}
			corr := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{Depth: 1, RelPos: 1, ColPos: 0}}}
			pred, err := BindFuncExprImplByPlanExpr(t.Context(), ">", []*planpb.Expr{GetColExpr(typ, 10, 0), corr})
			require.NoError(t, err)
			set := &planpb.Node{NodeType: planpb.Node_UNION, Children: []int32{0, 0}, BindingTags: []int32{20}, ProjectList: []*planpb.Expr{GetColExpr(typ, 10, 0)}}
			root := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{1}, BindingTags: []int32{30}, ProjectList: []*planpb.Expr{GetColExpr(typ, 20, 0)}}
			switch kind {
			case "computed":
				root.ProjectList[0] = makePlan2Int64ConstExprWithType(1)
			case "dropped-member":
				set.ProjectList = append(set.ProjectList, GetColExpr(typ, 10, 1))
			case "volatile":
				set.ProjectList[0], err = BindFuncExprImplByPlanExpr(t.Context(), "rand", nil)
				require.NoError(t, err)
			case "limit":
				root.Limit = makePlan2Uint64ConstExprWithType(1)
			}
			builder.qry.Nodes = []*planpb.Node{{NodeType: planpb.Node_FILTER, FilterList: []*planpb.Expr{pred}}, set, root}
			require.Equal(t, kind == "identity", builder.scalarSetNeedsReaggregation(2))
		})
	}
}

func TestScalarSetRetainsCardinalityOwner(t *testing.T) {
	for _, tc := range []struct{ name, source, key string }{
		{"table", "NATION", "N_NATIONKEY"},
		{"function", "generate_series(1,3)", "result"},
		{"function-alias", "(select result k from generate_series(1,3) g)", "k"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arm := "select cast(i.N_NATIONKEY as double) from NATION i where i.N_NATIONKEY>o." + tc.key
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select o."+tc.key+",("+arm+" union "+arm+") from "+tc.source+" o")
			require.NoError(t, err)
			single, memberGroups, outerGroups := false, false, false
			marked := 0
			identities := 0
			seen := make(map[int32]bool)
			var visit func(int32)
			visit = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				node := p.GetQuery().Nodes[id]
				if node.ScalarSetReaggregation {
					marked++
				}
				if node.NodeType == planpb.Node_WINDOW {
					for _, expr := range node.WinSpecList {
						window := expr.GetW()
						if window.WindowFunc.GetF().Func.ObjName == "row_number" {
							require.Empty(t, window.PartitionBy)
							require.Empty(t, window.OrderBy)
							identities++
						}
					}
				}
				if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_SINGLE {
					single = true
					require.Len(t, node.OnList, 1)
					require.Equal(t, "or", node.OnList[0].GetF().Func.ObjName)
				}
				if node.NodeType == planpb.Node_AGG {
					memberGroups = memberGroups || len(node.GroupByHashKey) == 3
					outerGroups = outerGroups || len(node.GroupByHashKey) == 1
				}
				for _, child := range node.Children {
					visit(child)
				}
			}
			for _, step := range p.GetQuery().Steps {
				visit(step)
			}
			require.True(t, single, "the final optimizer must preserve scalar cardinality checking")
			require.True(t, memberGroups, "membership must retain outer identity, marker and visible set key")
			require.True(t, outerGroups, "final scalar aggregation must partition by outer identity")
			require.Equal(t, 1, identities, "outer identity must survive optimization as a global WINDOW")
			require.Equal(t, 2, marked, "identity and candidate join retain bounded local fallback markers")
			floor, err := RequiredPersistedExpressionProtocolVersion(p.GetQuery())
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion123, floor)
		})
	}
}

func TestScalarSetCompositionRetainsOuterProducer(t *testing.T) {
	arm := "select cast(i.N_NATIONKEY as double) from NATION i where i.N_NATIONKEY>o.N_NATIONKEY"
	scalar := "(" + arm + " union " + arm + ")"
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"select o.r,"+scalar+","+scalar+" from (select N_NATIONKEY,rand() as r from NATION) o")
	require.NoError(t, err)
	identities, producers, cardinalityChecks := 0, 0, 0
	var visit func(int32)
	visit = func(id int32) {
		node := p.GetQuery().Nodes[id]
		if node.NodeType == planpb.Node_WINDOW {
			for _, expr := range node.WinSpecList {
				window := expr.GetW()
				if window.WindowFunc.GetF().Func.ObjName == "row_number" {
					require.Empty(t, window.PartitionBy)
					identities++
				}
			}
		}
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_SINGLE {
			cardinalityChecks++
		}
		for _, expr := range node.ProjectList {
			require.NoError(t, planpb.VisitExprTree(expr, func(e *planpb.Expr) error {
				if f := e.GetF(); f != nil && f.Func.ObjName == "rand" {
					producers++
				}
				return nil
			}))
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	for _, step := range p.GetQuery().Steps {
		visit(step)
	}
	require.Equal(t, 2, identities)
	require.Equal(t, 2, cardinalityChecks)
	require.Equal(t, 1, producers, "each scalar must consume the same outer producer without duplicating volatile evaluation")
}

func TestSetOperationFloatKeyPersistedAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "set_operation_float_key", []*planpb.Expr{GetColExpr(types.T_float64.ToType().PlanType(), 0, 0)})
	require.NoError(t, err)
	owner := &planpb.Query{Nodes: []*planpb.Node{{PhysicalEqualityKeyList: []*planpb.Expr{expr}}}}
	floor, err := RequiredPersistedExpressionProtocolVersion(owner)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion123, floor)
	rt := moruntime.ServiceRuntime(proc.GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		original, present := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if present {
				rt.SetGlobalVariables(key, original)
			} else if current, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, current)
			}
		})
	}
	for _, version := range []int64{defines.MORPCVersion122, defines.MORPCVersion123} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion123)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, version)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, version)
		readErr := RequirePersistedExpressionProtocol(proc.Ctx, proc, owner)
		writeErr := RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner)
		if version == defines.MORPCVersion122 {
			require.ErrorContains(t, readErr, "123")
			require.ErrorContains(t, writeErr, "123")
		} else {
			require.NoError(t, readErr)
			require.NoError(t, writeErr)
		}
	}
}

func TestSetArmSingletonBoundary(t *testing.T) {
	for _, tc := range []struct {
		name  string
		nodes []*planpb.Node
		want  bool
	}{
		{"ungrouped aggregate", []*planpb.Node{{NodeType: planpb.Node_AGG, AggList: []*planpb.Expr{MakePlan2Int64ConstExprWithType(1)}}}, true},
		{"grouped aggregate", []*planpb.Node{{NodeType: planpb.Node_AGG, GroupBy: []*planpb.Expr{MakePlan2Int64ConstExprWithType(1)}}}, false},
		{"transparent project", []*planpb.Node{{NodeType: planpb.Node_PROJECT, Children: []int32{1}}, {NodeType: planpb.Node_AGG, AggList: []*planpb.Expr{MakePlan2Int64ConstExprWithType(1)}}}, true},
		{"one row limit", []*planpb.Node{{NodeType: planpb.Node_PROJECT, Limit: MakePlan2Uint64ConstExprWithType(1)}}, true},
		{"zero limit", []*planpb.Node{{NodeType: planpb.Node_PROJECT, Limit: MakePlan2Uint64ConstExprWithType(0)}}, true},
		{"multirow limit", []*planpb.Node{{NodeType: planpb.Node_PROJECT, Limit: MakePlan2Uint64ConstExprWithType(2)}}, false},
		{"parameter limit", []*planpb.Node{{NodeType: planpb.Node_PROJECT, Limit: &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}}}, false},
		{"join can multiply singleton", []*planpb.Node{{NodeType: planpb.Node_JOIN, Children: []int32{1, 1}}, {NodeType: planpb.Node_PROJECT, Limit: MakePlan2Uint64ConstExprWithType(1)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := &QueryBuilder{qry: &planpb.Query{Nodes: tc.nodes}}
			require.Equal(t, tc.want, builder.setArmHasSingletonBoundary(0))
		})
	}
}

func TestPreparedSingletonUnionKeepsIndependentArms(t *testing.T) {
	for _, sql := range []string{
		"prepare p from (select cast(? as double) limit 1) union (select cast(? as double) limit 1)",
		"prepare p from (select cast(? as double) limit 2) union (select cast(? as double) limit 2)",
		"prepare p from select max(cast(? as double)) from NATION group by N_REGIONKEY union select max(cast(? as double)) from NATION group by N_REGIONKEY",
		"prepare p from select max(cast(? as double)) from NATION union select max(cast(? as double)) from NATION",
	} {
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
		require.NoError(t, err)
		q := p.GetDcl().GetPrepare().GetPlan().GetQuery()
		found := false
		for _, node := range q.Nodes {
			if node.NodeType == planpb.Node_UNION {
				found = true
				require.Len(t, node.Children, 2)
			}
		}
		require.True(t, found, "identical syntax has independent parameter slots")
	}
}

func TestIdenticalBoundaryUnionUsesPartitionedSetKeys(t *testing.T) {
	for _, arm := range []string{
		"select cast(i.N_NATIONKEY as double) from NATION i where cast(i.N_REGIONKEY as double)=cast(o.N_REGIONKEY as double) order by i.N_NATIONKEY limit 2 offset 1",
		"select max(cast(i.N_NATIONKEY as double)) from NATION i where cast(i.N_REGIONKEY as double)=cast(o.N_REGIONKEY as double) group by i.N_NAME",
	} {
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
			"select (("+arm+") union ("+arm+")) from NATION o")
		require.NoError(t, err)
		foundKeys, foundSingle, foundPagination := false, false, false
		for _, node := range p.GetQuery().Nodes {
			foundSingle = foundSingle || node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_SINGLE
			foundPagination = foundPagination || node.NodeType == planpb.Node_WINDOW
			if node.NodeType != planpb.Node_AGG || len(node.GroupByHashKey) == 0 {
				continue
			}
			key := node.GroupBy[node.GroupByHashKey[0]]
			if key.GetF() == nil || key.GetF().Func.ObjName != "set_operation_float_key" {
				continue
			}
			foundKeys = true
			require.Len(t, node.GroupByHashKey, 2, "visible membership plus correlation partition")
			identity := node.GroupBy[node.GroupByHashKey[1]]
			require.Equal(t, int32(types.T_float64), identity.Typ.Id)
			require.NotNil(t, identity.GetCol(), "hidden float identity retains scalar equality")
		}
		require.True(t, foundKeys, "deduplication uses set equality")
		require.True(t, foundSingle, "multiple distinct members retain scalar cardinality checking")
		if strings.Contains(arm, "limit") {
			require.True(t, foundPagination, "per-partition pagination survives DISTINCT lowering")
		}
	}
}

func TestVolatileBoundaryUnionKeepsIndependentArms(t *testing.T) {
	for _, arm := range []string{
		"select rand() from NATION limit 2",
		"select max(rand()) from NATION group by N_REGIONKEY",
	} {
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "("+arm+") union ("+arm+")")
		require.NoError(t, err)
		found := false
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType == planpb.Node_UNION {
				found = true
				require.Len(t, node.Children, 2)
			}
		}
		require.True(t, found, "volatile evaluations cannot be reused")
	}
}

func TestScalarFunctionOuterOutputBoundary(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, false)
	integer, floating := types.T_int64.ToType().PlanType(), types.T_float64.ToType().PlanType()
	builder.qry.Nodes = []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10}, TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Typ: integer}}}},
		{NodeType: planpb.Node_FUNCTION_SCAN, BindingTags: []int32{20}, Children: []int32{0}, TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Typ: integer}, {Typ: floating}}}},
		{NodeType: planpb.Node_PROJECT, BindingTags: []int32{30}, Children: []int32{1}, ProjectList: []*planpb.Expr{GetColExpr(floating, 20, 1)}},
	}
	ctx := &BindContext{}
	outputs, ok := builder.collectScalarOuterOutputs(1, -1, 0, ctx)
	require.True(t, ok)
	require.Equal(t, []scalarOuterOutput{{ref: [2]int32{20, 0}, typ: integer}, {ref: [2]int32{20, 1}, typ: floating}}, outputs)
	outputs, ok = builder.collectScalarOuterOutputs(2, -1, 0, ctx)
	require.True(t, ok)
	require.Equal(t, []scalarOuterOutput{{ref: [2]int32{30, 0}, typ: floating, aliases: [][2]int32{{20, 1}}}}, outputs)
	builder.qry.Nodes[1].TableDef = nil
	_, ok = builder.collectScalarOuterOutputs(1, -1, 0, ctx)
	require.False(t, ok, "missing function output schema must fail closed")
	require.Empty(t, builder.scalarProjectionAliases(1, GetColExpr(floating, 20, 1)))
}
