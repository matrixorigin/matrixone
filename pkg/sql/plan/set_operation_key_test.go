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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
			for _, oid := range []types.T{types.T_float64, types.T_int64, types.T_float32, types.T_varchar, types.T_float64} {
				node.ProjectList = []*planpb.Expr{GetColExpr(planpb.Type{Id: int32(oid), Scale: 2}, 0, 0)}
				require.NoError(t, refreshPreparedSetOperationPhysicalKeys(t.Context(), node, nil))
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
				if n.NodeType != planpb.Node_UNION {
					continue
				}
				found = true
				require.Len(t, n.ProjectList, 2)
				for _, child := range n.Children {
					require.Len(t, q.Nodes[child].ProjectList, 2)
				}
				if tc.name == "float-output" {
					require.Len(t, n.PhysicalEqualityKeyList, 2)
					require.Equal(t, "set_operation_float_key", n.PhysicalEqualityKeyList[0].GetF().Func.ObjName)
					identity := n.PhysicalEqualityKeyList[1].GetCol()
					require.NotNil(t, identity)
					require.Equal(t, n.ProjectList[1].GetCol().RelPos, identity.RelPos)
					require.Equal(t, n.ProjectList[1].GetCol().ColPos, identity.ColPos)
				}
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
		`SELECT 1 FROM NATION o WHERE EXISTS (SELECT 1 FROM NATION i WHERE i.N_NATIONKEY>o.N_NATIONKEY UNION SELECT 2 FROM NATION i WHERE i.N_NATIONKEY>o.N_NATIONKEY)`,
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
				require.Len(t, node.PhysicalEqualityKeyList, 2)
				require.Equal(t, "set_operation_float_key", node.PhysicalEqualityKeyList[0].GetF().Func.ObjName,
					"visible ANY specializing to float must retain signed-zero set membership")
				require.Equal(t, node.ProjectList[1].GetCol(), node.PhysicalEqualityKeyList[1].GetCol(),
					"hidden identity must keep scalar float equality across refreshes")
				require.NotSame(t, node.ProjectList[1], node.PhysicalEqualityKeyList[1])
			}
			node.PhysicalEqualityKeyList[0] = GetColExpr(outputType, 0, 1)
			require.NoError(t, refreshPreparedSetOperationPhysicalKeys(t.Context(), node, nil))
			require.NotNil(t, node.PhysicalEqualityKeyList[0].GetF(), "a different slot is not an identity marker")
		})
	}
}
