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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// newCorrelatedSpecialTypeMock builds the #31568 fixture shape: ENUM/SET
// driven tables with reversed member definitions, so a correlated key
// comparison crosses two different member orderings.
func newCorrelatedSpecialTypeMock(t testing.TB) *MockOptimizer {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	mock.ctxt.objects["enum_l"] = &planpb.ObjectRef{Obj: 9101, ObjName: "enum_l"}
	mock.ctxt.tables["enum_l"] = &planpb.TableDef{
		TblId: 9101,
		Name:  "enum_l",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "k", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}},
		},
	}
	mock.ctxt.objects["enum_r"] = &planpb.ObjectRef{Obj: 9102, ObjName: "enum_r"}
	mock.ctxt.tables["enum_r"] = &planpb.TableDef{
		TblId: 9102,
		Name:  "enum_r",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "k", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "b,a"}},
		},
	}
	mock.ctxt.objects["set_l"] = &planpb.ObjectRef{Obj: 9103, ObjName: "set_l"}
	mock.ctxt.tables["set_l"] = &planpb.TableDef{
		TblId: 9103,
		Name:  "set_l",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "k", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b"}},
		},
	}
	mock.ctxt.objects["set_r"] = &planpb.ObjectRef{Obj: 9104, ObjName: "set_r"}
	mock.ctxt.tables["set_r"] = &planpb.TableDef{
		TblId: 9104,
		Name:  "set_r",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "k", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: "b,a"}},
		},
	}
	mock.ctxt.objects["int_l"] = &planpb.ObjectRef{Obj: 9105, ObjName: "int_l"}
	mock.ctxt.tables["int_l"] = &planpb.TableDef{
		TblId: 9105,
		Name:  "int_l",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
	}
	mock.ctxt.objects["int_r"] = &planpb.ObjectRef{Obj: 9106, ObjName: "int_r"}
	mock.ctxt.tables["int_r"] = &planpb.TableDef{
		TblId: 9106,
		Name:  "int_r",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
	}
	return mock
}

// TestCorrelatedExistsOnEnumSetKeysPlans must ensure correlated EXISTS and
// NOT EXISTS whose association key is an ENUM/SET column plan successfully.
// Before #31568's repair the display wrapper dropped the correlation depth
// and the plan build failed with a column-remapping error.
func TestCorrelatedExistsOnEnumSetKeysPlans(t *testing.T) {
	failing := []string{
		"select l.id from enum_l l where not exists(select 1 from enum_r r where l.k=r.k)",
		"select l.id from enum_l l where exists(select 1 from enum_r r where l.k=r.k)",
		"select l.id from enum_l l where not exists(select 1 from enum_r r where cast(l.k as char)=cast(r.k as char))",
		"select l.id from set_l l where not exists(select 1 from set_r r where l.k=r.k)",
		"select l.id from set_l l where exists(select 1 from set_r r where l.k=r.k)",
		"select l.id from enum_l l where l.k not in (select r.k from enum_r r)",
		"select l.id from enum_l l where l.k in (select r.k from enum_r r)",
	}
	controls := []string{
		// Integer-key correlation must keep planning unchanged.
		"select l.id from int_l l where not exists(select 1 from int_r r where l.id=r.id)",
		// Non-correlated ENUM/SET subqueries keep planning.
		"select (select count(*) from enum_r) from enum_l l",
		"select l.id, (select count(*) from enum_r r where r.k=l.id) from enum_l l",
	}
	for _, sql := range append(append([]string{}, failing...), controls...) {
		t.Run(sql, func(t *testing.T) {
			p, err := runOneStmt(newCorrelatedSpecialTypeMock(t), t, sql)
			require.NoError(t, err)
			require.NotNil(t, p)
		})
	}
}

// TestCorrelatedEnumSetWrapperKeepsOuterReference asserts the flattened plan
// correlates both scopes: the join condition must wrap the outer and inner
// ENUM columns in display functions with their respective member definitions,
// each through a remapped column reference, instead of leaving an unresolved
// outer column inside the subquery scope.
func TestCorrelatedEnumSetWrapperKeepsOuterReference(t *testing.T) {
	p, err := runOneStmt(newCorrelatedSpecialTypeMock(t), t,
		"select l.id from enum_l l where exists(select 1 from enum_r r where l.k=r.k)")
	require.NoError(t, err)

	join := false
	var outerPos, innerPos int32 = -1, -1
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == planpb.Node_JOIN {
			join = true
		}
		for _, expr := range append(append(append([]*planpb.Expr{}, node.ProjectList...), node.FilterList...), node.OnList...) {
			for _, wrap := range planFunctionExprs(expr, moEnumCastIndexToValueFun) {
				args := wrap.GetF().GetArgs()
				if len(args) < 2 {
					continue
				}
				col := args[1].GetCol()
				if col == nil {
					continue
				}
				// After remapping both keys must be plain column refs into
				// their own relations; the #31568 failure mode leaves the
				// outer key unresolvable and never gets this far.
				switch args[0].GetLit().GetSval() {
				case "a,b":
					outerPos = col.ColPos
				case "b,a":
					innerPos = col.ColPos
				}
			}
		}
	}
	require.True(t, join, "the correlated EXISTS must flatten into a join on the ENUM keys")
	require.True(t, outerPos >= 0, "the outer ENUM key must stay wrapped with its member definition")
	require.True(t, innerPos >= 0, "the inner ENUM key must stay wrapped with its member definition")
}

// planFunctionExprs collects every function expression with the given name
// reachable from root, including through enclosing function calls.
func planFunctionExprs(root *planpb.Expr, name string) []*planpb.Expr {
	var found []*planpb.Expr
	var walk func(e *planpb.Expr)
	walk = func(e *planpb.Expr) {
		if e == nil {
			return
		}
		if fn := e.GetF(); fn != nil {
			if fn.Func.GetObjName() == name {
				found = append(found, e)
			}
			for _, arg := range fn.Args {
				walk(arg)
			}
		}
	}
	walk(root)
	return found
}
