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
	// Two-column INT controls mirror the (id, k) shape of the ENUM/SET
	// fixtures so ONLY_FULL_GROUP_BY decisions can be compared side by side.
	mock.ctxt.objects["int2_l"] = &planpb.ObjectRef{Obj: 9107, ObjName: "int2_l"}
	mock.ctxt.tables["int2_l"] = &planpb.TableDef{
		TblId: 9107,
		Name:  "int2_l",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "k", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
	}
	mock.ctxt.objects["int2_r"] = &planpb.ObjectRef{Obj: 9108, ObjName: "int2_r"}
	mock.ctxt.tables["int2_r"] = &planpb.TableDef{
		TblId: 9108,
		Name:  "int2_r",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "k", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
	}
	// Primary-key variants: grouping the primary key makes the ungrouped key
	// column functionally dependent, which MySQL's ONLY_FULL_GROUP_BY accepts.
	addPK := func(name string, tblId uint64, k *planpb.Type) {
		mock.ctxt.objects[name] = &planpb.ObjectRef{Obj: int64(9100 + tblId), ObjName: name}
		mock.ctxt.tables[name] = &planpb.TableDef{
			TblId: 9100 + tblId,
			Name:  name,
			Pkey:  &planpb.PrimaryKeyDef{PkeyColName: "id"},
			Cols: []*planpb.ColDef{
				{Name: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Name: "k", Typ: *k},
			},
		}
	}
	addPK("enump_l", 11, &planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"})
	addPK("enump_r", 12, &planpb.Type{Id: int32(types.T_enum), Enumvalues: "b,a"})
	addPK("setp_l", 13, &planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b"})
	addPK("setp_r", 14, &planpb.Type{Id: int32(types.T_uint64), Enumvalues: "b,a"})
	addPK("intp_l", 15, &planpb.Type{Id: int32(types.T_int32)})
	addPK("intp_r", 16, &planpb.Type{Id: int32(types.T_int32)})
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

// TestCorrelatedGroupedHavingOnEnumSetKeysPlans extends the #31568 proof to
// ONLY_FULL_GROUP_BY grouped subqueries whose HAVING references the outer
// ENUM/SET key. The Corr reference then nests inside the display wrapper;
// until HavingBinder read that wrapped correlation, legal outer keys were
// rejected with a GROUP BY error while their INT-key equivalents planned.
func TestCorrelatedGroupedHavingOnEnumSetKeysPlans(t *testing.T) {
	failing := []string{
		// The inner block groups on its own key and references the outer
		// ENUM/SET key in HAVING.
		"select l.id from enum_l l where exists(select r.k from enum_r r group by r.k having r.k=l.k)",
		"select l.id from enum_l l where not exists(select r.k from enum_r r group by r.k having r.k=l.k)",
		"select l.id from set_l l where exists(select r.k from set_r r group by r.k having r.k=l.k)",
		"select l.id from set_l l where not exists(select r.k from set_r r group by r.k having r.k=l.k)",
		// Grouped outer blocks: the outer primary key is grouped, so the
		// ungrouped outer key is functionally dependent and legal.
		"select l.id from enump_l l group by l.id having exists(select r.k from enump_r r group by r.k having r.k=l.k)",
		"select l.id from setp_l l group by l.id having exists(select r.k from setp_r r group by r.k having r.k=l.k)",
	}
	controls := []string{
		// INT-key correlation with the same shapes keeps planning.
		"select l.id from int_l l where exists(select r.id from int_r r group by r.id having r.id=l.id)",
		"select l.id from intp_l l group by l.id having exists(select r.k from intp_r r group by r.k having r.k=l.k)",
		// A grouped outer key resolves through the group splice, wrapper-free.
		"select l.k from enum_l l group by l.k having exists(select r.k from enum_r r group by r.k having r.k=l.k)",
		// A grouped local ENUM key in the subquery HAVING keeps validating.
		"select l.id from enum_l l where exists(select r.k from enum_r r group by r.k having r.k=r.k)",
	}
	for _, sql := range append(append([]string{}, failing...), controls...) {
		t.Run(sql, func(t *testing.T) {
			p, err := runOneStmt(newCorrelatedSpecialTypeMock(t), t, sql)
			require.NoError(t, err)
			require.NotNil(t, p)
		})
	}
}

// TestCorrelatedGroupedHavingMatchesIntKeyPolicy pins the decision contract:
// whatever ONLY_FULL_GROUP_BY decides for an outer INT key referenced in a
// grouped subquery's HAVING, the verdict must be identical when the key is
// ENUM/SET. The no-primary-key pairs keep the target-scope checks honest:
// with the referenced outer key neither grouped nor functionally dependent,
// both spellings must stay rejected.
func TestCorrelatedGroupedHavingMatchesIntKeyPolicy(t *testing.T) {
	pairs := []struct{ special, plain string }{
		{
			// Legal: the inner keys are grouped, the outer key is free.
			"select l.id from enum_l l where exists(select r.k from enum_r r group by r.k having r.k=l.k)",
			"select l.id from int2_l l where exists(select r.k from int2_r r group by r.k having r.k=l.k)",
		},
		{
			// Legal: grouping the outer primary key makes the outer key
			// functionally dependent.
			"select l.id from enump_l l group by l.id having exists(select r.k from enump_r r group by r.k having r.k=l.k)",
			"select l.id from intp_l l group by l.id having exists(select r.k from intp_r r group by r.k having r.k=l.k)",
		},
		{
			// Illegal: the outer block groups neither the referenced key nor
			// a primary key, so both spellings are rejected.
			"select l.id from enum_l l group by l.id having exists(select r.k from enum_r r group by r.k having r.k=l.k)",
			"select l.id from int2_l l group by l.id having exists(select r.k from int2_r r group by r.k having r.k=l.k)",
		},
		{
			// Illegal: the inner local key is not grouped; both spellings
			// are rejected.
			"select l.id from enum_l l where exists(select r.k from enum_r r group by r.id having r.k=l.k)",
			"select l.id from int2_l l where exists(select r.k from int2_r r group by r.id having r.k=l.k)",
		},
	}
	for _, pair := range pairs {
		specialErr := correlatedHavingErrFor(t, pair.special)
		plainErr := correlatedHavingErrFor(t, pair.plain)
		if plainErr == nil {
			require.NoErrorf(t, specialErr,
				"special-typed key rejected while the INT key plans: %q", pair.special)
		} else {
			require.Errorf(t, specialErr,
				"special-typed key accepted while the INT key is rejected: %q", pair.special)
		}
	}
}

func correlatedHavingErrFor(t *testing.T, sql string) error {
	t.Helper()
	_, err := runOneStmt(newCorrelatedSpecialTypeMock(t), t, sql)
	return err
}

// TestLooseGroupByHavingCorrelatedKeyRejected pins the relaxed-nothing
// sql_mode contract for #31568. Without ONLY_FULL_GROUP_BY a free HAVING
// column is folded into any_value; a correlated outer reference must never
// enter that aggregate list. Before the bind-time rejection the wrapped
// ENUM/SET correlation either leaked a raw correlated expression into the
// executed plan or died in column remapping, and it now fails binding with
// the same NYI an outer INT key gets in this mode.
func TestLooseGroupByHavingCorrelatedKeyRejected(t *testing.T) {
	correlated := []string{
		// Inner block groups its own key, HAVING references the outer key.
		"select l.id from enum_l l where exists(select r.k from enum_r r group by r.k having r.k=l.k)",
		"select l.id from enum_l l where not exists(select r.k from enum_r r group by r.k having r.k=l.k)",
		"select l.id from enum_l l where l.k in (select r.k from enum_r r group by r.k having r.k=l.k)",
		"select l.id from set_l l where exists(select r.k from set_r r group by r.k having r.k=l.k)",
		"select l.id from set_l l where not exists(select r.k from set_r r group by r.k having r.k=l.k)",
		// Outer block groups its primary key; the inner HAVING still carries
		// the correlation.
		"select l.id from enump_l l group by l.id having exists(select r.k from enump_r r group by r.k having r.k=l.k)",
		"select l.id from setp_l l group by l.id having exists(select r.k from setp_r r group by r.k having r.k=l.k)",
		// INT keys get the identical verdict in this mode: correlated keys
		// are rejected while the mode is relaxed, independent of type.
		"select l.id from int_l l where exists(select r.id from int_r r group by r.id having r.id=l.id)",
		"select l.id from intp_l l group by l.id having exists(select r.k from intp_r r group by r.k having r.k=l.k)",
	}
	for _, sql := range correlated {
		t.Run(sql, func(t *testing.T) {
			mock := newCorrelatedSpecialTypeMock(t)
			mock.ctxt.SetSqlModeOverride("")
			_, err := runOneStmt(mock, t, sql)
			require.ErrorContains(t, err, "correlated columns in aggregate function")
		})
	}

	// A free LOCAL ENUM key without any correlation keeps folding into
	// any_value and must keep planning: the rejection is scoped to the
	// correlation carried inside the display wrapper.
	t.Run("loose local free enum key still plans", func(t *testing.T) {
		mock := newCorrelatedSpecialTypeMock(t)
		mock.ctxt.SetSqlModeOverride("")
		p, err := runOneStmt(mock, t,
			"select r.k from enum_r r group by r.id having r.k='b'")
		require.NoError(t, err)
		require.NotNil(t, p)
	})
}

// TestCorrelatedAggregateArgsOnEnumSetKeysMatchIntPolicy extends the #31568
// proof to direct outer-column aggregate arguments (review follow-up). An
// ordinary aggregate's arguments cannot carry a live correlation: the subquery
// machinery only rewrites permitted correlated shapes, and group argument
// evaluation has no Expr_Corr implementation. Before this regression the
// display wrapper hid the correlation from HavingBinder's insideAgg branch,
// the binder admitted the plan with the correlation intact inside MAX/COUNT
// arguments, and execution failed with an unsupported-expression NYI while
// the same query with an INT key was rejected at bind time.
func TestCorrelatedAggregateArgsOnEnumSetKeysMatchIntPolicy(t *testing.T) {
	pairs := []struct{ special, plain string }{
		{
			"select l.id, (select max(l.k) from enum_r r) from enum_l l",
			"select l.id, (select max(l.k) from int2_r r) from int2_l l",
		},
		{
			"select l.id, (select count(l.k) from enum_r r) from enum_l l",
			"select l.id, (select count(l.k) from int2_r r) from int2_l l",
		},
		{
			"select l.id from enum_l l where exists(select 1 from enum_r r group by r.id having max(l.k)='a')",
			"select l.id from int2_l l where exists(select 1 from int2_r r group by r.id having max(l.k)=1)",
		},
		{
			"select l.id from enump_l l group by l.id having exists(select max(l.k) from enump_r r group by r.id)",
			"select l.id from intp_l l group by l.id having exists(select max(l.k) from intp_r r group by r.id)",
		},
		{
			"select l.id, (select max(l.k) from set_r r) from set_l l",
			"select l.id, (select max(l.k) from int2_r r) from int2_l l",
		},
		{
			"select l.id, (select count(l.k) from set_r r) from set_l l",
			"select l.id, (select count(l.k) from int2_r r) from int2_l l",
		},
		{
			"select l.id from set_l l where exists(select 1 from set_r r group by r.id having max(l.k)='a')",
			"select l.id from int2_l l where exists(select 1 from int2_r r group by r.id having max(l.k)=1)",
		},
		{
			"select l.id from setp_l l group by l.id having exists(select max(l.k) from setp_r r group by r.id)",
			"select l.id from intp_l l group by l.id having exists(select max(l.k) from intp_r r group by r.id)",
		},
	}
	for _, pair := range pairs {
		specialErr := correlatedHavingErrFor(t, pair.special)
		plainErr := correlatedHavingErrFor(t, pair.plain)
		if plainErr == nil {
			require.NoErrorf(t, specialErr,
				"special-typed key rejected while the INT key plans: %q", pair.special)
		} else {
			require.Errorf(t, specialErr,
				"special-typed key accepted while the INT key is rejected: %q", pair.special)
			require.ErrorContainsf(t, specialErr, "correlated columns in aggregate function",
				"special-typed key must get the correlated-aggregate NYI, not an invalid plan: %q", pair.special)
		}
	}

	// The aggregate-argument subquery exception stays intact: the correlation
	// belongs to the subquery's own WHERE, so the wrapper is admitted through
	// the subqueryInAggregateInput path and flattened exactly like a bare INT
	// reference, instead of dying in the aggregate argument.
	for _, sql := range []string{
		"select max((select r.id from enum_r r where r.k=l.k)) from enum_l l",
		"select max((select r.id from set_r r where r.k=l.k)) from set_l l",
		"select max((select r.id from int_r r where r.id=l.id)) from int_l l",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := runOneStmt(newCorrelatedSpecialTypeMock(t), t, sql)
			require.NoError(t, err)
			require.NotNil(t, p)
		})
	}
}
