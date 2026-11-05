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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ENUM retains its SQL domain in the table schema; index comparisons and
// locks use the unsigned ordinal, including an IF-masked key already lowered
// by the function binder. Both sides must enter this same physical domain.
func indexKeyStorageType(t plan.Type) plan.Type {
	if types.T(t.Id) == types.T_enum {
		return plan.Type{Id: int32(types.T_uint16), NotNullable: t.NotNullable}
	}
	return t
}
func bindIndexKeyStorageExpr(b *QueryBuilder, e *plan.Expr, t plan.Type) (*plan.Expr, error) {
	t = indexKeyStorageType(t)
	if e == nil {
		return nil, moerr.NewInternalError(b.GetContext(), "missing index key expression")
	}
	if e.Typ.Id == t.Id {
		return e, nil
	}
	if types.T(e.Typ.Id) == types.T_enum && types.T(t.Id) == types.T_uint16 {
		return forceCastExpr(b.GetContext(), e, t)
	}
	return nil, moerr.NewInternalErrorf(b.GetContext(), "index key type mismatch: logical %d, physical %d", e.Typ.Id, t.Id)
}

func (builder *QueryBuilder) requireOrderedODKUProtocol() error {
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return moerr.NewNotSupported(builder.GetContext(), "ordered ODKU requires 4.2 protocol version 12")
	}
	v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	n, valid := v.(int64)
	if ok && valid && n >= defines.MORPCVersion12 {
		return nil
	}
	return moerr.NewNotSupported(builder.GetContext(), "ordered ODKU requires 4.2 protocol version 12")
}
func odkuAffectedColumns(t *plan.TableDef, names []string) map[string]bool {
	affected := make(map[string]bool, len(names))
	for _, n := range names {
		affected[n] = true
	}
	for changed := true; changed; {
		changed = false
		for _, c := range t.Cols {
			if c.GeneratedCol == nil || affected[c.Name] {
				continue
			}
			for _, p := range collectRefColPos(c.GeneratedCol.Expr) {
				if p >= 0 && int(p) < len(t.Cols) && affected[t.Cols[p].Name] {
					affected[c.Name] = true
					changed = true
					break
				}
			}
		}
	}
	return affected
}
func (builder *QueryBuilder) odkuActionChecks(t *plan.TableDef, tag int32, affected map[string]bool) ([]*plan.Expr, []*plan.Expr, error) {
	refs := make([]*plan.Expr, len(t.Cols))
	for i, c := range t.Cols {
		refs[i] = &plan.Expr{Typ: c.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: int32(i), Name: c.Name}}}
	}
	inserts, err := buildCheckConstraintExprs(builder, t, refs, false)
	if err != nil {
		return nil, nil, err
	}
	subset := *t
	subset.Checks = nil
	for _, c := range t.Checks {
		for _, p := range collectRefColPos(c.Check) {
			if p < 0 || int(p) >= len(t.Cols) {
				return nil, nil, moerr.NewInternalError(builder.GetContext(), "invalid ODKU CHECK column")
			}
			if affected[t.Cols[p].Name] {
				subset.Checks = append(subset.Checks, c)
				break
			}
		}
	}
	updates, err := buildCheckConstraintExprs(builder, &subset, refs, false)
	if err != nil {
		return nil, nil, err
	}
	for i, c := range t.Cols {
		if c.Name == catalog.Row_ID || c.Default == nil || c.Default.NullAbility || strings.HasPrefix(c.Name, catalog.PrefixCBColName) {
			continue
		}
		ref := DeepCopyExpr(refs[i])
		ref.Typ.NotNullable = false
		ok, e := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*plan.Expr{ref})
		if e != nil {
			return nil, nil, e
		}
		check, e := BindFuncExprImplByPlanExpr(builder.GetContext(), "_check_constraint_assert", []*plan.Expr{ok, makePlan2StringConstExprWithType(fmt.Sprintf("Column '%s' cannot be null", c.Name))})
		if e != nil {
			return nil, nil, e
		}
		inserts = append(inserts, check)
		if affected[c.Name] {
			updates = append(updates, DeepCopyExpr(check))
		}
	}
	return inserts, updates, nil
}

func (builder *QueryBuilder) odkuForeignKeyChecks(t *plan.TableDef, scanTag, selectTag int32, selectNode *plan.Node, affected map[string]bool) ([]*plan.OdkuForeignKeyCheck, int32, error) {
	enabled, err := builder.modernInsertFkCheckEnabled(t)
	if err != nil || !enabled {
		return nil, -1, err
	}
	anyAffected := false
	idName := make(map[uint64]string, len(t.Cols))
	for _, c := range t.Cols {
		idName[c.ColId] = c.Name
	}
	for _, fk := range t.Fkeys {
		if fk.ForeignTbl != 0 {
			for _, id := range fk.Cols {
				if affected[idName[id]] {
					anyAffected = true
				}
			}
		}
	}
	flag := int32(-1)
	if anyAffected {
		flag = int32(len(selectNode.ProjectList))
		selectNode.ProjectList = append(selectNode.ProjectList, makePlan2BoolConstExprWithType(true))
	}
	checks := make([]*plan.OdkuForeignKeyCheck, 0, len(t.Fkeys))
	for _, fk := range t.Fkeys {
		if fk.ForeignTbl == 0 {
			continue
		}
		c := &plan.OdkuForeignKeyCheck{}
		for _, id := range fk.Cols {
			name, ok := idName[id]
			if !ok {
				return nil, -1, moerr.NewInternalError(builder.GetContext(), "unknown ODKU foreign-key column")
			}
			c.Columns = append(c.Columns, plan.ColRef{RelPos: scanTag, ColPos: t.Name2ColIndex[name], Name: name})
			c.CheckUpdates = c.CheckUpdates || affected[name]
		}
		pos := int32(len(selectNode.ProjectList))
		selectNode.ProjectList = append(selectNode.ProjectList, makePlan2BoolConstExprWithType(false))
		c.Eligibility = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: selectTag, ColPos: pos}}}
		checks = append(checks, c)
	}
	return checks, flag, nil
}
func (builder *QueryBuilder) appendODKUActionFKChecks(ctx *BindContext, t *plan.TableDef, last int32, tag int32, positions map[string]int32, checks []*plan.OdkuForeignKeyCheck, final int32, inputProject []*plan.Expr) (int32, error) {
	last, oks, err := builder.appendModernChildFkMarkOksWithInput(ctx, t, last, tag, func(name string) int32 {
		p, ok := positions[t.Name+"."+name]
		if !ok {
			return -1
		}
		return p
	}, false, inputProject)
	if err != nil {
		return 0, err
	}
	if len(oks) != len(checks) {
		return 0, moerr.NewInternalError(builder.GetContext(), "ODKU foreign-key check layout mismatch")
	}
	assertions := make([]*plan.Expr, len(oks))
	for i, ok := range oks {
		skip, e := BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{DeepCopyExpr(checks[i].Eligibility)})
		if e != nil {
			return 0, e
		}
		pass, e := BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{skip, ok})
		if e != nil {
			return 0, e
		}
		finalExpr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: final}}}
		pass, e = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{finalExpr, pass})
		if e != nil {
			return 0, e
		}
		assertions[i], e = BindFuncExprImplByPlanExpr(builder.GetContext(), "assert", []*plan.Expr{pass, makePlan2StringConstExprWithType("Cannot add or update a child row: a foreign key constraint fails"), makePlan2StringConstExprWithType(foreignKeyNoReferencedRowAssert)})
		if e != nil {
			return 0, e
		}
	}
	last = builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{last}, FilterList: assertions, FilterIsBarrier: true}, ctx)
	return builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{last}, FilterList: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: final}}}}}, ctx), nil
}

// Cols is authoritative; resolver-produced index metadata need not have the
// derived Name2ColIndex map initialized. Never silently substitute column zero.
func (builder *QueryBuilder) indexTableColumnPosition(t *plan.TableDef, name string) (int32, error) {
	if t != nil {
		for i, c := range t.Cols {
			if c != nil && c.Name == name {
				return int32(i), nil
			}
		}
	}
	return 0, moerr.NewInternalErrorf(builder.GetContext(), "index table is missing column %s", name)
}

// Scalar-subquery flattening can append columns under a separate binding tag.
// Carry those columns through arbitration without evaluating the assignment or
// changing the converted source layout. Old-row references remain replay inputs.
func (builder *QueryBuilder) preserveODKUSourceColumns(
	ctx *BindContext, lastID, tag, oldRowTag int32, source *plan.Node, ordered []*plan.Expr, updates map[string]*plan.Expr,
) (int32, int32, *plan.Node) {
	var walk func(*plan.Expr, func(*plan.Expr))
	walk = func(e *plan.Expr, visit func(*plan.Expr)) {
		if e == nil {
			return
		}
		if e.GetCol() != nil {
			visit(e)
			return
		}
		if f := e.GetF(); f != nil {
			for _, a := range f.Args {
				walk(a, visit)
			}
		}
		if l := e.GetList(); l != nil {
			for _, a := range l.List {
				walk(a, visit)
			}
		}
	}
	positions := make(map[[2]int32]int32)
	var helpers []*plan.Expr
	collect := func(e *plan.Expr) {
		c := e.GetCol()
		if c.RelPos == tag || c.RelPos == oldRowTag {
			return
		}
		key := [2]int32{c.RelPos, c.ColPos}
		if _, ok := positions[key]; !ok {
			positions[key] = int32(len(source.ProjectList) + len(helpers))
			helpers = append(helpers, DeepCopyExpr(e))
		}
	}
	for _, e := range ordered {
		walk(e, collect)
	}
	if len(helpers) == 0 {
		return lastID, tag, source
	}
	project := make([]*plan.Expr, len(source.ProjectList), len(source.ProjectList)+len(helpers))
	for i, e := range source.ProjectList {
		project[i] = &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: int32(i)}}}
	}
	project = append(project, helpers...)
	newTag := builder.genNewBindTag()
	rewrite := func(e *plan.Expr) {
		c := e.GetCol()
		if c.RelPos == oldRowTag {
			return
		}
		if c.RelPos == tag {
			c.RelPos = newTag
			return
		}
		if p, ok := positions[[2]int32{c.RelPos, c.ColPos}]; ok {
			c.RelPos = newTag
			c.ColPos = p
		}
	}
	for _, e := range ordered {
		walk(e, rewrite)
	}
	for _, e := range updates {
		walk(e, rewrite)
	}
	lastID = builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{lastID}, BindingTags: []int32{newTag}, ProjectList: project}, ctx)
	return lastID, newTag, builder.qry.Nodes[lastID]
}

// This is a structural upper bound, never a cardinality estimate. ValueScan's
// executor uses the actual first column's Data length, so validate every column.
func (builder *QueryBuilder) odkuSourceAtMostOne(id int32) bool {
	for range len(builder.qry.Nodes) {
		if id < 0 || int(id) >= len(builder.qry.Nodes) {
			return false
		}
		n := builder.qry.Nodes[id]
		if n == nil {
			return false
		}
		switch n.NodeType {
		case plan.Node_PROJECT, plan.Node_PRE_INSERT, plan.Node_FILTER:
			if len(n.Children) != 1 {
				return false
			}
			id = n.Children[0]
		case plan.Node_VALUE_SCAN:
			rows := n.RowsetData
			if len(n.Children) != 0 || rows == nil || rows.RowCount != 1 || len(rows.Cols) == 0 {
				return false
			}
			if n.TableDef == nil || len(n.TableDef.Cols) != len(rows.Cols) {
				return false
			}
			for _, c := range rows.Cols {
				if c == nil || len(c.Data) != 1 || c.Data[0] == nil || c.Data[0].Expr == nil {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	return false
}
