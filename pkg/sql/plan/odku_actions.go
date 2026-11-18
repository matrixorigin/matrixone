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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (builder *QueryBuilder) buildODKUActions(table *plan.TableDef, imageTag, sourceTag int32, positions map[string]int32, names []string, assignments []*plan.Expr, affected map[string]bool, fks []*plan.OdkuForeignKeyCheck, final int32) (*plan.OdkuActionCtx, error) {
	c := &plan.OdkuActionCtx{
		PkColumn: table.Name2ColIndex[table.Pkey.PkeyColName], RowIdColumn: table.Name2ColIndex[catalog.Row_ID],
		InputColumns: make([]int32, len(table.Cols)), ActionFinalColumn: final,
	}
	for i, col := range table.Cols {
		c.InputColumns[i] = -1
		if p, ok := positions[table.Name+"."+col.Name]; ok {
			c.InputColumns[i] = p
		}
	}
	for i, name := range names {
		e, err := builder.normalizeODKUExpr(assignments[i], imageTag, sourceTag)
		if err != nil {
			return nil, err
		}
		c.UpdateColumns = append(c.UpdateColumns, table.Name2ColIndex[name])
		c.UpdateExprs = append(c.UpdateExprs, e)
	}
	var err error
	c.InsertChecks, c.UpdateChecks, err = builder.odkuActionChecks(table, imageTag, affected)
	if err != nil {
		return nil, err
	}
	for _, list := range [][]*plan.Expr{c.InsertChecks, c.UpdateChecks} {
		for i, e := range list {
			list[i], err = builder.normalizeODKUExpr(e, imageTag, sourceTag)
			if err != nil {
				return nil, err
			}
		}
	}
	c.InsertCheckConstraintCount = uint32(len(table.Checks))
	for _, fk := range fks {
		e, err := builder.normalizeODKUExpr(fk.Eligibility, imageTag, sourceTag)
		if err != nil {
			return nil, err
		}
		copy := &plan.OdkuForeignKeyCheck{Columns: slices.Clone(fk.Columns), Eligibility: e, CheckUpdates: fk.CheckUpdates}
		for i := range copy.Columns {
			copy.Columns[i].RelPos = 0
		}
		c.ForeignKeyChecks = append(c.ForeignKeyChecks, copy)
	}
	return c, nil
}

func (builder *QueryBuilder) appendODKUActions(bindCtx *BindContext, child int32, actions *plan.OdkuActionCtx) (int32, int32, *plan.Node) {
	tag := builder.genNewBindTag()
	project := DeepCopyExprList(actions.SourceColumns)
	project = append(project, DeepCopyExprList(actions.OldColumns)...)
	for _, e := range project[len(actions.SourceColumns):] {
		e.Typ.NotNullable = false
	}
	node := &plan.Node{
		NodeType: plan.Node_PRE_INSERT_UK, Children: []int32{child}, BindingTags: []int32{tag}, ProjectList: project,
		PreInsertUkCtx: &plan.PreInsertUkCtx{OrderedOdku: true, OdkuActions: actions},
	}
	return builder.appendNode(node, bindCtx), tag, node
}

// Execution expressions use a fixed pair of row batches. Only source references
// participate in relational rewrites; image references never name a plan node.
func (builder *QueryBuilder) normalizeODKUExpr(expr *plan.Expr, imageTag, incomingTag int32) (*plan.Expr, error) {
	expr = DeepCopyExpr(expr)
	var walk func(*plan.Expr) error
	walk = func(e *plan.Expr) error {
		if e == nil {
			return nil
		}
		if c := e.GetCol(); c != nil {
			switch c.RelPos {
			case imageTag:
				c.RelPos = 0
			case incomingTag:
				c.RelPos = 1
			default:
				return moerr.NewInternalError(builder.GetContext(), "unmaterialized ODKU assignment input")
			}
		}
		if f := e.GetF(); f != nil {
			for _, arg := range f.Args {
				if err := walk(arg); err != nil {
					return err
				}
			}
		}
		if list := e.GetList(); list != nil {
			for _, arg := range list.List {
				if err := walk(arg); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return expr, walk(expr)
}

func odkuSourceExpressions(c *plan.OdkuActionCtx) []*plan.Expr {
	if c == nil {
		return nil
	}
	exprs := make([]*plan.Expr, 0, len(c.SourceColumns)+len(c.OldColumns)+2*len(c.Constraints)+1)
	exprs = append(exprs, c.SourceColumns...)
	exprs = append(exprs, c.OldColumns...)
	for _, key := range c.Constraints {
		exprs = append(exprs, key.InputKey, key.PriorTarget)
	}
	return append(exprs, c.SourceOrdinal)
}

func replaceODKUSourceColumns(c *plan.OdkuActionCtx, projMap map[[2]int32]*plan.Expr) {
	if c == nil {
		return
	}
	replaceColumnsForExprList(c.SourceColumns, projMap)
	replaceColumnsForExprList(c.OldColumns, projMap)
	for _, key := range c.Constraints {
		key.InputKey = replaceColumnsForExpr(key.InputKey, projMap)
		key.PriorTarget = replaceColumnsForExpr(key.PriorTarget, projMap)
	}
	c.SourceOrdinal = replaceColumnsForExpr(c.SourceOrdinal, projMap)
}
