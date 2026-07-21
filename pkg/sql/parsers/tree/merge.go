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

package tree

// Merge represents a SQL MERGE statement. The initial production consumer is
// the Iceberg row-level DML planner, so the AST keeps the standard action
// clauses explicit rather than lowering them back into UPDATE/INSERT text.
type Merge struct {
	statementImpl
	Target  TableExpr
	Source  TableExpr
	On      Expr
	Clauses MergeClauses
	With    *With
}

func (node *Merge) Format(ctx *FmtCtx) {
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("merge into ")
	if node.Target != nil {
		node.Target.Format(ctx)
	}
	ctx.WriteString(" using ")
	if node.Source != nil {
		node.Source.Format(ctx)
	}
	ctx.WriteString(" on ")
	if node.On != nil {
		node.On.Format(ctx)
	}
	for _, clause := range node.Clauses {
		ctx.WriteByte(' ')
		clause.Format(ctx)
	}
}

func (node *Merge) GetStatementType() string { return "Merge" }
func (node *Merge) GetQueryType() string     { return QueryTypeDML }
func (node *Merge) Free()                    {}

type MergeAction int

const (
	MergeActionUpdate MergeAction = iota + 1
	MergeActionDelete
	MergeActionInsert
)

type MergeClauses []*MergeClause

type MergeClause struct {
	NodeFormatter
	Matched       bool
	Condition     Expr
	Action        MergeAction
	UpdateExprs   UpdateExprs
	InsertColumns IdentifierList
	InsertValues  Exprs
}

func (node *MergeClause) Format(ctx *FmtCtx) {
	if node == nil {
		return
	}
	ctx.WriteString("when ")
	if !node.Matched {
		ctx.WriteString("not ")
	}
	ctx.WriteString("matched")
	if node.Condition != nil {
		ctx.WriteString(" and ")
		node.Condition.Format(ctx)
	}
	ctx.WriteString(" then ")
	switch node.Action {
	case MergeActionUpdate:
		ctx.WriteString("update set ")
		node.UpdateExprs.Format(ctx)
	case MergeActionDelete:
		ctx.WriteString("delete")
	case MergeActionInsert:
		ctx.WriteString("insert")
		if len(node.InsertColumns) > 0 {
			ctx.WriteString(" (")
			node.InsertColumns.Format(ctx)
			ctx.WriteByte(')')
		}
		ctx.WriteString(" values (")
		node.InsertValues.Format(ctx)
		ctx.WriteByte(')')
	}
}
