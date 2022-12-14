// Copyright 2021 Matrix Origin
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

// the INSERT statement.
type Insert struct {
	statementImpl
	Table             TableExpr
	PartitionNames    IdentifierList
	Columns           IdentifierList
	Rows              *Select
	OnDuplicateUpdate *UpdateList
}

func (node *Insert) Format(ctx *FmtCtx) {
	ctx.WriteString("insert into ")
	node.Table.Format(ctx)

	if node.PartitionNames != nil {
		ctx.WriteString(" partition(")
		node.PartitionNames.Format(ctx)
		ctx.WriteByte(')')
	}

	if node.Columns != nil {
		ctx.WriteString(" (")
		node.Columns.Format(ctx)
		ctx.WriteByte(')')
	}
	if node.Rows != nil {
		ctx.WriteByte(' ')
		node.Rows.Format(ctx)
	}
	if node.OnDuplicateUpdate != nil {
		ctx.WriteByte(' ')
		node.OnDuplicateUpdate.Format(ctx)
	}
}

func (node *Insert) GetStatementType() string { return "Insert" }
func (node *Insert) GetQueryType() string     { return QueryTypeDML }

func NewInsert(t TableExpr, c IdentifierList, r *Select, p IdentifierList) *Insert {
	return &Insert{
		Table:          t,
		Columns:        c,
		Rows:           r,
		PartitionNames: p,
	}
}

type Assignment struct {
	Column Identifier
	Expr   Expr
}

type UpdateList struct {
	Columns IdentifierList
	Rows    *Select
}

func (node *UpdateList) Format(ctx *FmtCtx) {
	ctx.WriteString("on duplicate key update ")
	if node.Columns != nil {
		ctx.WriteString(" (")
		node.Columns.Format(ctx)
		ctx.WriteByte(')')
	}
	if node.Rows != nil {
		ctx.WriteByte(' ')
		node.Rows.Format(ctx)
	}
}
