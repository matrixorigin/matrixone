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

import "strings"

// the INSERT statement.
type Insert struct {
	statementImpl
	Table TableExpr

	Accounts          IdentifierList
	PartitionNames    IdentifierList
	PartitionValues   PartitionValues
	Columns           IdentifierList
	Rows              *Select
	OnDuplicateUpdate UpdateExprs
	Overwrite         bool
	IsRestore         bool
	IsRestoreByTs     bool
	FromDataTenantID  uint32
	With              *With
}

func (node *Insert) Format(ctx *FmtCtx) {
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	if node.Overwrite {
		ctx.WriteString("insert overwrite ")
	} else {
		ctx.WriteString("insert into ")
	}
	node.Table.Format(ctx)

	if node.PartitionValues != nil {
		ctx.WriteString(" partition(")
		node.PartitionValues.Format(ctx)
		ctx.WriteByte(')')
	} else if node.PartitionNames != nil {
		ctx.WriteString(" partition(")
		node.PartitionNames.Format(ctx)
		ctx.WriteByte(')')
	}

	if node.Columns != nil {
		ctx.WriteString(" (")
		node.Columns.Format(ctx)
		ctx.WriteByte(')')
	}
	if node.Accounts != nil {
		ctx.WriteString(" accounts(")
		node.Accounts.Format(ctx)
		ctx.WriteByte(')')
	}
	if node.Rows != nil {
		ctx.WriteByte(' ')
		node.Rows.Format(ctx)
	}
	if len(node.OnDuplicateUpdate) > 0 {
		ctx.WriteString(" on duplicate key update ")
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

type InsertPartitionClause struct {
	Names  IdentifierList
	Values PartitionValues
}

type PartitionValue struct {
	Name Identifier
	Expr Expr
}

type PartitionValues []PartitionValue

func (node *PartitionValues) Format(ctx *FmtCtx) {
	for idx := range *node {
		if idx > 0 {
			ctx.WriteString(", ")
		}
		value := (*node)[idx]
		ctx.WriteString(string(value.Name))
		ctx.WriteString(" = ")
		if str, ok := value.Expr.(*StrVal); ok {
			writePartitionStringLiteral(ctx, str.String())
		} else if num, ok := value.Expr.(*NumVal); ok && num.ValType == P_char {
			writePartitionStringLiteral(ctx, num.String())
		} else if value.Expr != nil {
			value.Expr.Format(ctx)
		}
	}
}

func writePartitionStringLiteral(ctx *FmtCtx, value string) {
	ctx.WriteString("'")
	ctx.WriteString(strings.ReplaceAll(value, "'", "''"))
	ctx.WriteString("'")
}
