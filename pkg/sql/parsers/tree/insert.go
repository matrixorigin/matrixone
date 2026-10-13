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
	// TargetDatabaseName and TargetTableName preserve the user-visible target
	// identity even when resolution rewrites Table to a temporary physical name.
	TargetDatabaseName Identifier
	TargetTableName    Identifier

	Accounts        IdentifierList
	PartitionNames  IdentifierList
	PartitionValues PartitionValues
	Columns         IdentifierList
	ColumnNames     []*UnresolvedName
	Rows            *Select
	// Ignore keeps INSERT's error-conversion policy independent from the
	// duplicate-key action.  In particular, INSERT IGNORE ... ON DUPLICATE KEY
	// UPDATE must still carry the ordered UPDATE expressions.
	Ignore            bool
	OnDuplicateUpdate UpdateExprs
	Overwrite         bool
	IsRestore         bool
	IsRestoreByTs     bool
	FromDataTenantID  uint32
	With              *With
	Returning         SelectExprs
}

func (node *Insert) Format(ctx *FmtCtx) {
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	ignore := node.IsIgnore()
	if node.Overwrite {
		ctx.WriteString("insert overwrite ")
	} else if ignore {
		ctx.WriteString("insert ignore into ")
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

	if node.ColumnNames != nil {
		ctx.WriteString(" (")
		formatUnresolvedNames(ctx, node.ColumnNames)
		ctx.WriteByte(')')
	} else if node.Columns != nil {
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
	updates := node.GetOnDuplicateUpdate()
	if len(updates) > 0 {
		ctx.WriteString(" on duplicate key update ")
		updates.Format(ctx)
	}
	if node.HasReturning() {
		ctx.WriteString(" returning ")
		node.Returning.Format(ctx)
	}
}

func (node *Insert) HasReturning() bool { return len(node.Returning) > 0 }

// IsIgnore reports the statement-level INSERT IGNORE policy.  The nil update
// sentinel is retained as a read-only compatibility fallback for callers that
// construct pre-28159 ASTs directly; the parser no longer emits it.
func (node *Insert) IsIgnore() bool {
	return node != nil && (node.Ignore ||
		(len(node.OnDuplicateUpdate) == 1 && node.OnDuplicateUpdate[0] == nil))
}

// GetOnDuplicateUpdate returns only executable ODKU assignments.  The former
// [nil] sentinel represented INSERT IGNORE/ON DUPLICATE KEY IGNORE and must not
// be interpreted as an UPDATE list by planners or prepared-statement binders.
func (node *Insert) GetOnDuplicateUpdate() UpdateExprs {
	if node == nil || (len(node.OnDuplicateUpdate) == 1 && node.OnDuplicateUpdate[0] == nil) {
		return nil
	}
	return node.OnDuplicateUpdate
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
	Column     Identifier
	ColumnName *UnresolvedName
	Expr       Expr
}

type InsertColumns struct {
	Identifiers IdentifierList
	Names       []*UnresolvedName
}

func formatUnresolvedNames(ctx *FmtCtx, names []*UnresolvedName) {
	for i, name := range names {
		if i > 0 {
			ctx.WriteString(", ")
		}
		name.Format(ctx)
	}
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
