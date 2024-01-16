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

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[Insert](
		func() *Insert { return &Insert{} },
		func(i *Insert) {
			i.release()
		},
		reuse.DefaultOptions[Insert]().
			WithEnableChecker())
}

// the INSERT statement.
type Insert struct {
	statementImpl
	Table             TableExpr
	Accounts          IdentifierList
	PartitionNames    IdentifierList
	Columns           IdentifierList
	Rows              *Select
	OnDuplicateUpdate UpdateExprs
}

func (node *Insert) release() {
	if node.Rows != nil {
		reuse.Free(node.Rows, nil)
	}
}

func (node *Insert) Free() {
	reuse.Free[Insert](node, nil)
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

func NewInsert(columns IdentifierList, rows *Select) *Insert {
	insert := reuse.Alloc[Insert](nil)
	insert.Rows = rows
	return insert
}

type Assignment struct {
	Column Identifier
	Expr   Expr
}

func (node Insert) Name() string {
	return "tree.Insert"
}
