// Copyright 2021 - 2022 Matrix Origin
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

// the REPLACE statement.
type Replace struct {
	statementImpl
	Table          TableExpr
	PartitionNames IdentifierList
	Columns        IdentifierList
	Rows           *Select
}

func (node *Replace) Format(ctx *FmtCtx) {
	ctx.WriteString("replace into ")
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
}

func NewReplace(t TableExpr, c IdentifierList, r *Select, p IdentifierList) *Replace {
	return &Replace{
		Table:          t,
		Columns:        c,
		Rows:           r,
		PartitionNames: p,
	}
}
