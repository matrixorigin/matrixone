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

//Delete statement
type Delete struct {
	statementImpl
	Tables         TableExprs
	TableRefs      TableExprs
	PartitionNames IdentifierList
	Where          *Where
	OrderBy        OrderBy
	Limit          *Limit
	With           *With
}

func (node *Delete) Format(ctx *FmtCtx) {
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("delete from ")

	prefix := ""
	for _, a := range node.Tables {
		ctx.WriteString(prefix)
		a.Format(ctx)
		prefix = ", "
	}

	if node.PartitionNames != nil {
		ctx.WriteString(" partition(")
		node.PartitionNames.Format(ctx)
		ctx.WriteByte(')')
	}

	if node.TableRefs != nil {
		ctx.WriteString(" using ")
		node.TableRefs.Format(ctx)
	}

	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		node.OrderBy.Format(ctx)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		node.Limit.Format(ctx)
	}
}

func NewDelete(ts TableExprs, w *Where, o OrderBy, l *Limit) *Delete {
	return &Delete{
		Tables:  ts,
		Where:   w,
		OrderBy: o,
		Limit:   l,
	}
}
