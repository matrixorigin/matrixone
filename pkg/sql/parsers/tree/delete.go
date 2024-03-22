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
	reuse.CreatePool[Delete](
		func() *Delete { return &Delete{} },
		func(d *Delete) { d.reset() },
		reuse.DefaultOptions[Delete](),
	)
}

// Delete statement
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

func (node *Delete) GetStatementType() string { return "Delete" }

func (node *Delete) GetQueryType() string { return QueryTypeDML }

func (node Delete) TypeName() string { return "tree.Delete" }

func NewDelete(ts TableExprs, w *Where) *Delete {
	de := reuse.Alloc[Delete](nil)
	de.Tables = ts
	de.Where = w
	return de
}

func (node *Delete) Free() {
	reuse.Free[Delete](node, nil)
}

func (node *Delete) reset() {
	// if node.Where != nil {
	// 	node.Where.Free()
	// }
	// if node.Limit != nil {
	// 	node.Limit.Free()
	// }
	// if node.With != nil {
	// 	node.With.Free()
	// }
	// if node.OrderBy != nil {
	// 	for _, item := range node.OrderBy {
	// 		item.Free()
	// 	}
	// }
	// if node.Tables != nil {
	// 	for _ , item := range node.Tables {
	// 		item.Free()
	// 	}
	// }
	// if node.TableRefs != nil {
	// 	for _ , item := range node.TableRefs {
	// 		item.Free()
	// 	}
	// }

	*node = Delete{}
}
