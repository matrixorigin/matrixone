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
	reuse.CreatePool[CreateView](
		func() *CreateView { return &CreateView{} },
		func(c *CreateView) { c.reset() },
		reuse.DefaultOptions[CreateView](), //.
	) //WithEnableChecker()
}

type CreateView struct {
	statementImpl
	Replace     bool
	Name        *TableName
	ColNames    IdentifierList
	AsSource    *Select
	IfNotExists bool
}

func NewCreateView(replace bool, name *TableName, colNames IdentifierList, asSource *Select, ifNotExists bool) *CreateView {
	c := reuse.Alloc[CreateView](nil)
	c.Replace = replace
	c.Name = name
	c.ColNames = colNames
	c.AsSource = asSource
	c.IfNotExists = ifNotExists
	return c
}

func (node *CreateView) Free() {
	reuse.Free[CreateView](node, nil)
}

func (node *CreateView) Format(ctx *FmtCtx) {
	ctx.WriteString("create ")

	if node.Replace {
		ctx.WriteString("or replace ")
	}

	ctx.WriteString("view ")

	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}

	node.Name.Format(ctx)
	if len(node.ColNames) > 0 {
		ctx.WriteString(" (")
		node.ColNames.Format(ctx)
		ctx.WriteByte(')')
	}
	ctx.WriteString(" as ")
	node.AsSource.Format(ctx)
}

func (node *CreateView) reset() {
	// if node.Name != nil {
	// node.Name.Free()
	// }
	// if node.AsSource != nil {
	// node.AsSource.Free()
	// }
	*node = CreateView{}
}

func (node CreateView) TypeName() string { return "tree.CreateView" }

func (node *CreateView) GetStatementType() string { return "Create View" }
func (node *CreateView) GetQueryType() string     { return QueryTypeDDL }
