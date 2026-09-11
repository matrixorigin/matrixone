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
	Replace       bool
	Name          *TableName
	ColNames      IdentifierList
	AsSource      *Select
	IfNotExists   bool
	Materialized  bool
	RefreshMethod MaterializedViewRefreshMethod
	RefreshTiming MaterializedViewRefreshTiming
}

type MaterializedViewRefreshMethod int8

const (
	MaterializedViewRefreshForce MaterializedViewRefreshMethod = iota
	MaterializedViewRefreshFast
	MaterializedViewRefreshComplete
)

type MaterializedViewRefreshTiming int8

const (
	MaterializedViewRefreshOnChange MaterializedViewRefreshTiming = iota
	MaterializedViewRefreshOnDemand
)

func NewCreateMaterializedView(name *TableName, colNames IdentifierList, asSource *Select, ifNotExists bool) *CreateView {
	node := NewCreateView(false, name, colNames, asSource, ifNotExists)
	node.Materialized = true
	return node
}

func NewCreateMaterializedViewWithRefresh(name *TableName, colNames IdentifierList, asSource *Select, ifNotExists bool, method MaterializedViewRefreshMethod, timing MaterializedViewRefreshTiming) *CreateView {
	node := NewCreateMaterializedView(name, colNames, asSource, ifNotExists)
	node.RefreshMethod = method
	node.RefreshTiming = timing
	return node
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

	if node.Materialized {
		ctx.WriteString("materialized ")
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
	if node.Materialized && (node.RefreshMethod != MaterializedViewRefreshForce || node.RefreshTiming != MaterializedViewRefreshOnChange) {
		ctx.WriteString(" refresh ")
		switch node.RefreshMethod {
		case MaterializedViewRefreshFast:
			ctx.WriteString("fast")
		case MaterializedViewRefreshComplete:
			ctx.WriteString("complete")
		default:
			ctx.WriteString("force")
		}
		ctx.WriteString(" on ")
		if node.RefreshTiming == MaterializedViewRefreshOnDemand {
			ctx.WriteString("demand")
		} else {
			ctx.WriteString("change")
		}
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

type RefreshMaterializedView struct {
	statementImpl
	Name *TableName
}

func NewRefreshMaterializedView(name *TableName) *RefreshMaterializedView {
	return &RefreshMaterializedView{Name: name}
}

func (node *RefreshMaterializedView) Format(ctx *FmtCtx) {
	ctx.WriteString("refresh materialized view ")
	node.Name.Format(ctx)
}

func (node *RefreshMaterializedView) Free()                    {}
func (node RefreshMaterializedView) TypeName() string          { return "tree.RefreshMaterializedView" }
func (node *RefreshMaterializedView) GetStatementType() string { return "Refresh Materialized View" }
func (node *RefreshMaterializedView) GetQueryType() string     { return QueryTypeDDL }
