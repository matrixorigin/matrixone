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
	CheckOption string
}

func NewCreateView(replace bool, name *TableName, colNames IdentifierList, asSource *Select, ifNotExists bool, checkOption string) *CreateView {
	c := reuse.Alloc[CreateView](nil)
	c.Replace = replace
	c.Name = name
	c.ColNames = colNames
	c.AsSource = asSource
	c.IfNotExists = ifNotExists
	c.CheckOption = checkOption
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
	if node.CheckOption != "" && node.CheckOption != "NONE" {
		ctx.WriteString(" with ")
		ctx.WriteString(node.CheckOption)
		ctx.WriteString(" check option")
	}
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

// WithViewColumnNames returns an AST copy that exposes the explicit column
// names from CREATE/ALTER VIEW through a derived-table column list. The
// original SELECT remains the inner query, so aliases referenced by ORDER BY,
// HAVING, or other clauses keep their original scope and meaning. The outer
// projection is explicit rather than a wildcard so a previously expanded
// SELECT remains frozen. The wrapper also covers UNION output and does not
// mutate any of the input AST nodes.
func WithViewColumnNames(stmt *Select, colNames IdentifierList) *Select {
	if stmt == nil || len(colNames) == 0 {
		return stmt
	}
	inner := NewSubquery(stmt, false)
	derived := NewAliasedTableExpr(
		NewParenTableExpr(inner),
		"__mo_view_definition",
		colNames,
	)
	exprs := make(SelectExprs, len(colNames))
	for i, colName := range colNames {
		exprs[i] = SelectExpr{
			Expr: NewUnresolvedName(
				NewCStr("__mo_view_definition", 0),
				NewCStr(string(colName), 0),
			),
			As: NewCStr(string(colName), 0),
		}
	}
	return NewSelect(
		&SelectClause{
			Exprs: exprs,
			From:  NewFrom(TableExprs{derived}),
		},
		nil,
		nil,
	)
}
