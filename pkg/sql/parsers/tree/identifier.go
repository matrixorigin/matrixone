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

// IdentifierName is referenced in the expression
type IdentifierName interface {
	Expr
}

// sql indentifier
type Identifier string

func (node *Identifier) Format(ctx *FmtCtx) {
	ctx.WriteString(string(*node))
}

func (node *Identifier) String() string {
	return string(*node)
}

type UnrestrictedIdentifier string

// the list of identifiers.
type IdentifierList []Identifier

func (node *IdentifierList) Format(ctx *FmtCtx) {
	for i := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.WriteString(string((*node)[i]))
	}
}

type ColumnItem struct {
	IdentifierName

	//the name of the column
	ColumnName Identifier
}

// the unresolved qualified name like column name.
type UnresolvedName struct {
	exprImpl
	//the number of name parts specified, including the star. Always 1 or greater.
	NumParts int

	//the name ends with a star. then the first element is empty in the Parts
	Star bool

	// CStrParts are the name components (at most 4: column, table, db/schema, catalog.), in reverse order.
	CStrParts CStrParts
}

func (node *UnresolvedName) DbName() string {
	if node.NumParts < 3 {
		return ""
	}
	return node.CStrParts[2].Compare()
}

func (node *UnresolvedName) DbNameOrigin() string {
	if node.NumParts < 3 {
		return ""
	}
	return node.CStrParts[2].Origin()
}

func (node *UnresolvedName) TblName() string {
	if node.NumParts < 2 {
		return ""
	}
	return node.CStrParts[1].Compare()
}

func (node *UnresolvedName) TblNameOrigin() string {
	if node.NumParts < 2 {
		return ""
	}
	return node.CStrParts[1].Origin()
}

func (node *UnresolvedName) ColName() string {
	if node.NumParts < 1 {
		return ""
	}
	return node.CStrParts[0].Compare()
}

func (node *UnresolvedName) ColNameOrigin() string {
	if node.NumParts < 1 {
		return ""
	}
	return node.CStrParts[0].Origin()
}

func (node *UnresolvedName) Format(ctx *FmtCtx) {
	for i := node.NumParts - 1; i >= 0; i-- {
		ctx.WriteString(node.CStrParts[i].Origin())
		if i > 0 {
			ctx.WriteByte('.')
		}
	}
	if node.Star && node.NumParts > 0 {
		ctx.WriteString(".*")
	} else if node.Star {
		ctx.WriteByte('*')
	}
}

// Accept implements NodeChecker Accept interface.
func (node *UnresolvedName) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	return v.Exit(newNode)
}

func NewUnresolvedName(parts ...*CStr) *UnresolvedName {
	l := len(parts)
	u := &UnresolvedName{
		NumParts: l,
		Star:     false,
	}
	// parts[0] is the column name
	for i := 0; i < l; i++ {
		u.CStrParts[i] = parts[l-1-i]
	}
	return u
}

func NewUnresolvedColName(colName string) *UnresolvedName {
	u := &UnresolvedName{
		NumParts: 1,
		Star:     false,
	}
	// colName always ignore case sensitivity
	u.CStrParts[0] = NewCStr(colName, 1)
	return u
}

func NewUnresolvedNameWithStar(parts ...*CStr) *UnresolvedName {
	l := len(parts)
	u := &UnresolvedName{
		NumParts: l,
		Star:     true,
	}
	// parts[0] is the table name
	for i := 0; i < l; i++ {
		u.CStrParts[i] = parts[l-1-i]
	}
	return u
}

// variable in the scalar expression
type VarName interface {
	Expr
}

var _ VarName = &UnresolvedName{}
var _ VarName = UnqualifiedStar{}

// '*' in the scalar expression
type UnqualifiedStar struct {
	VarName
}

func (node UnqualifiedStar) Format(ctx *FmtCtx) {
	ctx.WriteByte('*')
}

var starName VarName = UnqualifiedStar{}

func StarExpr() VarName {
	return starName
}
