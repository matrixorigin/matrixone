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

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// IdentifierName is referenced in the expression
type IdentifierName interface {
	Expr
}

// sql indentifier
type Identifier string

func (node *Identifier) Format(ctx *FmtCtx) {
	ctx.WriteString(string(*node))
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

	// Parts are the name components (at most 4: column, table, db/schema, catalog.), in reverse order.
	Parts NameParts
}

func (node *UnresolvedName) Format(ctx *FmtCtx) {
	for i := node.NumParts - 1; i >= 0; i-- {
		ctx.WriteString(node.Parts[i])
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

// GetNames dbName, tableName, colName
func (node *UnresolvedName) GetNames() (string, string, string) {
	return node.Parts[2], node.Parts[1], node.Parts[0]
}

// the path in an UnresolvedName.
type NameParts = [4]string

func NewUnresolvedName(ctx context.Context, parts ...string) (*UnresolvedName, error) {
	l := len(parts)
	if l < 1 || l > 4 {
		return nil, moerr.NewInternalError(ctx, "the count of name parts among [1,4]")
	}
	u := &UnresolvedName{
		NumParts: len(parts),
		Star:     false,
	}
	for i := 0; i < len(parts); i++ {
		u.Parts[i] = parts[l-1-i]
	}
	return u, nil
}

func SetUnresolvedName(parts ...string) *UnresolvedName {
	l := len(parts)
	u := &UnresolvedName{
		NumParts: len(parts),
		Star:     false,
	}
	for i := 0; i < len(parts); i++ {
		u.Parts[i] = parts[l-1-i]
	}
	return u
}

func NewUnresolvedNameWithStar(ctx context.Context, parts ...string) (*UnresolvedName, error) {
	l := len(parts)
	if l < 1 || l > 3 {
		return nil, moerr.NewInternalError(ctx, "the count of name parts among [1,3]")
	}
	u := &UnresolvedName{
		NumParts: len(parts),
		Star:     true,
	}
	u.Parts[0] = ""
	for i := 0; i < len(parts); i++ {
		u.Parts[i] = parts[l-1-i]
	}
	return u, nil
}

func SetUnresolvedNameWithStar(parts ...string) *UnresolvedName {
	l := len(parts)
	u := &UnresolvedName{
		NumParts: len(parts),
		Star:     true,
	}
	for i := 0; i < len(parts); i++ {
		u.Parts[i] = parts[l-1-i]
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
