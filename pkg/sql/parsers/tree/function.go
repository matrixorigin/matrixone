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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type FunctionArg interface {
	NodeFormatter
	Expr
	GetName(ctx *FmtCtx) string
	// Deprecated: use plan.GetFunctionArgTypeStrFromAst instead
	GetType(ctx *FmtCtx) string
}

type FunctionArgImpl struct {
	FunctionArg
}

// container holding list of arguments in udf
type FunctionArgs []FunctionArg

type FunctionArgDecl struct {
	FunctionArgImpl
	Name       *UnresolvedName
	Type       ResolvableTypeReference
	DefaultVal Expr
}

type ReturnType struct {
	Type ResolvableTypeReference
}

func (node *FunctionArgDecl) Format(ctx *FmtCtx) {
	if node.Name != nil {
		node.Name.Format(ctx)
		ctx.WriteByte(' ')
	}
	node.Type.(*T).InternalType.Format(ctx)
	if node.DefaultVal != nil {
		ctx.WriteString(" default ")
		ctx.PrintExpr(node, node.DefaultVal, true)
	}
}

func (node *FunctionArgDecl) GetName(ctx *FmtCtx) string {
	if node.Name == nil {
		return ""
	}
	node.Name.Format(ctx)
	return ctx.String()
}

func (node *FunctionArgDecl) GetType(ctx *FmtCtx) string {
	node.Type.(*T).InternalType.Format(ctx)
	return ctx.String()
}

func (node *ReturnType) Format(ctx *FmtCtx) {
	node.Type.(*T).InternalType.Format(ctx)
}

type FunctionName struct {
	Name objName
}

type FunctionLanguage string

const (
	SQL    FunctionLanguage = "sql"
	PYTHON FunctionLanguage = "python"
)

type CreateFunction struct {
	statementImpl
	Replace    bool
	Name       *FunctionName
	Args       FunctionArgs
	ReturnType *ReturnType
	Language   string
	Import     bool
	Body       string
	Handler    string
}

type DropFunction struct {
	statementImpl
	Name *FunctionName
	Args FunctionArgs
}

func (node *FunctionName) Format(ctx *FmtCtx) {
	if node.Name.ExplicitCatalog {
		ctx.WriteString(string(node.Name.CatalogName))
		ctx.WriteByte('.')
	}
	if node.Name.ExplicitSchema {
		ctx.WriteString(string(node.Name.SchemaName))
		ctx.WriteByte('.')
	}
	ctx.WriteString(string(node.Name.ObjectName))
}

func (node *FunctionName) HasNoNameQualifier() bool {
	return !node.Name.ExplicitCatalog && !node.Name.ExplicitSchema
}

func (node *CreateFunction) Valid() error {
	node.Language = strings.ToLower(node.Language)
	switch node.Language {
	case string(SQL):
		if node.Import {
			return moerr.NewInvalidInputNoCtx("import")
		}
		return nil
	case string(PYTHON):
		return nil // TODO
	default:
		return moerr.NewInvalidArgNoCtx("function language", node.Language)
	}
}

func (node *CreateFunction) Format(ctx *FmtCtx) {
	// create or replace
	ctx.WriteString("create ")
	if node.Replace {
		ctx.WriteString("or replace ")
	}
	ctx.WriteString("function ")

	// func_name (args)
	node.Name.Format(ctx)
	ctx.WriteString(" (")
	for i, def := range node.Args {
		if i != 0 {
			ctx.WriteString(",")
			ctx.WriteByte(' ')
		}
		def.Format(ctx)
	}
	ctx.WriteString(")")

	// returns type
	ctx.WriteString(" returns ")
	node.ReturnType.Format(ctx)

	// language lang
	ctx.WriteString(" language ")
	ctx.WriteString(node.Language)

	// as 'body', or import 'body'
	if !node.Import {
		ctx.WriteString(" as '")
	} else {
		ctx.WriteString(" import '")
	}
	ctx.WriteString(node.Body)
	ctx.WriteString("'")

	// handler 'handler'
	if node.Handler != "" {
		ctx.WriteString(" handler '")
		ctx.WriteString(node.Handler)
		ctx.WriteString("'")
	}
}

func (node *DropFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("drop function ")
	node.Name.Format(ctx)
	ctx.WriteString(" (")

	for i, def := range node.Args {
		if i != 0 {
			ctx.WriteString(",")
			ctx.WriteByte(' ')
		}
		def.Format(ctx)
	}

	ctx.WriteString(")")
}

func NewFunctionArgDecl(n *UnresolvedName, t ResolvableTypeReference, d Expr) *FunctionArgDecl {
	return &FunctionArgDecl{
		Name:       n,
		Type:       t,
		DefaultVal: d,
	}
}

func NewFuncName(name Identifier, prefix ObjectNamePrefix) *FunctionName {
	return &FunctionName{
		Name: objName{
			ObjectName:       name,
			ObjectNamePrefix: prefix,
		},
	}
}

func NewReturnType(t ResolvableTypeReference) *ReturnType {
	return &ReturnType{
		Type: t,
	}
}

func (node *CreateFunction) GetStatementType() string { return "CreateFunction" }
func (node *CreateFunction) GetQueryType() string     { return QueryTypeDDL }

func (node *DropFunction) GetStatementType() string { return "DropFunction" }
func (node *DropFunction) GetQueryType() string     { return QueryTypeDDL }
