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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[DropFunction](
		func() *DropFunction { return &DropFunction{} },
		func(d *DropFunction) { d.reset() },
		reuse.DefaultOptions[DropFunction](), //.
	) //WithEnableChecker()

	reuse.CreatePool[FunctionArgDecl](
		func() *FunctionArgDecl { return &FunctionArgDecl{} },
		func(f *FunctionArgDecl) { f.reset() },
		reuse.DefaultOptions[FunctionArgDecl](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ReturnType](
		func() *ReturnType { return &ReturnType{} },
		func(r *ReturnType) { r.reset() },
		reuse.DefaultOptions[ReturnType](), //.
	) //WithEnableChecker()

	reuse.CreatePool[FunctionName](
		func() *FunctionName { return &FunctionName{} },
		func(f *FunctionName) { f.reset() },
		reuse.DefaultOptions[FunctionName](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateFunction](
		func() *CreateFunction { return &CreateFunction{} },
		func(c *CreateFunction) { c.reset() },
		reuse.DefaultOptions[CreateFunction](), //.
	) //WithEnableChecker()

}

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

func (node FunctionArgDecl) TypeName() string { return "tree.FunctionArgDecl" }

func (node *FunctionArgDecl) reset() {
	// if node.Name != nil {
	// node.Name.Free()
	// }
	*node = FunctionArgDecl{}
}

func (node *FunctionArgDecl) Free() {
	reuse.Free[FunctionArgDecl](node, nil)
}

type ReturnType struct {
	Type ResolvableTypeReference
}

func (node *ReturnType) Format(ctx *FmtCtx) {
	node.Type.(*T).InternalType.Format(ctx)
}

func (node ReturnType) TypeName() string { return "tree.ReturnType" }

func (node *ReturnType) reset() {
	*node = ReturnType{}
}

func (node *ReturnType) Free() {
	reuse.Free[ReturnType](node, nil)
}

type FunctionName struct {
	Name objName
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

func (node FunctionName) TypeName() string { return "tree.FunctionName" }

func (node *FunctionName) reset() {
	*node = FunctionName{}
}

func (node *FunctionName) Free() {
	reuse.Free[FunctionName](node, nil)
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

func NewCreateFunction(replace bool, name *FunctionName, args FunctionArgs, returnType *ReturnType, lang string, import_ bool, body string, handler string) *CreateFunction {
	createFunction := reuse.Alloc[CreateFunction](nil)
	createFunction.Replace = replace
	createFunction.Name = name
	createFunction.Args = args
	createFunction.ReturnType = returnType
	createFunction.Language = lang
	createFunction.Import = import_
	createFunction.Body = body
	createFunction.Handler = handler
	return createFunction
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
		return nil
	default:
		return moerr.NewInvalidArgNoCtx("function language", node.Language)
	}
}

func (node *CreateFunction) Format(ctx *FmtCtx) {
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

func (node *CreateFunction) GetStatementType() string { return "CreateFunction" }
func (node *CreateFunction) GetQueryType() string     { return QueryTypeDDL }

func (node CreateFunction) TypeName() string { return "tree.CreateFunction" }

func (node *CreateFunction) reset() {
	if node.Name != nil {
		node.Name.Free()
	}
	if node.ReturnType != nil {
		node.ReturnType.Free()
	}
	*node = CreateFunction{}
}

func (node *CreateFunction) Free() {
	reuse.Free[CreateFunction](node, nil)
}

type DropFunction struct {
	statementImpl
	Name *FunctionName
	Args FunctionArgs
}

func (node DropFunction) TypeName() string { return "tree.DropFunction" }

func (node *DropFunction) Free() {
	reuse.Free[DropFunction](node, nil)
}

func (node *DropFunction) reset() {
	if node.Name != nil {
		node.Name.Free()
	}
	*node = DropFunction{}
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

func (node *DropFunction) GetStatementType() string { return "DropFunction" }
func (node *DropFunction) GetQueryType() string     { return QueryTypeDDL }
func NewDropFunction(name *FunctionName, args FunctionArgs) *DropFunction {
	dropFunction := reuse.Alloc[DropFunction](nil)
	dropFunction.Name = name
	dropFunction.Args = args
	return dropFunction
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
