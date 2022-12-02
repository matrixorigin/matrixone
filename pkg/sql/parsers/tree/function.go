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

type FunctionArg interface {
	NodeFormatter
	Expr
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

func (node *ReturnType) Format(ctx *FmtCtx) {
	node.Type.(*T).InternalType.Format(ctx)
}

type FunctionName struct {
	Name Identifier
}

type CreateFunction struct {
	statementImpl
	Name       *FunctionName
	Args       FunctionArgs
	ReturnType *ReturnType
	Body       string
	Language   string
}

type DropFunction struct {
	statementImpl
	Name *FunctionName
	Args FunctionArgs
}

func (node *FunctionName) Format(ctx *FmtCtx) {
	node.Name.Format(ctx)
}

func (node *CreateFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("create function ")

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
	ctx.WriteString(" returns ")

	node.ReturnType.Format(ctx)

	ctx.WriteString(" language ")
	ctx.WriteString(node.Language)

	ctx.WriteString(" as '")

	ctx.WriteString(node.Body)
	ctx.WriteString("'")
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

func NewFuncName(name Identifier) *FunctionName {
	return &FunctionName{
		Name: name,
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
