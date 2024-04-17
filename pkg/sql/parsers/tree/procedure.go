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
	reuse.CreatePool[DropProcedure](
		func() *DropProcedure { return &DropProcedure{} },
		func(d *DropProcedure) { d.reset() },
		reuse.DefaultOptions[DropProcedure](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ProcedureArgDecl](
		func() *ProcedureArgDecl { return &ProcedureArgDecl{} },
		func(p *ProcedureArgDecl) { p.reset() },
		reuse.DefaultOptions[ProcedureArgDecl](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ProcedureName](
		func() *ProcedureName { return &ProcedureName{} },
		func(p *ProcedureName) { p.reset() },
		reuse.DefaultOptions[ProcedureName](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateProcedure](
		func() *CreateProcedure { return &CreateProcedure{} },
		func(c *CreateProcedure) { c.reset() },
		reuse.DefaultOptions[CreateProcedure](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CallStmt](
		func() *CallStmt { return &CallStmt{} },
		func(c *CallStmt) { c.reset() },
		reuse.DefaultOptions[CallStmt](), //.
	) //WithEnableChecker()
}

type InOutArgType int

const (
	TYPE_IN InOutArgType = iota
	TYPE_OUT
	TYPE_INOUT
)

type ProcedureArgType struct {
	Type InOutArgType
}

type ProcedureArg interface {
	NodeFormatter
	GetName(ctx *FmtCtx) string
	GetType() int
}

type ProcedureArgImpl struct {
	ProcedureArg
}

// container holding list of arguments in udf
type ProcedureArgs []ProcedureArg

type ProcedureArgDecl struct {
	ProcedureArgImpl
	Name      *UnresolvedName
	Type      ResolvableTypeReference
	InOutType InOutArgType
}

func (node *ProcedureArgDecl) Format(ctx *FmtCtx) {
	// in out type
	switch node.InOutType {
	case TYPE_IN:
		ctx.WriteString("in ")
	case TYPE_OUT:
		ctx.WriteString("out ")
	case TYPE_INOUT:
		ctx.WriteString("inout ")
	}
	if node.Name != nil {
		node.Name.Format(ctx)
		ctx.WriteByte(' ')
	}
	node.Type.(*T).InternalType.Format(ctx)
}

func (node *ProcedureArgDecl) GetName(ctx *FmtCtx) string {
	node.Name.Format(ctx)
	return ctx.String()
}

func (node *ProcedureArgDecl) GetType() int {
	return int(node.InOutType)
}

func (node *ProcedureArgDecl) reset() {
	// if node.Name != nil {
	// node.Name.Free()
	// }
	*node = ProcedureArgDecl{}
}

func (node ProcedureArgDecl) TypeName() string { return "tree.ProcedureArgDecl" }

func (node *ProcedureArgDecl) Free() {
	reuse.Free[ProcedureArgDecl](node, nil)
}

type ProcedureArgForMarshal struct {
	Name      *UnresolvedName
	Type      ResolvableTypeReference
	InOutType InOutArgType
}

type ProcedureName struct {
	Name objName
}

func (node *ProcedureName) Format(ctx *FmtCtx) {
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

func (node *ProcedureName) HasNoNameQualifier() bool {
	return !node.Name.ExplicitCatalog && !node.Name.ExplicitSchema
}

func (node *ProcedureName) reset() {
	*node = ProcedureName{}
}

func (node ProcedureName) TypeName() string { return "tree.ProcedureName" }

func (node *ProcedureName) Free() {
	reuse.Free[ProcedureName](node, nil)
}

func NewProcedureName(name Identifier, prefix ObjectNamePrefix) *ProcedureName {
	return &ProcedureName{
		Name: objName{
			ObjectName:       name,
			ObjectNamePrefix: prefix,
		},
	}
}

func NewProcedureArgDecl(f InOutArgType, n *UnresolvedName, t ResolvableTypeReference) *ProcedureArgDecl {
	return &ProcedureArgDecl{
		Name:      n,
		Type:      t,
		InOutType: f,
	}
}

type CreateProcedure struct {
	statementImpl
	Name *ProcedureName
	Args ProcedureArgs
	Body string
}

func NewCreateProcedure(n *ProcedureName, a ProcedureArgs, b string) *CreateProcedure {
	createProcedure := reuse.Alloc[CreateProcedure](nil)
	createProcedure.Name = n
	createProcedure.Args = a
	createProcedure.Body = b
	return createProcedure
}

func (node *CreateProcedure) Format(ctx *FmtCtx) {
	ctx.WriteString("create procedure ")

	node.Name.Format(ctx)

	ctx.WriteString(" (")
	for i, def := range node.Args {
		if i != 0 {
			ctx.WriteString(",")
			ctx.WriteByte(' ')
		}
		def.Format(ctx)
	}
	ctx.WriteString(") '")

	ctx.WriteString(node.Body)
	ctx.WriteString("'")
}

func (node *CreateProcedure) GetStatementType() string { return "Create Procedure" }

func (node *CreateProcedure) GetQueryType() string { return QueryTypeOth }

func (node *CreateProcedure) reset() {
	if node.Name != nil {
		node.Name.Free()
	}
	*node = CreateProcedure{}
}

func (node CreateProcedure) TypeName() string { return "tree.CreateProcedure" }

func (node *CreateProcedure) Free() {
	reuse.Free[CreateProcedure](node, nil)
}

type DropProcedure struct {
	statementImpl
	Name     *ProcedureName
	IfExists bool
}

func (node *DropProcedure) Free() {
	reuse.Free[DropProcedure](node, nil)
}

func (node DropProcedure) TypeName() string { return "tree.DropProcedure" }

func (node *DropProcedure) reset() {
	if node.Name != nil {
		node.Name.Free()
	}
	*node = DropProcedure{}
}

func (node *DropProcedure) Format(ctx *FmtCtx) {
	ctx.WriteString("drop procedure ")
	if node.IfExists {
		ctx.WriteString("if exists ")
	}
	node.Name.Format(ctx)
}

func (node *DropProcedure) GetStatementType() string { return "Create Procedure" }

func (node *DropProcedure) GetQueryType() string { return QueryTypeOth }

func NewDropProcedure(n *ProcedureName, i bool) *DropProcedure {
	dropProcedure := reuse.Alloc[DropProcedure](nil)
	dropProcedure.Name = n
	dropProcedure.IfExists = i
	return dropProcedure
}

type CallStmt struct {
	statementImpl
	Name *ProcedureName
	Args Exprs
}

func (node *CallStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("call ")
	node.Name.Format(ctx)
	ctx.WriteString("(")
	if len(node.Args) != 0 {
		node.Args.Format(ctx)
	}
	ctx.WriteString(")")
}

func (node *CallStmt) GetStatementType() string { return "Call" }

func (node *CallStmt) GetQueryType() string { return QueryTypeOth }

func (node *CallStmt) reset() {
	if node.Name != nil {
		node.Name.Free()
	}
	*node = CallStmt{}
}

func (node CallStmt) TypeName() string { return "tree.CallStmt" }

func (node *CallStmt) Free() {
	reuse.Free[CallStmt](node, nil)
}
