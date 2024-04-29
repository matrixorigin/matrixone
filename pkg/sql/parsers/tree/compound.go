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
	reuse.CreatePool[BeginCompound](
		func() *BeginCompound { return &BeginCompound{} },
		func(b *BeginCompound) { b.reset() },
		reuse.DefaultOptions[BeginCompound](),
		)

	reuse.CreatePool[EndCompound](
		func() *EndCompound { return &EndCompound{} },
		func(e *EndCompound) { e.reset() },
		reuse.DefaultOptions[EndCompound](),
		)

	reuse.CreatePool[CompoundStmt](
		func() *CompoundStmt { return &CompoundStmt{} },
		func(c *CompoundStmt) { c.reset() },
		reuse.DefaultOptions[CompoundStmt](),
		)
}

// Begin statement
type BeginCompound struct {
	statementImpl
}

type EndCompound struct {
	statementImpl
}

type CompoundStmt struct {
	statementImpl
	Stmts []Statement
}

func NewCompoundStmt(s []Statement) *CompoundStmt {
	return &CompoundStmt{
		Stmts: s,
	}
}

func (node *CompoundStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("begin ")
	for _, s := range node.Stmts {
		if s != nil {
			s.Format(ctx)
			ctx.WriteString("; ")
		}
	}
	ctx.WriteString("end")
}

func (node *CompoundStmt) Free() {
	reuse.Free[CompoundStmt](node, nil)
}

func (node *CompoundStmt) reset() {
	if node.Stmts != nil {
		for _, item := range node.Stmts {
			if item != nil {
				item.Free()
			}
		}
	}
	*node = CompoundStmt{}
}

func (node CompoundStmt) TypeName() string { return "tree.CompoundStmt"}

func (node *CompoundStmt) GetStatementType() string { return "compound" }

func (node *CompoundStmt) GetQueryType() string { return QueryTypeTCL }

func (node *BeginCompound) Format(ctx *FmtCtx) {
	ctx.WriteString("begin")
}

func (node *BeginCompound) Free() {
	reuse.Free[BeginCompound](node, nil)
}

func (node *BeginCompound) reset() {
	*node = BeginCompound{}
}

func (node BeginCompound) TypeName() string { return "tree.BeginCompound"}

func (node *BeginCompound) GetStatementType() string { return "begin" }

func (node *BeginCompound) GetQueryType() string { return QueryTypeTCL }

func (node *EndCompound) Format(ctx *FmtCtx) {
	ctx.WriteString("end")
}

func (node *EndCompound) Free() {
	reuse.Free[EndCompound](node, nil)
}

func (node *EndCompound) reset() {
	*node = EndCompound{}
}

func (node *EndCompound) GetStatementType() string { return "end" }

func (node *EndCompound) GetQueryType() string { return QueryTypeTCL }

func (node EndCompound) TypeName() string { return "tree.EndCompound"}