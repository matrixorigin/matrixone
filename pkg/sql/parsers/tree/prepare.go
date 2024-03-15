// Copyright 2021 - 2022 Matrix Origin
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
	reuse.CreatePool[PrepareStmt](
		func() *PrepareStmt { return &PrepareStmt{} },
		func(p *PrepareStmt) { p.reset() },
		reuse.DefaultOptions[PrepareStmt](), //.
	) //WithEnableChecker()

	reuse.CreatePool[PrepareString](
		func() *PrepareString { return &PrepareString{} },
		func(p *PrepareString) { p.reset() },
		reuse.DefaultOptions[PrepareString](), //.
	) //WithEnableChecker()
}

type Prepare interface {
	Statement
}

type prepareImpl struct {
	Prepare
	Format string
}

func (e *prepareImpl) Free() {
}

type PrepareStmt struct {
	prepareImpl
	Name Identifier
	Stmt Statement
}

func (node *PrepareStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("prepare ")
	node.Name.Format(ctx)
	ctx.WriteString(" from ")
	node.Stmt.Format(ctx)
}

func (node *PrepareStmt) GetStatementType() string { return "Prepare" }
func (node *PrepareStmt) GetQueryType() string     { return QueryTypeOth }

func (node *PrepareStmt) Free() {
	reuse.Free[PrepareStmt](node, nil)
}

func (node *PrepareStmt) reset() {
	*node = PrepareStmt{}
}

func (node PrepareStmt) TypeName() string { return "tree.PrepareStmt" }

type PrepareString struct {
	prepareImpl
	Name Identifier
	Sql  string
}

func (node *PrepareString) Format(ctx *FmtCtx) {
	ctx.WriteString("prepare ")
	node.Name.Format(ctx)
	ctx.WriteString(" from ")
	ctx.WriteString(node.Sql)
}

func (node *PrepareString) GetStatementType() string { return "Prepare" }
func (node *PrepareString) GetQueryType() string     { return QueryTypeOth }

func (node *PrepareString) Free() {
	reuse.Free[PrepareString](node, nil)
}

func (node *PrepareString) reset() {
	*node = PrepareString{}
}

func (node PrepareString) TypeName() string { return "tree.PrepareString" }

func NewPrepareStmt(name Identifier, statement Statement) *PrepareStmt {
	preparestmt := reuse.Alloc[PrepareStmt](nil)
	preparestmt.Name = name
	preparestmt.Stmt = statement
	return preparestmt
}

func NewPrepareString(name Identifier, sql string) *PrepareString {
	preparestmt := reuse.Alloc[PrepareString](nil)
	preparestmt.Name = name
	preparestmt.Sql = sql
	return preparestmt
}
