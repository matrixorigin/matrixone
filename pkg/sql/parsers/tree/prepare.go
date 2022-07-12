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

type Prepare struct {
	Statement
	Name Identifier
	Stmt Statement
}

type PrepareString struct {
	Statement
	Name Identifier
	Sql  string
}

func (node *Prepare) Format(ctx *FmtCtx) {
	ctx.WriteString("prepare ")
	node.Name.Format(ctx)
	ctx.WriteString(" from ")
	node.Stmt.Format(ctx)
}

func (node *PrepareString) Format(ctx *FmtCtx) {
	ctx.WriteString("prepare ")
	node.Name.Format(ctx)
	ctx.WriteString(" from ")
	ctx.WriteString(node.Sql)
}

func NewPrepare(name Identifier, statement Statement) *Prepare {
	return &Prepare{
		Name: name,
		Stmt: statement,
	}
}

func NewPrepareFromStr(name Identifier, sql string) *PrepareString {
	return &PrepareString{
		Name: name,
		Sql:  sql,
	}
}
