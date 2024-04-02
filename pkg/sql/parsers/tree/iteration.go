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

type RepeatStmt struct {
	statementImpl
	Name Identifier
	Body []Statement
	Cond Expr
}

func (node *RepeatStmt) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString(string(node.Name))
		ctx.WriteString(": ")
	}
	ctx.WriteString("repeat ")
	for _, s := range node.Body {
		if s != nil {
			s.Format(ctx)
			ctx.WriteString("; ")
		}
	}
	ctx.WriteString("until ")
	node.Cond.Format(ctx)
	ctx.WriteString(" end repeat")
	if node.Name != "" {
		ctx.WriteString(string(node.Name))
	}
}

func (node *RepeatStmt) GetStatementType() string { return "Repeat Statement" }
func (node *RepeatStmt) GetQueryType() string     { return QueryTypeTCL }

type WhileStmt struct {
	statementImpl
	Name Identifier
	Cond Expr
	Body []Statement
}

func (node *WhileStmt) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString(string(node.Name))
		ctx.WriteString(": ")
	}
	ctx.WriteString("while ")
	node.Cond.Format(ctx)
	ctx.WriteString(" do ")
	for _, s := range node.Body {
		if s != nil {
			s.Format(ctx)
			ctx.WriteString("; ")
		}
	}
	ctx.WriteString("end while")
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(string(node.Name))
	}
}

func (node *WhileStmt) GetStatementType() string { return "While Statement" }
func (node *WhileStmt) GetQueryType() string     { return QueryTypeTCL }

type LoopStmt struct {
	statementImpl
	Name Identifier
	Body []Statement
}

func (node *LoopStmt) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString(string(node.Name))
		ctx.WriteString(": ")
	}
	ctx.WriteString("loop ")
	for _, s := range node.Body {
		if s != nil {
			s.Format(ctx)
			ctx.WriteString("; ")
		}
	}
	ctx.WriteString("end loop")
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(string(node.Name))
	}
}

func (node *LoopStmt) GetStatementType() string { return "Loop Statement" }
func (node *LoopStmt) GetQueryType() string     { return QueryTypeTCL }

type IterateStmt struct {
	statementImpl
	Name Identifier
}

func (node *IterateStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("iterate ")
	ctx.WriteString(string(node.Name))
}

func (node *IterateStmt) GetStatementType() string { return "Iterate Statement" }
func (node *IterateStmt) GetQueryType() string     { return QueryTypeTCL }

type LeaveStmt struct {
	statementImpl
	Name Identifier
}

func (node *LeaveStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("leave ")
	ctx.WriteString(string(node.Name))
}

func (node *LeaveStmt) GetStatementType() string { return "Leave Statement" }
func (node *LeaveStmt) GetQueryType() string     { return QueryTypeTCL }
