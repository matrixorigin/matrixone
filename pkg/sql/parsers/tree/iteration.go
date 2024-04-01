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
	reuse.CreatePool[RepeatStmt](
		func() *RepeatStmt { return &RepeatStmt{} },
		func(r *RepeatStmt) { r.reset() },
		reuse.DefaultOptions[RepeatStmt](),
	)

	reuse.CreatePool[WhileStmt](
		func() *WhileStmt { return &WhileStmt{} },
		func(w *WhileStmt) { w.reset() },
		reuse.DefaultOptions[WhileStmt](),
	)

	reuse.CreatePool[LoopStmt](
		func() *LoopStmt { return &LoopStmt{} },
		func(l *LoopStmt) { l.reset() },
		reuse.DefaultOptions[LoopStmt](),
	)

	reuse.CreatePool[IterateStmt](
		func() *IterateStmt { return &IterateStmt{} },
		func(i *IterateStmt) { i.reset() },
		reuse.DefaultOptions[IterateStmt](),
	)

	reuse.CreatePool[LeaveStmt](
		func() *LeaveStmt { return &LeaveStmt{} },
		func(l *LeaveStmt) { l.reset() },
		reuse.DefaultOptions[LeaveStmt](),
	)
}

type RepeatStmt struct {
	statementImpl
	Name Identifier
	Body []Statement
	Cond Expr
}

func NewRepeatStmt(name Identifier, body []Statement, cond Expr) *RepeatStmt {
	r := reuse.Alloc[RepeatStmt](nil)
	r.Name = name
	r.Body = body
	r.Cond = cond
	return r
}

func (node *RepeatStmt) reset() {
	if node.Body != nil {
		for _, item := range node.Body {
			item.Free()
		}
	}
	*node = RepeatStmt{}
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

func (node *RepeatStmt) Free() {
	reuse.Free[RepeatStmt](node, nil)
}

func (node *RepeatStmt) GetStatementType() string { return "Repeat Statement" }

func (node *RepeatStmt) GetQueryType() string { return QueryTypeTCL }

func (node RepeatStmt) TypeName() string { return "tree.RepeatStmt" }

type WhileStmt struct {
	statementImpl
	Name Identifier
	Cond Expr
	Body []Statement
}

func NewWhileStmt(name Identifier, cond Expr, body []Statement) *WhileStmt {
	w := reuse.Alloc[WhileStmt](nil)
	w.Name = name
	w.Cond = cond
	w.Body = body
	return w
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

func (node *WhileStmt) reset() {
	if node.Body != nil {
		for _, item := range node.Body {
			item.Free()
		}
	}
	*node = WhileStmt{}
}

func (node *WhileStmt) Free() {
	reuse.Free[WhileStmt](node, nil)
}

func (node *WhileStmt) GetStatementType() string { return "While Statement" }

func (node *WhileStmt) GetQueryType() string { return QueryTypeTCL }

func (node WhileStmt) TypeName() string { return "tree.WhileStmt" }

type LoopStmt struct {
	statementImpl
	Name Identifier
	Body []Statement
}

func NewLoopStmt(name Identifier, body []Statement) *LoopStmt {
	l := reuse.Alloc[LoopStmt](nil)
	l.Name = name
	l.Body = body
	return l
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

func (node *LoopStmt) reset() {
	if node.Body != nil {
		for _, item := range node.Body {
			item.Free()
		}
	}
	*node = LoopStmt{}
}

func (node *LoopStmt) Free() {
	reuse.Free[LoopStmt](node, nil)
}

func (node *LoopStmt) GetStatementType() string { return "Loop Statement" }

func (node *LoopStmt) GetQueryType() string { return QueryTypeTCL }

func (node LoopStmt) TypeName() string { return "tree.LoopStmt" }

type IterateStmt struct {
	statementImpl
	Name Identifier
}

func NewIterateStmt(name Identifier) *IterateStmt {
	i := reuse.Alloc[IterateStmt](nil)
	i.Name = name
	return i
}

func (node *IterateStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("iterate ")
	ctx.WriteString(string(node.Name))
}

func (node *IterateStmt) reset() {
	*node = IterateStmt{}
}

func (node *IterateStmt) Free() {
	reuse.Free[IterateStmt](node, nil)
}

func (node *IterateStmt) GetStatementType() string { return "Iterate Statement" }

func (node *IterateStmt) GetQueryType() string { return QueryTypeTCL }

func (node IterateStmt) TypeName() string { return "tree.IterateStmt" }

type LeaveStmt struct {
	statementImpl
	Name Identifier
}

func NewLeaveStmt(name Identifier) *LeaveStmt {
	i := reuse.Alloc[LeaveStmt](nil)
	i.Name = name
	return i
}

func (node *LeaveStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("leave ")
	ctx.WriteString(string(node.Name))
}

func (node *LeaveStmt) reset() {
	*node = LeaveStmt{}
}

func (node *LeaveStmt) Free() {
	reuse.Free[LeaveStmt](node, nil)
}

func (node *LeaveStmt) GetStatementType() string { return "Leave Statement" }

func (node *LeaveStmt) GetQueryType() string { return QueryTypeTCL }

func (node LeaveStmt) TypeName() string { return "tree.LeaveStmt" }
