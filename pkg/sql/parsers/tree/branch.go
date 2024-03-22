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
	reuse.CreatePool[ElseIfStmt](
		func() *ElseIfStmt { return &ElseIfStmt{} },
		func(e *ElseIfStmt) { e.reset() },
		reuse.DefaultOptions[ElseIfStmt](),
	)

	reuse.CreatePool[IfStmt](
		func() *IfStmt { return &IfStmt{} },
		func(i *IfStmt) { i.reset() },
		reuse.DefaultOptions[IfStmt](),
	)

	reuse.CreatePool[WhenStmt](
		func() *WhenStmt { return &WhenStmt{} },
		func(w *WhenStmt) { w.reset() },
		reuse.DefaultOptions[WhenStmt](),
	)

	reuse.CreatePool[CaseStmt](
		func() *CaseStmt { return &CaseStmt{} },
		func(c *CaseStmt) { c.reset() },
		reuse.DefaultOptions[CaseStmt](),
	)
}

type ElseIfStmt struct {
	statementImpl
	Cond Expr
	Body []Statement
}

func NewElseIfStmt(cond Expr, body []Statement) *ElseIfStmt {
	e := reuse.Alloc[ElseIfStmt](nil)
	e.Cond = cond
	e.Body = body
	return e
}

func (node *ElseIfStmt) reset() {
	if node.Body != nil {
		for _, item := range node.Body {
			item.Free()
		}
	}
	if node.Cond != nil {
		// node.Cond.Free()
	}
	*node = ElseIfStmt{}
}

func (node *ElseIfStmt) Free() {
	reuse.Free[ElseIfStmt](node, nil)
}

func (node *ElseIfStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("elseif ")
	node.Cond.Format(ctx)
	ctx.WriteString(" then ")
	for _, s := range node.Body {
		if s != nil {
			s.Format(ctx)
			ctx.WriteString("; ")
		}
	}
}
func (node *ElseIfStmt) GetStatementType() string { return "ElseIf Statement" }

func (node *ElseIfStmt) GetQueryType() string { return QueryTypeTCL }

func (node ElseIfStmt) TypeName() string { return "tree.ElseIfStmt" }

type IfStmt struct {
	statementImpl
	Cond  Expr
	Body  []Statement
	Elifs []*ElseIfStmt
	Else  []Statement
}

func NewIfStmt(cond Expr, body []Statement, elifs []*ElseIfStmt, elseStmt []Statement) *IfStmt {
	i := reuse.Alloc[IfStmt](nil)
	i.Cond = cond
	i.Body = body
	i.Elifs = elifs
	i.Else = elseStmt
	return i
}

func (node *IfStmt) reset() {
	if node.Body != nil {
		for _, item := range node.Body {
			item.Free()
		}
	}
	if node.Elifs != nil {
		for _, item := range node.Elifs {
			item.Free()
		}
	}
	if node.Else != nil {
		for _, item := range node.Else {
			item.Free()
		}
	}
	*node = IfStmt{}
}

func (node *IfStmt) Free() {
	reuse.Free[IfStmt](node, nil)
}

func (node *IfStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("if ")
	node.Cond.Format(ctx)
	ctx.WriteString(" then ")
	for _, s := range node.Body {
		if s != nil {
			s.Format(ctx)
			ctx.WriteString("; ")
		}
	}
	if node.Elifs != nil {
		for _, elif := range node.Elifs {
			elif.Format(ctx)
		}
	}
	if len(node.Else) != 0 {
		ctx.WriteString("else ")
		for _, s := range node.Else {
			if s != nil {
				s.Format(ctx)
				ctx.WriteString("; ")
			}
		}
	}
	ctx.WriteString("end if")
}

func (node *IfStmt) GetStatementType() string { return "If Statement" }

func (node *IfStmt) GetQueryType() string { return QueryTypeTCL }

func (node IfStmt) TypeName() string { return "tree.IfStmt" }

type WhenStmt struct {
	statementImpl
	Cond Expr
	Body []Statement
}

func NewWhenStmt(cond Expr, body []Statement) *WhenStmt {
	w := reuse.Alloc[WhenStmt](nil)
	w.Cond = cond
	w.Body = body
	return w
}

func (node *WhenStmt) reset() {
	if node.Body != nil {
		for _, item := range node.Body {
			item.Free()
		}
	}
	*node = WhenStmt{}
}

func (node *WhenStmt) Free() {
	reuse.Free[WhenStmt](node, nil)
}

func (node *WhenStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("when ")
	node.Cond.Format(ctx)
	ctx.WriteString(" then ")
	for _, s := range node.Body {
		if s != nil {
			s.Format(ctx)
			ctx.WriteString("; ")
		}
	}
}
func (node *WhenStmt) GetStatementType() string { return "When Statement" }

func (node *WhenStmt) GetQueryType() string { return QueryTypeTCL }

func (node WhenStmt) TypeName() string { return "tree.WhenStmt" }

type CaseStmt struct {
	statementImpl
	Expr  Expr
	Whens []*WhenStmt
	Else  []Statement
}

func NewCaseStmt(expr Expr, whens []*WhenStmt, elseStmt []Statement) *CaseStmt {
	c := reuse.Alloc[CaseStmt](nil)
	c.Expr = expr
	c.Whens = whens
	c.Else = elseStmt
	return c
}

func (node *CaseStmt) reset() {
	if node.Whens != nil {
		for _, item := range node.Whens {
			item.Free()
		}
	}
	if node.Else != nil {
		for _, item := range node.Else {
			item.Free()
		}
	}
	*node = CaseStmt{}
}

func (node *CaseStmt) Free() {
	reuse.Free[CaseStmt](node, nil)
}

func (node *CaseStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("case ")
	node.Expr.Format(ctx)
	ctx.WriteByte(' ')
	for _, w := range node.Whens {
		w.Format(ctx)
	}
	if node.Else != nil {
		ctx.WriteString("else ")
		for _, s := range node.Else {
			if s != nil {
				s.Format(ctx)
				ctx.WriteString("; ")
			}
		}
	}
	ctx.WriteString("end case")
}

func (node *CaseStmt) GetStatementType() string { return "Case Statement" }

func (node *CaseStmt) GetQueryType() string { return QueryTypeTCL }

func (node CaseStmt) TypeName() string { return "tree.CaseStmt" }
