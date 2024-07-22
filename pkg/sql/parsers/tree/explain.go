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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[ExplainStmt](
		func() *ExplainStmt { return &ExplainStmt{} },
		func(e *ExplainStmt) { e.reset() },
		reuse.DefaultOptions[ExplainStmt](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ExplainAnalyze](
		func() *ExplainAnalyze { return &ExplainAnalyze{} },
		func(e *ExplainAnalyze) { e.reset() },
		reuse.DefaultOptions[ExplainAnalyze](), //.
	) //WithEnableChecker()

	reuse.CreatePool[ExplainFor](
		func() *ExplainFor { return &ExplainFor{} },
		func(e *ExplainFor) { e.reset() },
		reuse.DefaultOptions[ExplainFor](), //.
	) //WithEnableChecker()

}

type Explain interface {
	Statement
}

type explainImpl struct {
	Explain
	Statement Statement
	Format    string
	Options   []OptionElem
}

func (e *explainImpl) Free() {
}

// EXPLAIN stmt statement
type ExplainStmt struct {
	explainImpl
}

func (node *ExplainStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("explain")
	if node.Options != nil && len(node.Options) > 0 {
		ctx.WriteString(" (")
		var temp string
		for _, v := range node.Options {
			temp += v.Name
			if v.Value != "NULL" {
				temp += " " + v.Value
			}
			temp += ","
		}
		ctx.WriteString(temp[:len(temp)-1] + ")")
	}

	stmt := node.explainImpl.Statement
	switch st := stmt.(type) {
	case *ShowColumns:
		if st.Table != nil {
			ctx.WriteByte(' ')
			st.Table.ToTableName().Format(ctx)
		}
		if st.ColName != nil {
			ctx.WriteByte(' ')
			st.ColName.Format(ctx)
		}
	default:
		if stmt != nil {
			ctx.WriteByte(' ')
			stmt.Format(ctx)
		}
	}
}

func (node *ExplainStmt) GetStatementType() string { return "Explain" }
func (node *ExplainStmt) GetQueryType() string     { return QueryTypeOth }

// EXPLAIN FOR CONNECTION statement

func (node *ExplainStmt) Free() {
	reuse.Free[ExplainStmt](node, nil)
}

func (node *ExplainStmt) reset() {
	*node = ExplainStmt{}
}

func (node ExplainStmt) TypeName() string { return "tree.ExplainStmt" }

func NewExplainStmt(stmt Statement, f string) *ExplainStmt {
	ex := reuse.Alloc[ExplainStmt](nil)
	ex.explainImpl.Statement = stmt
	ex.explainImpl.Format = f
	return ex
}

// EXPLAIN ANALYZE statement
type ExplainAnalyze struct {
	explainImpl
}

func (node *ExplainAnalyze) Format(ctx *FmtCtx) {
	ctx.WriteString("explain")
	if node.Options != nil && len(node.Options) > 0 {
		ctx.WriteString(" (")
		var temp string
		for _, v := range node.Options {
			temp += v.Name
			if v.Value != "NULL" {
				temp += " " + v.Value
			}
			temp += ","
		}
		ctx.WriteString(temp[:len(temp)-1] + ")")
	}

	stmt := node.explainImpl.Statement
	switch st := stmt.(type) {
	case *ShowColumns:
		if st.Table != nil {
			ctx.WriteByte(' ')
			st.Table.ToTableName().Format(ctx)
		}
		if st.ColName != nil {
			ctx.WriteByte(' ')
			st.ColName.Format(ctx)
		}
	default:
		if stmt != nil {
			ctx.WriteByte(' ')
			stmt.Format(ctx)
		}
	}
}

func (node *ExplainAnalyze) GetStatementType() string { return "Explain Analyze" }
func (node *ExplainAnalyze) GetQueryType() string     { return QueryTypeOth }

func (node *ExplainAnalyze) Free() {
	reuse.Free[ExplainAnalyze](node, nil)
}

func (node *ExplainAnalyze) reset() {
	*node = ExplainAnalyze{}
}

func (node ExplainAnalyze) TypeName() string { return "tree.ExplainAnalyze" }

func NewExplainAnalyze(stmt Statement, f string) *ExplainAnalyze {
	ex := reuse.Alloc[ExplainAnalyze](nil)
	ex.explainImpl.Statement = stmt
	ex.explainImpl.Format = f
	return ex
}

// EXPLAIN FOR CONNECTION statement
type ExplainFor struct {
	explainImpl
	ID uint64
}

func (node *ExplainFor) Format(ctx *FmtCtx) {
	ctx.WriteString("explain format = ")
	ctx.WriteString(node.explainImpl.Format)
	ctx.WriteString(" for connection ")
	ctx.WriteString(strconv.FormatInt(int64(node.ID), 10))
}

func (node *ExplainFor) GetStatementType() string { return "Explain Format" }
func (node *ExplainFor) GetQueryType() string     { return QueryTypeOth }

func (node *ExplainFor) Free() {
	reuse.Free[ExplainFor](node, nil)
}

func (node *ExplainFor) reset() {
	*node = ExplainFor{}
}

func (node ExplainFor) TypeName() string { return "tree.ExplainFor" }

func NewExplainFor(f string, id uint64) *ExplainFor {
	ex := reuse.Alloc[ExplainFor](nil)
	ex.explainImpl = explainImpl{Statement: nil, Format: f}
	ex.ID = id
	return ex
}

type OptionElem struct {
	Name  string
	Value string
}

func MakeOptionElem(name string, value string) OptionElem {
	return OptionElem{
		Name:  name,
		Value: value,
	}
}

func MakeOptions(elem OptionElem) []OptionElem {
	var options = make([]OptionElem, 1)
	options[0] = elem
	return options
}

func IsContainAnalyze(options []OptionElem) bool {
	if len(options) > 0 {
		for _, option := range options {
			if strings.EqualFold(option.Name, "analyze") && strings.EqualFold(option.Value, "true") {
				return true
			}
		}
	}
	return false
}
