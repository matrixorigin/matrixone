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

// the VALUES clause
type ValuesClause struct {
	SelectStatement
	RowWord bool
	Rows    []Exprs
}

func (node *ValuesClause) Format(ctx *FmtCtx) {
	ctx.WriteString("values ")
	comma := ""
	for i := range node.Rows {
		ctx.WriteString(comma)
		if node.RowWord {
			ctx.WriteString("row")
		}
		ctx.WriteByte('(')
		node.Rows[i].Format(ctx)
		ctx.WriteByte(')')
		comma = ", "
	}
}

func NewValuesClause(r []Exprs) *ValuesClause {
	return &ValuesClause{
		Rows: r,
	}
}

// ValuesStatement the VALUES Statement
type ValuesStatement struct {
	statementImpl
	NodeFormatter
	Rows    []Exprs
	OrderBy OrderBy
	Limit   *Limit
}

func (node *ValuesStatement) Format(ctx *FmtCtx) {
	ctx.WriteString("values ")

	if len(node.Rows) > 0 {
		prefix := ""
		for _, row := range node.Rows {
			ctx.WriteString(prefix + "row(")
			comma := ""
			for i := range row {
				ctx.WriteString(comma)
				row[i].Format(ctx)
				comma = ", "
			}
			prefix = "), "
		}
		ctx.WriteString(")")
	}

	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		node.OrderBy.Format(ctx)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		node.Limit.Format(ctx)
	}
}
func (node *ValuesStatement) GetStatementType() string { return "Values" }
func (node *ValuesStatement) GetQueryType() string     { return QueryTypeDML }
