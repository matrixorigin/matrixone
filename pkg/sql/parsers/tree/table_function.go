// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

type TableFunction struct {
	statementImpl
	Func       *FuncExpr
	SelectStmt *Select
	JSONTable  *JSONTable
}

func (t *TableFunction) Format(ctx *FmtCtx) {
	if t.JSONTable != nil {
		ctx.WriteString("json_table(")
		sourceCtx := *ctx
		sourceCtx.quoteString = false
		sourceCtx.singleQuoteString = true
		t.Func.Exprs[0].Format(&sourceCtx)
		ctx.WriteString(", ")
		formatSQLTextLiteral(ctx, t.JSONTable.Path)
		formatJSONTableColumns(ctx, t.JSONTable.Columns)
		ctx.WriteByte(')')
		return
	}
	if t.Func != nil {
		t.Func.Format(ctx)
	}
}

func (t TableFunction) Id() string {
	return t.Func.Func.FunctionReference.(*UnresolvedName).ColName()
}

func (t *TableFunction) GetStatementType() string { return "Table Function" }
func (t *TableFunction) GetQueryType() string     { return QueryTypeOth }

// JSONTable keeps the column grammar out of the ordinary function arguments.
// The source remains in Func.Exprs so existing expression visitors see it.
type JSONTable struct {
	Path    string
	Columns []*JSONTableColumn
}

type JSONTableResponse struct {
	Action  string
	Default string
}

type JSONTableColumn struct {
	Name     string
	Kind     string
	Type     *T
	Path     string
	OnEmpty  *JSONTableResponse
	OnError  *JSONTableResponse
	Children []*JSONTableColumn
	// Charset is nil when omitted; an explicitly empty name still needs validation.
	Charset *string
	// ReversePolicies requires a parse/prepare-phase diagnostic, not a runtime
	// warning. Retain it so a core-only binder can fail closed until that owner
	// is implemented rather than silently canonicalizing deprecated syntax.
	ReversePolicies bool
}

func formatJSONTableColumns(ctx *FmtCtx, columns []*JSONTableColumn) {
	ctx.WriteString(" columns(")
	for i, column := range columns {
		if i > 0 {
			ctx.WriteString(", ")
		}
		if column.Kind == "nested" {
			ctx.WriteString("nested path ")
			formatSQLTextLiteral(ctx, column.Path)
			formatJSONTableColumns(ctx, column.Children)
			continue
		}
		name := Identifier(column.Name)
		nameCtx := *ctx
		nameCtx.quoteIdentifier = true
		name.Format(&nameCtx)
		if column.Kind == "ordinality" {
			ctx.WriteString(" for ordinality")
			continue
		}
		ctx.WriteByte(' ')
		column.Type.InternalType.Format(ctx)
		if column.Charset != nil {
			ctx.WriteString(" character set ")
			formatSQLTextLiteral(ctx, *column.Charset)
		}
		if column.Kind == "exists" {
			ctx.WriteString(" exists")
		}
		ctx.WriteString(" path ")
		formatSQLTextLiteral(ctx, column.Path)
		for _, response := range []struct {
			value  *JSONTableResponse
			clause string
		}{{column.OnEmpty, " empty"}, {column.OnError, " error"}} {
			if response.value != nil {
				ctx.WriteByte(' ')
				ctx.WriteString(response.value.Action)
				if response.value.Action == "default" {
					ctx.WriteByte(' ')
					formatSQLTextLiteral(ctx, response.value.Default)
				}
				ctx.WriteString(" on" + response.clause)
			}
		}
	}
	ctx.WriteByte(')')
}
