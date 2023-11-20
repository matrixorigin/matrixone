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

type CreateDynamicTable struct {
	statementImpl
	Replace          bool
	Source           bool
	IfNotExists      bool
	DynamicTableName *TableName
	Defs             TableDefs
	ColNames         IdentifierList
	AsSource         *Select
	Options          []TableOption
}

func (node *CreateDynamicTable) Format(ctx *FmtCtx) {
	ctx.WriteString("create")
	if node.Replace {
		ctx.WriteString(" or replace")
	}
	if node.Defs != nil {
		if node.Source {
			ctx.WriteString(" source")
		}
		ctx.WriteString(" dynamic table")
		if node.IfNotExists {
			ctx.WriteString(" if not exists")
		}
		ctx.WriteByte(' ')
		node.DynamicTableName.Format(ctx)

		ctx.WriteString(" (")
		for i, def := range node.Defs {
			if i != 0 {
				ctx.WriteString(",")
				ctx.WriteByte(' ')
			}
			def.Format(ctx)
		}
		ctx.WriteByte(')')

		if node.Options != nil {
			prefix := " with ("
			for _, t := range node.Options {
				ctx.WriteString(prefix)
				t.Format(ctx)
				prefix = ", "
			}
			ctx.WriteByte(')')
		}
		return
	}
	ctx.WriteString(" dynamic table")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	ctx.WriteByte(' ')
	node.DynamicTableName.Format(ctx)
	if node.Options != nil {
		prefix := " with ("
		for _, t := range node.Options {
			ctx.WriteString(prefix)
			t.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	ctx.WriteString(" as ")
	node.AsSource.Format(ctx)
}

func (node *CreateDynamicTable) GetStatementType() string { return "Create Dynamic Table" }
func (node *CreateDynamicTable) GetQueryType() string     { return QueryTypeDDL }

type CreateDynamicTableWithOption struct {
	createOptionImpl
	Key Identifier
	Val Expr
}

func (node *CreateDynamicTableWithOption) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Key))
	ctx.WriteString(" = ")
	node.Val.Format(ctx)
}

type DTAttributeHeader struct {
	columnAttributeImpl
	Key string
}

func (node *DTAttributeHeader) Format(ctx *FmtCtx) {
	ctx.WriteString("header(")
	ctx.WriteString(node.Key)
	ctx.WriteByte(')')
}

func NewDTAttributeHeader(key string) *DTAttributeHeader {
	return &DTAttributeHeader{
		Key: key,
	}
}

type DTAttributeHeaders struct {
	columnAttributeImpl
}

func (node *DTAttributeHeaders) Format(ctx *FmtCtx) {
	ctx.WriteString("headers")
}

func NewDTAttributeHeaders() *DTAttributeHeaders {
	return &DTAttributeHeaders{}
}
