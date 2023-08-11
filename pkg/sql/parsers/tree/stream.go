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

type CreateStream struct {
	statementImpl
	Replace     bool
	Source      bool
	IfNotExists bool
	StreamName  TableName
	Defs        TableDefs
	ColNames    IdentifierList
	AsSource    *Select
	Options     []TableOption
}

func (node *CreateStream) Format(ctx *FmtCtx) {
	ctx.WriteString("create")
	if node.Replace {
		ctx.WriteString(" or replace")
	}
	if node.Defs != nil {
		if node.Source {
			ctx.WriteString(" source")
		}
		ctx.WriteString(" stream")
		if node.IfNotExists {
			ctx.WriteString(" if not exists")
		}
		ctx.WriteByte(' ')
		node.StreamName.Format(ctx)

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
	ctx.WriteString(" stream")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	ctx.WriteByte(' ')
	node.StreamName.Format(ctx)
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

type CreateStreamWithOption struct {
	createOptionImpl
	Key Identifier
	Val Expr
}

func (node *CreateStreamWithOption) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Key))
	ctx.WriteString(" = ")
	node.Val.Format(ctx)
}

type AttributeHeader struct {
	columnAttributeImpl
	Key string
}

func (node *AttributeHeader) Format(ctx *FmtCtx) {
	ctx.WriteString("header(")
	ctx.WriteString(node.Key)
	ctx.WriteByte(')')
}

func NewAttributeHeader(key string) *AttributeHeader {
	return &AttributeHeader{
		Key: key,
	}
}

type AttributeHeaders struct {
	columnAttributeImpl
}

func (node *AttributeHeaders) Format(ctx *FmtCtx) {
	ctx.WriteString("headers")
}

func NewAttributeHeaders() *AttributeHeaders {
	return &AttributeHeaders{}
}
