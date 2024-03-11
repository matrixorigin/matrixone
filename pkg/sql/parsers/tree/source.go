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
	reuse.CreatePool[CreateSource](
		func() *CreateSource { return &CreateSource{} },
		func(c *CreateSource) { c.reset() },
		reuse.DefaultOptions[CreateSource]().
			WithEnableChecker())

	reuse.CreatePool[CreateSourceWithOption](
		func() *CreateSourceWithOption { return &CreateSourceWithOption{} },
		func(c *CreateSourceWithOption) { c.reset() },
		reuse.DefaultOptions[CreateSourceWithOption]().
			WithEnableChecker())

	reuse.CreatePool[AttributeHeader](
		func() *AttributeHeader { return &AttributeHeader{} },
		func(a *AttributeHeader) { a.reset() },
		reuse.DefaultOptions[AttributeHeader]().
			WithEnableChecker())

	reuse.CreatePool[AttributeHeaders](
		func() *AttributeHeaders { return &AttributeHeaders{} },
		func(a *AttributeHeaders) { a.reset() },
		reuse.DefaultOptions[AttributeHeaders]().
			WithEnableChecker())
}

type CreateSource struct {
	statementImpl
	Replace     bool
	IfNotExists bool
	SourceName  *TableName
	Defs        TableDefs
	ColNames    IdentifierList
	Options     []TableOption
}

func NewCreateSource(replace bool, ifNotExists bool, sourceName *TableName, defs TableDefs, options []TableOption) *CreateSource {
	c := reuse.Alloc[CreateSource](nil)
	c.Replace = replace
	c.IfNotExists = ifNotExists
	c.SourceName = sourceName
	c.Defs = defs
	c.Options = options
	return c
}

func (node *CreateSource) Format(ctx *FmtCtx) {
	ctx.WriteString("create")
	if node.Replace {
		ctx.WriteString(" or replace")
	}
	if node.Defs != nil {
		ctx.WriteString(" source")
		if node.IfNotExists {
			ctx.WriteString(" if not exists")
		}
		ctx.WriteByte(' ')
		node.SourceName.Format(ctx)

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
	ctx.WriteString(" source")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	ctx.WriteByte(' ')
	node.SourceName.Format(ctx)
	if node.Options != nil {
		prefix := " with ("
		for _, t := range node.Options {
			ctx.WriteString(prefix)
			t.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
}

func (node *CreateSource) GetStatementType() string { return "Create Source" }
func (node *CreateSource) GetQueryType() string     { return QueryTypeDDL }

func (node CreateSource) TypeName() string { return "tree.CreateSource" }

func (node *CreateSource) reset() {
	// if node.SourceName != nil {
	// 	reuse.Free[TableName](node.SourceName, nil)
	// }
	if node.Options != nil {
		for _, item := range node.Options {
			switch opt := item.(type) {
			case *TableOptionProperties:
				reuse.Free(opt, nil)
			case *TableOptionEngine:
				reuse.Free(opt, nil)
			case *TableOptionEngineAttr:
				reuse.Free(opt, nil)
			case *TableOptionInsertMethod:
				reuse.Free(opt, nil)
			case *TableOptionSecondaryEngine:
				reuse.Free(opt, nil)
			case *TableOptionSecondaryEngineNull:
				reuse.Free(opt, nil)
			case *TableOptionCharset:
				reuse.Free(opt, nil)
			case *TableOptionCollate:
				reuse.Free(opt, nil)
			case *TableOptionAUTOEXTEND_SIZE:
				reuse.Free(opt, nil)
			case *TableOptionAutoIncrement:
				reuse.Free(opt, nil)
			case *TableOptionComment:
				reuse.Free(opt, nil)
			case *TableOptionAvgRowLength:
				reuse.Free(opt, nil)
			case *TableOptionChecksum:
				reuse.Free(opt, nil)
			case *TableOptionCompression:
				reuse.Free(opt, nil)
			case *TableOptionConnection:
				reuse.Free(opt, nil)
			case *TableOptionPassword:
				reuse.Free(opt, nil)
			case *TableOptionKeyBlockSize:
				reuse.Free(opt, nil)
			case *TableOptionMaxRows:
				reuse.Free(opt, nil)
			case *TableOptionMinRows:
				reuse.Free(opt, nil)
			case *TableOptionDelayKeyWrite:
				reuse.Free(opt, nil)
			case *TableOptionRowFormat:
				reuse.Free(opt, nil)
			case *TableOptionStartTrans:
				reuse.Free(opt, nil)
			case *TableOptionSecondaryEngineAttr:
				reuse.Free(opt, nil)
			case *TableOptionStatsPersistent:
				reuse.Free(opt, nil)
			case *TableOptionStatsAutoRecalc:
				reuse.Free(opt, nil)
			case *TableOptionPackKeys:
				reuse.Free(opt, nil)
			case *TableOptionTablespace:
				reuse.Free(opt, nil)
			case *TableOptionDataDirectory:
				reuse.Free(opt, nil)
			case *TableOptionIndexDirectory:
				reuse.Free(opt, nil)
			case *TableOptionStorageMedia:
				reuse.Free(opt, nil)
			case *TableOptionStatsSamplePages:
				reuse.Free(opt, nil)
			case *TableOptionUnion:
				reuse.Free(opt, nil)
			case *TableOptionEncryption:
				reuse.Free(opt, nil)
			default:
				panic("should not happen")
			}
		}
	}
	*node = CreateSource{}
}

func (node *CreateSource) Free() {
	reuse.Free[CreateSource](node, nil)
}

type CreateSourceWithOption struct {
	createOptionImpl
	Key Identifier
	Val Expr
}

func NewCreateSourceWithOption(key Identifier, val Expr) *CreateSourceWithOption {
	c := reuse.Alloc[CreateSourceWithOption](nil)
	c.Key = key
	c.Val = val
	return c
}

func (node *CreateSourceWithOption) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Key))
	ctx.WriteString(" = ")
	node.Val.Format(ctx)
}

func (node CreateSourceWithOption) TypeName() string { return "tree.CreateSourceWithOption" }

func (node *CreateSourceWithOption) reset() {
	*node = CreateSourceWithOption{}
}

func (node *CreateSourceWithOption) Free() {
	reuse.Free[CreateSourceWithOption](node, nil)
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

func (node AttributeHeader) TypeName() string { return "tree.AttributeHeader" }

func (node *AttributeHeader) reset() {
	*node = AttributeHeader{}
}

func (node *AttributeHeader) Free() {
	reuse.Free[AttributeHeader](node, nil)
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

func (node AttributeHeaders) TypeName() string { return "tree.AttributeHeaders" }

func (node *AttributeHeaders) reset() {
	*node = AttributeHeaders{}
}

func (node *AttributeHeaders) Free() {
	reuse.Free[AttributeHeaders](node, nil)
}
func NewAttributeHeaders() *AttributeHeaders {
	return &AttributeHeaders{}
}
