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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[CreateSource](
		func() *CreateSource { return &CreateSource{} },
		func(c *CreateSource) { c.reset() },
		reuse.DefaultOptions[CreateSource](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CreateSourceWithOption](
		func() *CreateSourceWithOption { return &CreateSourceWithOption{} },
		func(c *CreateSourceWithOption) { c.reset() },
		reuse.DefaultOptions[CreateSourceWithOption](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeHeader](
		func() *AttributeHeader { return &AttributeHeader{} },
		func(a *AttributeHeader) { a.reset() },
		reuse.DefaultOptions[AttributeHeader](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AttributeHeaders](
		func() *AttributeHeaders { return &AttributeHeaders{} },
		func(a *AttributeHeaders) { a.reset() },
		reuse.DefaultOptions[AttributeHeaders](), //.
	) //WithEnableChecker()
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
	// node.SourceName.Free()
	// }
	if node.Options != nil {
		for _, item := range node.Options {
			switch opt := item.(type) {
			case *TableOptionProperties:
				opt.Free()
			case *TableOptionEngine:
				opt.Free()
			case *TableOptionEngineAttr:
				opt.Free()
			case *TableOptionInsertMethod:
				opt.Free()
			case *TableOptionSecondaryEngine:
				opt.Free()
			case *TableOptionSecondaryEngineNull:
				panic("currently not used")
			case *TableOptionCharset:
				opt.Free()
			case *TableOptionCollate:
				opt.Free()
			case *TableOptionAUTOEXTEND_SIZE:
				opt.Free()
			case *TableOptionAutoIncrement:
				opt.Free()
			case *TableOptionComment:
				opt.Free()
			case *TableOptionAvgRowLength:
				opt.Free()
			case *TableOptionChecksum:
				opt.Free()
			case *TableOptionCompression:
				opt.Free()
			case *TableOptionConnection:
				opt.Free()
			case *TableOptionPassword:
				opt.Free()
			case *TableOptionKeyBlockSize:
				opt.Free()
			case *TableOptionMaxRows:
				opt.Free()
			case *TableOptionMinRows:
				opt.Free()
			case *TableOptionDelayKeyWrite:
				opt.Free()
			case *TableOptionRowFormat:
				opt.Free()
			case *TableOptionStartTrans:
				opt.Free()
			case *TableOptionSecondaryEngineAttr:
				opt.Free()
			case *TableOptionStatsPersistent:
				opt.Free()
			case *TableOptionStatsAutoRecalc:
				opt.Free()
			case *TableOptionPackKeys:
				opt.Free()
			case *TableOptionTablespace:
				opt.Free()
			case *TableOptionDataDirectory:
				opt.Free()
			case *TableOptionIndexDirectory:
				opt.Free()
			case *TableOptionStorageMedia:
				opt.Free()
			case *TableOptionStatsSamplePages:
				opt.Free()
			case *TableOptionUnion:
				opt.Free()
			case *TableOptionEncryption:
				opt.Free()
			default:
				if opt != nil {
					panic(fmt.Sprintf("miss Free for %v", item))
				}
			}
		}
	}

	if node.Defs != nil {
		for _, def := range node.Defs {
			switch d := def.(type) {
			case *ColumnTableDef:
				d.Free()
			case *PrimaryKeyIndex:
				d.Free()
			case *Index:
				d.Free()
			case *UniqueIndex:
				d.Free()
			case *ForeignKey:
				d.Free()
			case *FullTextIndex:
				d.Free()
			case *CheckIndex:
				d.Free()
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
