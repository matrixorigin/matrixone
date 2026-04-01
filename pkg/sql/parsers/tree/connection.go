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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[DropConnection](
		func() *DropConnection { return &DropConnection{} },
		func(d *DropConnection) { d.reset() },
		reuse.DefaultOptions[DropConnection](),
	)

	reuse.CreatePool[CreateConnection](
		func() *CreateConnection { return &CreateConnection{} },
		func(c *CreateConnection) { c.reset() },
		reuse.DefaultOptions[CreateConnection](),
	)

	reuse.CreatePool[ConnectionOption](
		func() *ConnectionOption { return &ConnectionOption{} },
		func(c *ConnectionOption) { c.reset() },
		reuse.DefaultOptions[ConnectionOption](),
	)
}

type DropConnection struct {
	statementImpl
	IfExists bool
	Name     Identifier
}

func NewDropConnection(ifExists bool, name Identifier) *DropConnection {
	stmt := reuse.Alloc[DropConnection](nil)
	stmt.IfExists = ifExists
	stmt.Name = name
	return stmt
}

func (node *DropConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("drop connection")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	ctx.WriteString(string(node.Name))
}

func (node *DropConnection) GetStatementType() string { return "Drop Connection" }
func (node *DropConnection) GetQueryType() string     { return QueryTypeDDL }
func (node DropConnection) TypeName() string          { return "tree.DropConnection" }

func (node *DropConnection) Free() {
	reuse.Free[DropConnection](node, nil)
}

func (node *DropConnection) reset() {
	*node = DropConnection{}
}

type CreateConnection struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
	Type        string
	Options     []*ConnectionOption
}

func NewCreateConnection(ifNotExists bool, name Identifier, typ string, options []*ConnectionOption) *CreateConnection {
	stmt := reuse.Alloc[CreateConnection](nil)
	stmt.IfNotExists = ifNotExists
	stmt.Name = name
	stmt.Type = typ
	stmt.Options = options
	return stmt
}

func (node *CreateConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("create connection")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	ctx.WriteByte(' ')
	ctx.WriteString(string(node.Name))
	ctx.WriteString(" type = ")
	writeConnectionQuotedString(ctx, node.Type)
	ctx.WriteString(" options (")
	for i, opt := range node.Options {
		if i > 0 {
			ctx.WriteString(", ")
		}
		opt.Format(ctx)
	}
	ctx.WriteByte(')')
}

func (node *CreateConnection) GetStatementType() string { return "Create Connection" }
func (node *CreateConnection) GetQueryType() string     { return QueryTypeDDL }
func (node CreateConnection) TypeName() string          { return "tree.CreateConnection" }

func (node *CreateConnection) Free() {
	reuse.Free[CreateConnection](node, nil)
}

func (node *CreateConnection) reset() {
	*node = CreateConnection{}
}

type ConnectionOption struct {
	createOptionImpl
	Key   Identifier
	Value string
}

func NewConnectionOption(key Identifier, value string) *ConnectionOption {
	opt := reuse.Alloc[ConnectionOption](nil)
	opt.Key = key
	opt.Value = value
	return opt
}

func (node *ConnectionOption) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Key))
	ctx.WriteString(" = ")
	writeConnectionQuotedString(ctx, node.Value)
}

func (node *ConnectionOption) Free() {
	reuse.Free[ConnectionOption](node, nil)
}

func (node ConnectionOption) TypeName() string { return "tree.ConnectionOption" }

func (node *ConnectionOption) reset() {
	*node = ConnectionOption{}
}

func writeConnectionQuotedString(ctx *FmtCtx, value string) {
	ctx.WriteByte('\'')
	ctx.WriteString(strings.ReplaceAll(value, "'", "''"))
	ctx.WriteByte('\'')
}
