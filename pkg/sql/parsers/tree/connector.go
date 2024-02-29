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
	reuse.CreatePool[DropConnector](
		func() *DropConnector { return &DropConnector{} },
		func(d *DropConnector) { d.reset() },
		reuse.DefaultOptions[DropConnector]().
			WithEnableChecker())

	reuse.CreatePool[CreateConnector](
		func() *CreateConnector { return &CreateConnector{} },
		func(c *CreateConnector) { c.reset() },
		reuse.DefaultOptions[CreateConnector]().
			WithEnableChecker())

	reuse.CreatePool[ConnectorOption](
		func() *ConnectorOption { return &ConnectorOption{} },
		func(c *ConnectorOption) { c.reset() },
		reuse.DefaultOptions[ConnectorOption]().
			WithEnableChecker())
}

type DropConnector struct {
	statementImpl
	IfExists bool
	Names    TableNames
}

func (node *DropConnector) Format(ctx *FmtCtx) {
	ctx.WriteString("drop connector")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	node.Names.Format(ctx)
}

func (node *DropConnector) GetStatementType() string { return "Drop Connector" }
func (node *DropConnector) GetQueryType() string     { return QueryTypeDDL }

func (node *DropConnector) Free() {
	reuse.Free[DropConnector](node, nil)
}

func (node DropConnector) TypeName() string { return "tree.DropConnector" }

func (node *DropConnector) reset() {
	*node = DropConnector{}
}

func NewDropConnector(i bool, n TableNames) *DropConnector {
	dropView := reuse.Alloc[DropConnector](nil)
	dropView.IfExists = i
	dropView.Names = n
	return dropView
}

type CreateConnector struct {
	statementImpl
	TableName *TableName
	Options   []*ConnectorOption
}

func (node *CreateConnector) Format(ctx *FmtCtx) {
	ctx.WriteString("create connector for ")
	node.TableName.Format(ctx)
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

func (node *CreateConnector) GetStatementType() string { return "Create Connector" }
func (node *CreateConnector) GetQueryType() string     { return QueryTypeDDL }

func (node *CreateConnector) Free() {
	reuse.Free[CreateConnector](node, nil)
}

func (node CreateConnector) TypeName() string { return "tree.CreateConnector" }

func (node *CreateConnector) reset() {
	// if node.TableName != nil {
	// 	reuse.Free[TableName](node.TableName, nil)
	// }
	// if node.Options != nil {
	// 	for _, item := range node.Options {
	// 		reuse.Free[ConnectorOption](item, nil)
	// 	}
	// }
	*node = CreateConnector{}
}

type ConnectorOption struct {
	createOptionImpl
	Key Identifier
	Val Expr
}

func (node *ConnectorOption) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Key))
	ctx.WriteString(" = ")
	node.Val.Format(ctx)
}

func (node *ConnectorOption) Free() {
	reuse.Free[ConnectorOption](node, nil)
}

func (node ConnectorOption) TypeName() string { return "tree.ConnectorOption" }

func (node *ConnectorOption) reset() {
	*node = ConnectorOption{}
}
