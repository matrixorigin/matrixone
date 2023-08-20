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

type CreateConnector struct {
	statementImpl
	ConnectorName *TableName
	Options       []TableOption
}

func (node *CreateConnector) Format(ctx *FmtCtx) {
	ctx.WriteString("create")
	ctx.WriteString(" connector")
	ctx.WriteByte(' ')
	node.ConnectorName.Format(ctx)
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

type CreateConnectorWithOption struct {
	createOptionImpl
	Key Identifier
	Val Expr
}

func (node *CreateConnectorWithOption) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Key))
	ctx.WriteString(" = ")
	node.Val.Format(ctx)
}

func (node *CreateConnector) GetStatementType() string { return "Create Connector" }
func (node *CreateConnector) GetQueryType() string     { return QueryTypeDDL }
