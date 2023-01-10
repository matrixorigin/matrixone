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

type CreateView struct {
	statementImpl
	Name        *TableName
	ColNames    IdentifierList
	AsSource    *Select
	IfNotExists bool
	Temporary   bool
}

func (node *CreateView) Format(ctx *FmtCtx) {
	ctx.WriteString("create ")

	if node.Temporary {
		ctx.WriteString("temporary ")
	}
	ctx.WriteString("view ")

	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}

	node.Name.Format(ctx)
	if len(node.ColNames) > 0 {
		ctx.WriteString(" (")
		node.ColNames.Format(ctx)
		ctx.WriteByte(')')
	}
	ctx.WriteString(" as ")
	node.AsSource.Format(ctx)
}

func (node *CreateView) GetStatementType() string { return "Create View" }
func (node *CreateView) GetQueryType() string     { return QueryTypeDDL }
