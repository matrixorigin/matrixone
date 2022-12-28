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

type With struct {
	statementImpl
	IsRecursive bool
	CTEs        []*CTE
}

func (node *With) Format(ctx *FmtCtx) {
	ctx.WriteString("with ")
	if node.IsRecursive {
		ctx.WriteString("recursive ")
	}
	prefix := ""
	for _, cte := range node.CTEs {
		ctx.WriteString(prefix)
		cte.Format(ctx)
		prefix = ", "
	}
}
func (node *With) GetStatementType() string { return "With" }
func (node *With) GetQueryType() string     { return QueryTypeDML }

type CTE struct {
	Name *AliasClause
	Stmt Statement
}

func (node *CTE) Format(ctx *FmtCtx) {
	node.Name.Format(ctx)
	ctx.WriteString(" as (")
	node.Stmt.Format(ctx)
	ctx.WriteString(")")
}
