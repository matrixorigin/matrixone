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

import "fmt"

type ShowProfileStmt struct {
	statementImpl
	ForQuery int64
	Limit    *Limit
}

func NewShowProfileStmt(forQuery int64, limit *Limit) *ShowProfileStmt {
	return &ShowProfileStmt{
		ForQuery: forQuery,
		Limit:    limit,
	}
}

func (node *ShowProfileStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("show profile")
	if node.ForQuery > 0 {
		ctx.WriteString(fmt.Sprintf(" for query %d", node.ForQuery))
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		node.Limit.Format(ctx)
	}
}

func (node *ShowProfileStmt) GetStatementType() string { return "Show Profile" }
func (node *ShowProfileStmt) GetQueryType() string     { return QueryTypeOth }
