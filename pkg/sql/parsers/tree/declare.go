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

// Declare statement
type Declare struct {
	statementImpl
	Variables  []string
	ColumnType *T
	DefaultVal Expr
}

func (node *Declare) Format(ctx *FmtCtx) {
	ctx.WriteString("declare ")
	for _, v := range node.Variables {
		ctx.WriteString(v + " ")
	}
	node.ColumnType.InternalType.Format(ctx)
	ctx.WriteString(" default ")
	node.DefaultVal.Format(ctx)
}

func (node *Declare) GetStatementType() string { return "Declare" }
func (node *Declare) GetQueryType() string     { return QueryTypeDCL }
