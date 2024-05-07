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
	reuse.CreatePool[AnalyzeStmt](
		func() *AnalyzeStmt { return &AnalyzeStmt{} },
		func(a *AnalyzeStmt) { a.reset() },
		reuse.DefaultOptions[AnalyzeStmt](), //.
	) //WithEnableChecker()
}

// Use statement
type AnalyzeStmt struct {
	statementImpl
	Table *TableName
	Cols  IdentifierList
}

func (node *AnalyzeStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("analyze table ")
	node.Table.Format(ctx)
	ctx.WriteString("(")
	node.Cols.Format(ctx)
	ctx.WriteString(")")
}

func (node *AnalyzeStmt) GetStatementType() string { return "Analyze Table" }
func (node *AnalyzeStmt) GetQueryType() string     { return QueryTypeOth }

func (node *AnalyzeStmt) Free() {
	reuse.Free[AnalyzeStmt](node, nil)
}

func (node AnalyzeStmt) TypeName() string { return "tree.AnalyzeStmt" }

func (node *AnalyzeStmt) reset() {
	// if node.Table != nil {
	// node.Table.Free()
	// }
	*node = AnalyzeStmt{}
}

func NewAnalyzeStmt(tbl *TableName, cols IdentifierList) *AnalyzeStmt {
	analyzestmt := reuse.Alloc[AnalyzeStmt](nil)
	analyzestmt.Table = tbl
	analyzestmt.Cols = cols
	return analyzestmt
}
