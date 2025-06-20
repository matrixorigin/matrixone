// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

type CloneTable struct {
	statementImpl

	SrcTable    TableName
	CreateTable CreateTable

	IsRestore     bool
	IsRestoreByTS bool
	FromAccount   uint32
	ToAccount     uint32

	Sql string
}

func (node *CloneTable) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CloneTable) Format(ctx *FmtCtx) {
	ctx.WriteString("create table clone")
	//node.Name.ToTableName().Format(ctx)
	//if node.AtTsExpr != nil {
	//	ctx.WriteString(" ")
	//	node.AtTsExpr.Format(ctx)
	//}
}

func (node *CloneTable) GetStatementType() string { return "CREATE TABLE CLONE" }
func (node *CloneTable) GetQueryType() string     { return QueryTypeOth }

func NewCloneTable() *CloneTable {
	return reuse.Alloc[CloneTable](nil)
}

func (node CloneTable) TypeName() string { return "tree.CloneTable" }

func (node *CloneTable) Free() {
	reuse.Free[CloneTable](node, nil)
}

func (node *CloneTable) reset() {
	node.CreateTable.reset()
	*node = CloneTable{}
}
