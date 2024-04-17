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
	reuse.CreatePool[TruncateTable](
		func() *TruncateTable { return &TruncateTable{} },
		func(t *TruncateTable) { t.reset() },
		reuse.DefaultOptions[TruncateTable](), //.
	) //WithEnableChecker()

}

// truncate table statement
type TruncateTable struct {
	statementImpl
	Name *TableName
}

func NewTruncateTable(name *TableName) *TruncateTable {
	truncate := reuse.Alloc[TruncateTable](nil)
	truncate.Name = name
	return truncate
}

func (node *TruncateTable) Format(ctx *FmtCtx) {
	ctx.WriteString("truncate table")
	ctx.WriteByte(' ')
	node.Name.Format(ctx)
}

func (node *TruncateTable) GetStatementType() string { return "Truncate" }
func (node *TruncateTable) GetQueryType() string     { return QueryTypeDDL }

func (node *TruncateTable) Free() {
	reuse.Free[TruncateTable](node, nil)
}

func (node *TruncateTable) reset() {
	// if node.Name != nil {
	// node.Name.Free()
	// }
	*node = TruncateTable{}
}

func (node TruncateTable) TypeName() string { return "tree.TruncateTable" }
