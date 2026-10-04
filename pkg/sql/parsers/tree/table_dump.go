// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

// DumpTable exports the metadata needed to attach a table's immutable objects.
// When MetadataOnly is false, the object files are exported as well.
type DumpTable struct {
	statementImpl
	Table        *TableName
	Path         string
	MetadataOnly bool
}

func (node *DumpTable) Format(ctx *FmtCtx) {
	ctx.WriteString("dump table ")
	node.Table.Format(ctx)
	ctx.WriteString(" to '")
	ctx.WriteString(node.Path)
	ctx.WriteByte('\'')
	if node.MetadataOnly {
		ctx.WriteString(" metadata only")
	}
}

func (node *DumpTable) StmtKind() StmtKind       { return frontendStatusTyp }
func (node *DumpTable) GetStatementType() string { return "dump table" }
func (node *DumpTable) GetQueryType() string     { return QueryTypeOth }
func (node *DumpTable) TypeName() string         { return "tree.DumpTable" }
func (node *DumpTable) Free()                    {}
func (node *DumpTable) String() string           { return node.GetStatementType() }

// LoadTable attaches immutable objects exported by DumpTable to an existing,
// empty table.
type LoadTable struct {
	statementImpl
	Table *TableName
	Path  string
}

func (node *LoadTable) Format(ctx *FmtCtx) {
	ctx.WriteString("load table ")
	node.Table.Format(ctx)
	ctx.WriteString(" from '")
	ctx.WriteString(node.Path)
	ctx.WriteByte('\'')
}

func (node *LoadTable) StmtKind() StmtKind       { return frontendStatusTyp }
func (node *LoadTable) GetStatementType() string { return "load table" }
func (node *LoadTable) GetQueryType() string     { return QueryTypeDML }
func (node *LoadTable) TypeName() string         { return "tree.LoadTable" }
func (node *LoadTable) Free()                    {}
func (node *LoadTable) String() string           { return node.GetStatementType() }
