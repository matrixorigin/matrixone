// Copyright 2026 Matrix Origin
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

// AlterDatabase changes the defaults inherited by subsequently created tables.
type AlterDatabase struct {
	statementImpl
	Name    Identifier
	Options []CreateOption
}

func (node *AlterDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("alter database")
	if node.Name != "" {
		ctx.WriteByte(' ')
		node.Name.Format(ctx)
	}
	for _, option := range node.Options {
		ctx.WriteByte(' ')
		option.Format(ctx)
	}
}

func (node *AlterDatabase) GetStatementType() string { return "Alter Database" }
func (node *AlterDatabase) GetQueryType() string     { return QueryTypeDDL }
func (node *AlterDatabase) StmtKind() StmtKind       { return defaultStatusTyp }
func (node *AlterDatabase) Free() {
	for _, option := range node.Options {
		switch opt := option.(type) {
		case *CreateOptionCharset:
			opt.Free()
		case *CreateOptionCollate:
			opt.Free()
		}
	}
	node.Options = nil
}
