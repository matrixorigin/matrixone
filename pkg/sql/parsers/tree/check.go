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

// CheckTableOption represents the option for CHECK TABLE statement.
type CheckTableOption int

const (
	CheckTableOptionNone CheckTableOption = iota
	CheckTableOptionExtended
	CheckTableOptionForUpgrade
)

type CheckTableStmt struct {
	statementImpl
	Tables TableNames
	Option CheckTableOption
}

func NewCheckTableStmt(tables TableNames, opt CheckTableOption) *CheckTableStmt {
	return &CheckTableStmt{
		Tables: tables,
		Option: opt,
	}
}

func (node *CheckTableStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("check table ")
	for i, tbl := range node.Tables {
		if i > 0 {
			ctx.WriteString(", ")
		}
		tbl.Format(ctx)
	}
	switch node.Option {
	case CheckTableOptionExtended:
		ctx.WriteString(" extended")
	case CheckTableOptionForUpgrade:
		ctx.WriteString(" for upgrade")
	}
}

func (node *CheckTableStmt) GetStatementType() string { return "Check Table" }
func (node *CheckTableStmt) GetQueryType() string     { return QueryTypeOth }
