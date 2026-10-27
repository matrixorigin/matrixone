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

import "strings"

type RenameUserPair struct{ From, To *User }
type RenameUser struct {
	statementImpl
	Pairs []RenameUserPair
}

func (n *RenameUser) Format(ctx *FmtCtx) {
	ctx.WriteString("rename user ")
	write := func(u *User) {
		quote := func(s string) {
			ctx.WriteString("'")
			ctx.WriteString(strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "''"))
			ctx.WriteString("'")
		}
		quote(u.Username)
		ctx.WriteString("@")
		quote(u.Hostname)
	}
	for i, p := range n.Pairs {
		if i > 0 {
			ctx.WriteString(", ")
		}
		write(p.From)
		ctx.WriteString(" to ")
		write(p.To)
	}
}
func (*RenameUser) GetStatementType() string { return "Rename User" }
func (*RenameUser) GetQueryType() string     { return QueryTypeDCL }
func (n *RenameUser) Free() {
	for _, p := range n.Pairs {
		p.From.Free()
		p.To.Free()
	}
	n.Pairs = nil
}

func (*RenameUser) StmtKind() StmtKind { return frontendStatusTyp }
