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

// DROP Database statement
type DropDatabase struct {
	statementImpl
	Name     Identifier
	IfExists bool
}

func (node *DropDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("drop database")
	if node.IfExists {
		ctx.WriteByte(' ')
		ctx.WriteString("if exists")
	}
	if node.Name != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(string(node.Name))
	}
}

func NewDropDatabase(n Identifier, i bool) *DropDatabase {
	return &DropDatabase{
		Name:     n,
		IfExists: i,
	}
}

// DROP Table statement
type DropTable struct {
	statementImpl
	IfExists bool
	Names    TableNames
}

func (node *DropTable) Format(ctx *FmtCtx) {
	ctx.WriteString("drop table")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	node.Names.Format(ctx)
}

func NewDropTable(i bool, n TableNames) *DropTable {
	return &DropTable{
		IfExists: i,
		Names:    n,
	}
}

// DropView DROP View statement
type DropView struct {
	statementImpl
	IfExists bool
	Names    TableNames
}

func (node *DropView) Format(ctx *FmtCtx) {
	ctx.WriteString("drop view")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	node.Names.Format(ctx)
}

func NewDropView(i bool, n TableNames) *DropView {
	return &DropView{
		IfExists: i,
		Names:    n,
	}
}

type DropIndex struct {
	statementImpl
	Name       Identifier
	TableName  TableName
	IfExists   bool
	MiscOption []MiscOption
}

func (node *DropIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("drop index")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	ctx.WriteString(string(node.Name))

	ctx.WriteString(" on ")
	node.TableName.Format(ctx)
}

func NewDropIndex(i Identifier, t TableName, ife bool, m []MiscOption) *DropIndex {
	return &DropIndex{
		Name:       i,
		TableName:  t,
		IfExists:   ife,
		MiscOption: m,
	}
}

type DropRole struct {
	statementImpl
	IfExists bool
	Roles    []*Role
}

func (node *DropRole) Format(ctx *FmtCtx) {
	ctx.WriteString("drop role")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	prefix := " "
	for _, r := range node.Roles {
		ctx.WriteString(prefix)
		r.Format(ctx)
		prefix = ", "
	}
}

func NewDropRole(ife bool, r []*Role) *DropRole {
	return &DropRole{
		IfExists: ife,
		Roles:    r,
	}
}

type DropUser struct {
	statementImpl
	IfExists bool
	Users    []*User
}

func (node *DropUser) Format(ctx *FmtCtx) {
	ctx.WriteString("drop user")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	prefix := " "
	for _, u := range node.Users {
		ctx.WriteString(prefix)
		u.Format(ctx)
		prefix = ", "
	}
}

func NewDropUser(ife bool, u []*User) *DropUser {
	return &DropUser{
		IfExists: ife,
		Users:    u,
	}
}

type DropAccount struct {
	statementImpl
	IfExists bool
	Name     string
}

func (node *DropAccount) Format(ctx *FmtCtx) {
	ctx.WriteString("drop account")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteString(" ")
	ctx.WriteString(node.Name)
}
