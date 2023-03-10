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

func (node *DropDatabase) GetStatementType() string { return "Drop Database" }
func (node *DropDatabase) GetQueryType() string     { return QueryTypeDDL }

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

func (node *DropTable) GetStatementType() string { return "Drop Table" }
func (node *DropTable) GetQueryType() string     { return QueryTypeDDL }

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

func (node *DropView) GetStatementType() string { return "Drop View" }
func (node *DropView) GetQueryType() string     { return QueryTypeDDL }

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

func (node *DropIndex) GetStatementType() string { return "Drop Index" }
func (node *DropIndex) GetQueryType() string     { return QueryTypeDDL }

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

func (node *DropRole) GetStatementType() string { return "Drop Role" }
func (node *DropRole) GetQueryType() string     { return QueryTypeDCL }

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

func (node *DropUser) GetStatementType() string { return "Drop User" }
func (node *DropUser) GetQueryType() string     { return QueryTypeDCL }

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

func (node *DropAccount) GetStatementType() string { return "Drop Account" }
func (node *DropAccount) GetQueryType() string     { return QueryTypeDCL }

type DropPublication struct {
	statementImpl
	Name     Identifier
	IfExists bool
}

func (node *DropPublication) Format(ctx *FmtCtx) {
	ctx.WriteString("drop publication")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	node.Name.Format(ctx)
}
