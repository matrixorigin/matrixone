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
	reuse.CreatePool[DropDatabase](
		func() *DropDatabase { return &DropDatabase{} },
		func(d *DropDatabase) { d.reset() },
		reuse.DefaultOptions[DropDatabase](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropTable](
		func() *DropTable { return &DropTable{} },
		func(d *DropTable) { d.reset() },
		reuse.DefaultOptions[DropTable](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropView](
		func() *DropView { return &DropView{} },
		func(d *DropView) { d.reset() },
		reuse.DefaultOptions[DropView](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropIndex](
		func() *DropIndex { return &DropIndex{} },
		func(d *DropIndex) { d.reset() },
		reuse.DefaultOptions[DropIndex](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropRole](
		func() *DropRole { return &DropRole{} },
		func(d *DropRole) { d.reset() },
		reuse.DefaultOptions[DropRole](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropUser](
		func() *DropUser { return &DropUser{} },
		func(d *DropUser) { d.reset() },
		reuse.DefaultOptions[DropUser](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropAccount](
		func() *DropAccount { return &DropAccount{} },
		func(d *DropAccount) { d.reset() },
		reuse.DefaultOptions[DropAccount](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropPublication](
		func() *DropPublication { return &DropPublication{} },
		func(d *DropPublication) { d.reset() },
		reuse.DefaultOptions[DropPublication](), //.
	) //WithEnableChecker()
}

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

func (node *DropDatabase) Free() {
	reuse.Free[DropDatabase](node, nil)
}

func (node *DropDatabase) reset() {
	*node = DropDatabase{}
}

func (node DropDatabase) TypeName() string { return "tree.DropDatabase" }

func NewDropDatabase(n Identifier, i bool) *DropDatabase {
	dropDatabase := reuse.Alloc[DropDatabase](nil)
	dropDatabase.Name = n
	dropDatabase.IfExists = i
	return dropDatabase
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

func (node *DropTable) Free() {
	reuse.Free[DropTable](node, nil)
}

func (node *DropTable) reset() {
	*node = DropTable{}
}

func (node DropTable) TypeName() string { return "tree.DropTable" }

func NewDropTable(i bool, n TableNames) *DropTable {
	dropTable := reuse.Alloc[DropTable](nil)
	dropTable.IfExists = i
	dropTable.Names = n
	return dropTable
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

func (node *DropView) Free() {
	reuse.Free[DropView](node, nil)
}

func (node *DropView) reset() {
	*node = DropView{}
}

func (node DropView) TypeName() string { return "tree.DropView" }

func NewDropView(i bool, n TableNames) *DropView {
	dropView := reuse.Alloc[DropView](nil)
	dropView.IfExists = i
	dropView.Names = n
	return dropView
}

type DropIndex struct {
	statementImpl
	Name       Identifier
	TableName  *TableName
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

func (node *DropIndex) Free() {
	reuse.Free[DropIndex](node, nil)
}

func (node *DropIndex) reset() {
	// if node.TableName != nil {
	// 	reuse.Free[TableName](node.TableName, nil)
	// }
	// if node.MiscOption != nil {
	// 	for _, item := range node.MiscOption {
	// 		reuse.Free[MiscOption](item, nil)
	// 	}
	// }
	*node = DropIndex{}
}

func (node DropIndex) TypeName() string { return "tree.DropIndex" }

func NewDropIndex(i Identifier, t *TableName, ife bool) *DropIndex {
	dropIndex := reuse.Alloc[DropIndex](nil)
	dropIndex.Name = i
	dropIndex.TableName = t
	dropIndex.IfExists = ife
	return dropIndex
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

func (node *DropRole) Free() {
	reuse.Free[DropRole](node, nil)
}

func (node *DropRole) reset() {
	if node.Roles != nil {
		for _, role := range node.Roles {
			role.Free()
		}
	}
	*node = DropRole{}
}

func (node DropRole) TypeName() string { return "tree.DropRole" }

func NewDropRole(ife bool, r []*Role) *DropRole {
	dropRole := reuse.Alloc[DropRole](nil)
	dropRole.IfExists = ife
	dropRole.Roles = r
	return dropRole
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

func (node *DropUser) Free() {
	reuse.Free[DropUser](node, nil)
}

func (node *DropUser) reset() {
	if node.Users != nil {
		for _, item := range node.Users {
			item.Free()
		}
	}
	*node = DropUser{}
}

func (node DropUser) TypeName() string { return "tree.DropUser" }

func NewDropUser(ife bool, u []*User) *DropUser {
	dropUser := reuse.Alloc[DropUser](nil)
	dropUser.IfExists = ife
	dropUser.Users = u
	return dropUser
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

func (node *DropAccount) Free() {
	reuse.Free[DropAccount](node, nil)
}

func (node *DropAccount) reset() {
	*node = DropAccount{}
}

func (node DropAccount) TypeName() string { return "tree.DropAccount" }

func NewDropAccount(ife bool, n string) *DropAccount {
	dropAccount := reuse.Alloc[DropAccount](nil)
	dropAccount.IfExists = ife
	dropAccount.Name = n
	return dropAccount
}

type DropPublication struct {
	statementImpl
	Name     Identifier
	IfExists bool
}

func NewDropPublication(ife bool, n Identifier) *DropPublication {
	dropPublication := reuse.Alloc[DropPublication](nil)
	dropPublication.IfExists = ife
	dropPublication.Name = n
	return dropPublication
}

func (node *DropPublication) Format(ctx *FmtCtx) {
	ctx.WriteString("drop publication")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	node.Name.Format(ctx)
}

func (node *DropPublication) GetStatementType() string { return "Drop Publication" }
func (node *DropPublication) GetQueryType() string     { return QueryTypeDCL }

func (node *DropPublication) Free() {
	reuse.Free[DropPublication](node, nil)
}

func (node *DropPublication) reset() {
	*node = DropPublication{}
}

func (node DropPublication) TypeName() string { return "tree.DropPublication" }
