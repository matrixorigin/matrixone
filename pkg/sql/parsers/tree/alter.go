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

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[AlterUser](
		func() *AlterUser { return &AlterUser{} },
		func(a *AlterUser) { a.reset() },
		reuse.DefaultOptions[AlterUser](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterAccount](
		func() *AlterAccount { return &AlterAccount{} },
		func(a *AlterAccount) { a.reset() },
		reuse.DefaultOptions[AlterAccount](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterView](
		func() *AlterView { return &AlterView{} },
		func(a *AlterView) { a.reset() },
		reuse.DefaultOptions[AlterView](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterDataBaseConfig](
		func() *AlterDataBaseConfig { return &AlterDataBaseConfig{} },
		func(a *AlterDataBaseConfig) { a.reset() },
		reuse.DefaultOptions[AlterDataBaseConfig](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterTable](
		func() *AlterTable { return &AlterTable{} },
		func(a *AlterTable) { a.reset() },
		reuse.DefaultOptions[AlterTable](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterOptionAlterIndex](
		func() *AlterOptionAlterIndex { return &AlterOptionAlterIndex{} },
		func(a *AlterOptionAlterIndex) { a.reset() },
		reuse.DefaultOptions[AlterOptionAlterIndex](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterOptionAlterReIndex](
		func() *AlterOptionAlterReIndex { return &AlterOptionAlterReIndex{} },
		func(a *AlterOptionAlterReIndex) { a.reset() },
		reuse.DefaultOptions[AlterOptionAlterReIndex](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterOptionAlterCheck](
		func() *AlterOptionAlterCheck { return &AlterOptionAlterCheck{} },
		func(a *AlterOptionAlterCheck) { a.reset() },
		reuse.DefaultOptions[AlterOptionAlterCheck](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterOptionAdd](
		func() *AlterOptionAdd { return &AlterOptionAdd{} },
		func(a *AlterOptionAdd) { a.reset() },
		reuse.DefaultOptions[AlterOptionAdd](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterOptionDrop](
		func() *AlterOptionDrop { return &AlterOptionDrop{} },
		func(a *AlterOptionDrop) { a.reset() },
		reuse.DefaultOptions[AlterOptionDrop](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterOptionTableName](
		func() *AlterOptionTableName { return &AlterOptionTableName{} },
		func(a *AlterOptionTableName) { a.reset() },
		reuse.DefaultOptions[AlterOptionTableName](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterAddCol](
		func() *AlterAddCol { return &AlterAddCol{} },
		func(a *AlterAddCol) { a.reset() },
		reuse.DefaultOptions[AlterAddCol](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterPublication](
		func() *AlterPublication { return &AlterPublication{} },
		func(a *AlterPublication) { a.reset() },
		reuse.DefaultOptions[AlterPublication](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterTableModifyColumnClause](
		func() *AlterTableModifyColumnClause { return &AlterTableModifyColumnClause{} },
		func(a *AlterTableModifyColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableModifyColumnClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterTableChangeColumnClause](
		func() *AlterTableChangeColumnClause { return &AlterTableChangeColumnClause{} },
		func(a *AlterTableChangeColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableChangeColumnClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterTableAddColumnClause](
		func() *AlterTableAddColumnClause { return &AlterTableAddColumnClause{} },
		func(a *AlterTableAddColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableAddColumnClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterTableRenameColumnClause](
		func() *AlterTableRenameColumnClause { return &AlterTableRenameColumnClause{} },
		func(a *AlterTableRenameColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableRenameColumnClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterTableAlterColumnClause](
		func() *AlterTableAlterColumnClause { return &AlterTableAlterColumnClause{} },
		func(a *AlterTableAlterColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableAlterColumnClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterTableOrderByColumnClause](
		func() *AlterTableOrderByColumnClause { return &AlterTableOrderByColumnClause{} },
		func(a *AlterTableOrderByColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableOrderByColumnClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterAccountAuthOption](
		func() *AlterAccountAuthOption { return &AlterAccountAuthOption{} },
		func(a *AlterAccountAuthOption) { a.reset() },
		reuse.DefaultOptions[AlterAccountAuthOption](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterColumnOrder](
		func() *AlterColumnOrder { return &AlterColumnOrder{} },
		func(a *AlterColumnOrder) { a.reset() },
		reuse.DefaultOptions[AlterColumnOrder](), //.
	) // WithEnableChecker()

	reuse.CreatePool[ColumnPosition](
		func() *ColumnPosition { return &ColumnPosition{} },
		func(c *ColumnPosition) { c.reset() },
		reuse.DefaultOptions[ColumnPosition](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterPartitionRedefinePartitionClause](
		func() *AlterPartitionRedefinePartitionClause { return &AlterPartitionRedefinePartitionClause{} },
		func(a *AlterPartitionRedefinePartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionRedefinePartitionClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterPartitionAddPartitionClause](
		func() *AlterPartitionAddPartitionClause { return &AlterPartitionAddPartitionClause{} },
		func(a *AlterPartitionAddPartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionAddPartitionClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterPartitionDropPartitionClause](
		func() *AlterPartitionDropPartitionClause { return &AlterPartitionDropPartitionClause{} },
		func(a *AlterPartitionDropPartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionDropPartitionClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AlterPartitionTruncatePartitionClause](
		func() *AlterPartitionTruncatePartitionClause { return &AlterPartitionTruncatePartitionClause{} },
		func(a *AlterPartitionTruncatePartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionTruncatePartitionClause](), //.
	) // WithEnableChecker()

	reuse.CreatePool[AccountsSetOption](
		func() *AccountsSetOption { return &AccountsSetOption{} },
		func(a *AccountsSetOption) { a.reset() },
		reuse.DefaultOptions[AccountsSetOption](), //.
	) // WithEnableChecker()
}

type AlterUser struct {
	statementImpl
	IfExists bool
	Users    []*User
	Role     *Role
	MiscOpt  UserMiscOption
	// comment or attribute
	CommentOrAttribute AccountCommentOrAttribute
}

func NewAlterUser(ifExists bool, users []*User, role *Role, miscOpt UserMiscOption, commentOrAttribute AccountCommentOrAttribute) *AlterUser {
	alter := reuse.Alloc[AlterUser](nil)
	alter.IfExists = ifExists
	alter.Users = users
	alter.Role = role
	alter.MiscOpt = miscOpt
	alter.CommentOrAttribute = commentOrAttribute
	return alter
}

func (node *AlterUser) Free() { reuse.Free[AlterUser](node, nil) }

func (node *AlterUser) Format(ctx *FmtCtx) {
	ctx.WriteString("alter user")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	if node.Users != nil {
		prefix := " "
		for _, u := range node.Users {
			ctx.WriteString(prefix)
			u.Format(ctx)
			prefix = ", "
		}
	}
	if node.Role != nil {
		ctx.WriteString(" default role ")
		node.Role.Format(ctx)
	}
	if node.MiscOpt != nil {
		prefix := " "
		ctx.WriteString(prefix)
		node.MiscOpt.Format(ctx)
	}
	node.CommentOrAttribute.Format(ctx)
}

func (node AlterUser) TypeName() string { return "tree.AlterUser" }

func (node *AlterUser) reset() {
	if node.Users != nil {
		for _, item := range node.Users {
			item.Free()
		}
	}
	if node.Role != nil {
		node.Role.Free()
	}
	if node.MiscOpt != nil {
		switch mt := node.MiscOpt.(type) {
		case *UserMiscOptionPasswordExpireNone:
			mt.Free()
		case *UserMiscOptionPasswordExpireDefault:
			mt.Free()
		case *UserMiscOptionPasswordExpireNever:
			mt.Free()
		case *UserMiscOptionPasswordExpireInterval:
			mt.Free()
		case *UserMiscOptionPasswordHistoryDefault:
			mt.Free()
		case *UserMiscOptionPasswordHistoryCount:
			mt.Free()
		case *UserMiscOptionPasswordReuseIntervalDefault:
			mt.Free()
		case *UserMiscOptionPasswordReuseIntervalCount:
			mt.Free()
		case *UserMiscOptionPasswordRequireCurrentNone:
			mt.Free()
		case *UserMiscOptionPasswordRequireCurrentDefault:
			mt.Free()
		case *UserMiscOptionPasswordRequireCurrentOptional:
			mt.Free()
		case *UserMiscOptionFailedLoginAttempts:
			mt.Free()
		case *UserMiscOptionPasswordLockTimeCount:
			mt.Free()
		case *UserMiscOptionPasswordLockTimeUnbounded:
			mt.Free()
		case *UserMiscOptionAccountLock:
			mt.Free()
		case *UserMiscOptionAccountUnlock:
			mt.Free()
		default:
			if mt != nil {
				panic(fmt.Sprintf("miss Free for %v", node.MiscOpt))
			}
		}
	}
	node.CommentOrAttribute.Free()
	*node = AlterUser{}
}

func (node *AlterUser) GetStatementType() string { return "Alter User" }
func (node *AlterUser) GetQueryType() string     { return QueryTypeDCL }

type AlterAccountAuthOption struct {
	Exist          bool
	Equal          string
	AdminName      string
	IdentifiedType AccountIdentified
}

func (node *AlterAccountAuthOption) Format(ctx *FmtCtx) {
	if node.Exist {
		ctx.WriteString(" admin_name")
		if len(node.Equal) != 0 {
			ctx.WriteString(" ")
			ctx.WriteString(node.Equal)
		}

		ctx.WriteString(fmt.Sprintf(" '%s'", node.AdminName))
		node.IdentifiedType.Format(ctx)
	}
}

func (node AlterAccountAuthOption) TypeName() string { return "tree.AlterAccountAuthOption" }

func (node *AlterAccountAuthOption) reset() {
	node.IdentifiedType.Free()
	*node = AlterAccountAuthOption{}
}

func (node *AlterAccountAuthOption) Free() { reuse.Free[AlterAccountAuthOption](node, nil) }

type AlterAccount struct {
	statementImpl
	IfExists   bool
	Name       string
	AuthOption AlterAccountAuthOption
	// status_option or not
	StatusOption AccountStatus
	// comment or not
	Comment AccountComment
}

func NewAlterAccount(exist bool, name string, aopt AlterAccountAuthOption, sopt AccountStatus, c AccountComment) *AlterAccount {
	a := reuse.Alloc[AlterAccount](nil)
	a.IfExists = exist
	a.Name = name
	a.AuthOption = aopt
	a.StatusOption = sopt
	a.Comment = c
	return a
}

func (node *AlterAccount) Free() {
	node.AuthOption.Free()
	node.StatusOption.Free()
	node.Comment.Free()
	reuse.Free[AlterAccount](node, nil)
}

func (node *AlterAccount) Format(ctx *FmtCtx) {
	ctx.WriteString("alter account ")
	if node.IfExists {
		ctx.WriteString("if exists ")
	}
	ctx.WriteString(node.Name)
	node.AuthOption.Format(ctx)
	node.StatusOption.Format(ctx)
	node.Comment.Format(ctx)
}

func (node *AlterAccount) GetStatementType() string { return "Alter Account" }
func (node *AlterAccount) GetQueryType() string     { return QueryTypeDCL }

func (node AlterAccount) TypeName() string { return "tree.AlterAccount" }

func (node *AlterAccount) reset() {
	node.AuthOption.Free()
	*node = AlterAccount{}
}

type AlterView struct {
	statementImpl
	IfExists bool
	Name     *TableName
	ColNames IdentifierList
	AsSource *Select
}

func NewAlterView(exist bool, name *TableName, colNames IdentifierList, asSource *Select) *AlterView {
	a := reuse.Alloc[AlterView](nil)
	a.IfExists = exist
	a.Name = name
	a.ColNames = colNames
	a.AsSource = asSource
	return a
}

func (node *AlterView) Free() { reuse.Free[AlterView](node, nil) }

func (node *AlterView) Format(ctx *FmtCtx) {
	ctx.WriteString("alter ")

	ctx.WriteString("view ")

	if node.IfExists {
		ctx.WriteString("if exists ")
	}

	node.Name.Format(ctx)
	if len(node.ColNames) > 0 {
		ctx.WriteString(" (")
		node.ColNames.Format(ctx)
		ctx.WriteByte(')')
	}
	ctx.WriteString(" as ")
	node.AsSource.Format(ctx)
}

func (node *AlterView) GetStatementType() string { return "Alter View" }
func (node *AlterView) GetQueryType() string     { return QueryTypeDDL }

func (node AlterView) TypeName() string { return "tree.AlterView" }

func (node *AlterView) reset() {
	// if node.Name != nil {
	//   node.Name()
	// }
	// if node.AsSource != nil {
	// node.AsSource.Free()
	// }
	*node = AlterView{}
}

type AlterDataBaseConfig struct {
	statementImpl
	AccountName    string
	DbName         string
	IsAccountLevel bool
	UpdateConfig   string
}

func NewAlterDataBaseConfig(accountName, dbName string, isAccountLevel bool, updateConfig string) *AlterDataBaseConfig {
	a := reuse.Alloc[AlterDataBaseConfig](nil)
	a.AccountName = accountName
	a.DbName = dbName
	a.IsAccountLevel = isAccountLevel
	a.UpdateConfig = updateConfig
	return a
}

func (node *AlterDataBaseConfig) Free() { reuse.Free[AlterDataBaseConfig](node, nil) }

func (node *AlterDataBaseConfig) Format(ctx *FmtCtx) {
	if node.IsAccountLevel {
		ctx.WriteString("alter ")
		ctx.WriteString("account configuration ")

		ctx.WriteString("for ")
		ctx.WriteString(fmt.Sprintf("%s ", node.AccountName))
	} else {
		ctx.WriteString("alter ")
		ctx.WriteString("database configuration ")

		ctx.WriteString("for ")
		ctx.WriteString(fmt.Sprintf("%s ", node.DbName))
	}

	ctx.WriteString("as ")
	ctx.WriteString(fmt.Sprintf("%s ", node.UpdateConfig))
}

func (node *AlterDataBaseConfig) GetStatementType() string { return "Alter DataBase config" }
func (node *AlterDataBaseConfig) GetQueryType() string     { return QueryTypeDDL }

func (node AlterDataBaseConfig) TypeName() string { return "tree.AlterDataBaseConfig" }

func (node *AlterDataBaseConfig) reset() {
	*node = AlterDataBaseConfig{}
}

// AlterTable
// see https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
type AlterTable struct {
	statementImpl
	Table           *TableName
	Options         AlterTableOptions
	PartitionOption AlterPartitionOption
}

func NewAlterTable(table *TableName) *AlterTable {
	a := reuse.Alloc[AlterTable](nil)
	a.Table = table
	return a
}

func (node *AlterTable) Free() {
	reuse.Free[AlterTable](node, nil)
}

func (node *AlterTable) Format(ctx *FmtCtx) {
	ctx.WriteString("alter table ")
	node.Table.Format(ctx)

	prefix := " "
	for _, t := range node.Options {
		ctx.WriteString(prefix)
		t.Format(ctx)
		prefix = ", "
	}

	if node.PartitionOption != nil {
		node.PartitionOption.Format(ctx)
	}
}

func (node *AlterTable) GetStatementType() string { return "Alter Table" }
func (node *AlterTable) GetQueryType() string     { return QueryTypeDDL }

func (node AlterTable) TypeName() string { return "tree.AlterTable" }

func (node *AlterTable) reset() {
	// if node.Table != nil {
	// node.Table.Free()
	// }

	if node.Options != nil {
		for _, option := range node.Options {
			switch opt := option.(type) {
			case *AlterOptionTableName:
				opt.Free()
			case *AlterOptionAlterIndex:
				opt.Free()
			case *AlterOptionAlterReIndex:
				opt.Free()
			case *AlterOptionAlterCheck:
				opt.Free()
			case *AlterOptionAdd:
				opt.Free()
			case *AlterOptionDrop:
				opt.Free()
			case *AlterTableModifyColumnClause:
				opt.Free()
			case *AlterTableChangeColumnClause:
				opt.Free()
			case *AlterTableAddColumnClause:
				opt.Free()
			case *AlterTableRenameColumnClause:
				opt.Free()
			case *AlterTableAlterColumnClause:
				opt.Free()
			case *AlterTableOrderByColumnClause:
				opt.Free()
			case *AlterAddCol:
				opt.Free()
			case *TableOptionProperties:
				opt.Free()
			case *TableOptionEngine:
				opt.Free()
			case *TableOptionEngineAttr:
				opt.Free()
			case *TableOptionInsertMethod:
				opt.Free()
			case *TableOptionSecondaryEngine:
				opt.Free()
			case *TableOptionSecondaryEngineNull:
				panic("currently not used")
			case *TableOptionCharset:
				opt.Free()
			case *TableOptionCollate:
				opt.Free()
			case *TableOptionAUTOEXTEND_SIZE:
				opt.Free()
			case *TableOptionAutoIncrement:
				opt.Free()
			case *TableOptionComment:
				opt.Free()
			case *TableOptionAvgRowLength:
				opt.Free()
			case *TableOptionChecksum:
				opt.Free()
			case *TableOptionCompression:
				opt.Free()
			case *TableOptionConnection:
				opt.Free()
			case *TableOptionPassword:
				opt.Free()
			case *TableOptionKeyBlockSize:
				opt.Free()
			case *TableOptionMaxRows:
				opt.Free()
			case *TableOptionMinRows:
				opt.Free()
			case *TableOptionDelayKeyWrite:
				opt.Free()
			case *TableOptionRowFormat:
				opt.Free()
			case *TableOptionStartTrans:
				opt.Free()
			case *TableOptionSecondaryEngineAttr:
				opt.Free()
			case *TableOptionStatsPersistent:
				opt.Free()
			case *TableOptionStatsAutoRecalc:
				opt.Free()
			case *TableOptionPackKeys:
				opt.Free()
			case *TableOptionTablespace:
				opt.Free()
			case *TableOptionDataDirectory:
				opt.Free()
			case *TableOptionIndexDirectory:
				opt.Free()
			case *TableOptionStorageMedia:
				opt.Free()
			case *TableOptionStatsSamplePages:
				opt.Free()
			case *TableOptionUnion:
				opt.Free()
			case *TableOptionEncryption:
				opt.Free()
			default:
				if opt != nil {
					panic(fmt.Sprintf("miss Free for %v", option))
				}
			}
		}
	}

	if node.PartitionOption != nil {
		switch opt := node.PartitionOption.(type) {
		case *AlterPartitionRedefinePartitionClause:
			opt.Free()
		case *AlterPartitionAddPartitionClause:
			opt.Free()
		case *AlterPartitionDropPartitionClause:
			opt.Free()
		case *AlterPartitionTruncatePartitionClause:
			opt.Free()
		default:
			if opt != nil {
				panic(fmt.Sprintf("miss Free for %v", node.PartitionOption))
			}
		}
	}

	*node = AlterTable{}
}

type AlterTableOptions = []AlterTableOption

type AlterTableOption interface {
	NodeFormatter
}

type alterOptionImpl struct {
	AlterTableOption
}

func (a *alterOptionImpl) Free() {
	panic("should implement by child")
}

type AlterOptionAlterIndex struct {
	alterOptionImpl
	Name       Identifier
	Visibility VisibleType
}

func NewAlterOptionAlterIndex(name Identifier, visibility VisibleType) *AlterOptionAlterIndex {
	a := reuse.Alloc[AlterOptionAlterIndex](nil)
	a.Name = name
	a.Visibility = visibility
	return a
}

func (node *AlterOptionAlterIndex) Free() { reuse.Free[AlterOptionAlterIndex](node, nil) }

func (node *AlterOptionAlterIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("alter index ")
	node.Name.Format(ctx)
	switch node.Visibility {
	case VISIBLE_TYPE_VISIBLE:
		ctx.WriteString(" visible")
	case VISIBLE_TYPE_INVISIBLE:
		ctx.WriteString(" invisible")
	}
}

func (node AlterOptionAlterIndex) TypeName() string { return "tree.AlterOptionAlterIndex" }

func (node *AlterOptionAlterIndex) reset() {
	*node = AlterOptionAlterIndex{}
}

type AlterOptionAlterReIndex struct {
	alterOptionImpl
	Name          Identifier
	KeyType       IndexType
	AlgoParamList int64
}

func NewAlterOptionAlterReIndex(name Identifier, keyType IndexType, algoParamList int64) *AlterOptionAlterReIndex {
	a := reuse.Alloc[AlterOptionAlterReIndex](nil)
	a.Name = name
	a.KeyType = keyType
	a.AlgoParamList = algoParamList
	return a
}

func (node *AlterOptionAlterReIndex) Free() { reuse.Free[AlterOptionAlterReIndex](node, nil) }

func (node *AlterOptionAlterReIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("alter reindex ")
	node.Name.Format(ctx)
	if node.KeyType != INDEX_TYPE_INVALID {
		ctx.WriteString(" ")
		ctx.WriteString(node.KeyType.ToString())
	}
	if node.AlgoParamList != 0 {
		ctx.WriteString(fmt.Sprintf(" lists = %d", node.AlgoParamList))
	}
}

func (node AlterOptionAlterReIndex) TypeName() string { return "tree.AlterOptionAlterReIndex" }

func (node *AlterOptionAlterReIndex) reset() {
	*node = AlterOptionAlterReIndex{}
}

type AlterOptionAlterCheck struct {
	alterOptionImpl
	Type    string
	Enforce bool
}

func NewAlterOptionAlterCheck(t string, enforce bool) *AlterOptionAlterCheck {
	a := reuse.Alloc[AlterOptionAlterCheck](nil)
	a.Type = t
	a.Enforce = enforce
	return a
}

func (node *AlterOptionAlterCheck) Free() { reuse.Free[AlterOptionAlterCheck](node, nil) }

func (node *AlterOptionAlterCheck) Format(ctx *FmtCtx) {
	ctx.WriteString("alter ")
	ctx.WriteString(node.Type + " ")
	if node.Enforce {
		ctx.WriteString("enforce")
	} else {
		ctx.WriteString("not enforce")
	}
}

func (node AlterOptionAlterCheck) TypeName() string { return "tree.AlterOptionAlterCheck" }

func (node *AlterOptionAlterCheck) reset() {
	*node = AlterOptionAlterCheck{}
}

type AlterOptionAdd struct {
	alterOptionImpl
	Def TableDef
}

func NewAlterOptionAdd(def TableDef) *AlterOptionAdd {
	a := reuse.Alloc[AlterOptionAdd](nil)
	a.Def = def
	return a
}

func (node *AlterOptionAdd) Free() { reuse.Free[AlterOptionAdd](node, nil) }

func (node *AlterOptionAdd) Format(ctx *FmtCtx) {
	ctx.WriteString("add ")
	node.Def.Format(ctx)
}

func (node AlterOptionAdd) TypeName() string { return "tree.AlterOptionAdd" }

func (node *AlterOptionAdd) reset() {
	switch d := node.Def.(type) {
	case *ColumnTableDef:
		d.Free()
	case *PrimaryKeyIndex:
		d.Free()
	case *Index:
		d.Free()
	case *UniqueIndex:
		d.Free()
	case *ForeignKey:
		d.Free()
	case *FullTextIndex:
		d.Free()
	case *CheckIndex:
		d.Free()
	default:
		if d != nil {
			panic(fmt.Sprintf("miss Free for %v", node.Def))
		}
	}
	*node = AlterOptionAdd{}
}

type AlterTableDropType int

const (
	AlterTableDropColumn AlterTableDropType = iota
	AlterTableDropIndex
	AlterTableDropKey
	AlterTableDropPrimaryKey
	AlterTableDropForeignKey
)

type AlterOptionDrop struct {
	alterOptionImpl
	Typ  AlterTableDropType
	Name Identifier
}

func NewAlterOptionDrop(typ AlterTableDropType, name Identifier) *AlterOptionDrop {
	a := reuse.Alloc[AlterOptionDrop](nil)
	a.Typ = typ
	a.Name = name
	return a
}

func (node *AlterOptionDrop) Free() { reuse.Free[AlterOptionDrop](node, nil) }

func (node *AlterOptionDrop) Format(ctx *FmtCtx) {
	ctx.WriteString("drop ")
	switch node.Typ {
	case AlterTableDropColumn:
		ctx.WriteString("column ")
		node.Name.Format(ctx)
	case AlterTableDropIndex:
		ctx.WriteString("index ")
		node.Name.Format(ctx)
	case AlterTableDropKey:
		ctx.WriteString("key ")
		node.Name.Format(ctx)
	case AlterTableDropPrimaryKey:
		ctx.WriteString("primary key")
	case AlterTableDropForeignKey:
		ctx.WriteString("foreign key ")
		node.Name.Format(ctx)
	}
}

func (node AlterOptionDrop) TypeName() string { return "tree.AlterOptionDrop" }

func (node *AlterOptionDrop) reset() {
	*node = AlterOptionDrop{}
}

type AlterOptionTableName struct {
	alterOptionImpl
	Name *UnresolvedObjectName
}

func NewAlterOptionTableName(name *UnresolvedObjectName) *AlterOptionTableName {
	// a := reuse.Alloc[AlterTableName](nil)
	a := new(AlterOptionTableName)
	a.Name = name
	return a
}

func (node *AlterOptionTableName) Free() {
	//  reuse.Free[AlterTableName](node, nil)
}

func (node *AlterOptionTableName) Format(ctx *FmtCtx) {
	ctx.WriteString("rename to ")
	node.Name.ToTableName().Format(ctx)
}

func (node AlterOptionTableName) TypeName() string { return "tree.AlterTableName" }

func (node *AlterOptionTableName) reset() {
	// if node.Name != nil {
	// node.Name.Free()
	// }
	*node = AlterOptionTableName{}
}

type AlterColPos struct {
	PreColName *UnresolvedName
	Pos        int32
}

// suggest rename: AlterAddColumnPosition
type AlterAddCol struct {
	alterOptionImpl
	Column   *ColumnTableDef
	Position *ColumnPosition
}

func NewAlterAddCol(column *ColumnTableDef, position *ColumnPosition) *AlterAddCol {
	a := reuse.Alloc[AlterAddCol](nil)
	a.Column = column
	a.Position = position
	return a
}

func (node *AlterAddCol) Free() { reuse.Free[AlterAddCol](node, nil) }

func (node *AlterAddCol) Format(ctx *FmtCtx) {
	ctx.WriteString("add column ")
	node.Column.Format(ctx)
	node.Position.Format(ctx)
}

func (node AlterAddCol) TypeName() string { return "tree.AlterAddCol" }

func (node *AlterAddCol) reset() {
	if node.Column != nil {
		node.Column.Free()
	}
	if node.Position != nil {
		node.Position.Free()
	}
	*node = AlterAddCol{}
}

type AccountsSetOption struct {
	All          bool
	SetAccounts  IdentifierList
	AddAccounts  IdentifierList
	DropAccounts IdentifierList
}

func NewAccountsSetOption(al bool, se, ad, dr IdentifierList) *AccountsSetOption {
	a := reuse.Alloc[AccountsSetOption](nil)
	a.All = al
	a.SetAccounts = se
	a.AddAccounts = ad
	a.DropAccounts = dr
	return a
}

func (node AccountsSetOption) TypeName() string { return "tree.AccountsSetOption" }

func (node *AccountsSetOption) reset() {
	*node = AccountsSetOption{}
}

func (node *AccountsSetOption) Free() { reuse.Free[AccountsSetOption](node, nil) }

type AlterPublication struct {
	statementImpl
	IfExists    bool
	Name        Identifier
	AccountsSet *AccountsSetOption
	Comment     string
}

func NewAlterPublication(exist bool, name Identifier, accountsSet *AccountsSetOption, comment string) *AlterPublication {
	a := reuse.Alloc[AlterPublication](nil)
	a.IfExists = exist
	a.Name = name
	a.AccountsSet = accountsSet
	a.Comment = comment
	return a
}

func (node *AlterPublication) Free() { reuse.Free[AlterPublication](node, nil) }

func (node *AlterPublication) Format(ctx *FmtCtx) {
	ctx.WriteString("alter publication ")
	if node.IfExists {
		ctx.WriteString("if exists ")
	}
	node.Name.Format(ctx)
	ctx.WriteString(" account ")
	if node.AccountsSet != nil {
		if node.AccountsSet.All {
			ctx.WriteString("all")
		} else {
			if len(node.AccountsSet.SetAccounts) > 0 {
				node.AccountsSet.SetAccounts.Format(ctx)
			}
			if len(node.AccountsSet.AddAccounts) > 0 {
				ctx.WriteString("add ")
				node.AccountsSet.AddAccounts.Format(ctx)
			}
			if len(node.AccountsSet.DropAccounts) > 0 {
				ctx.WriteString("drop ")
				node.AccountsSet.DropAccounts.Format(ctx)
			}
		}
	}
	if node.Comment != "" {
		ctx.WriteString(" comment ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Comment))
	}
}

func (node *AlterPublication) GetStatementType() string { return "Alter Publication" }
func (node *AlterPublication) GetQueryType() string     { return QueryTypeDCL }

func (node AlterPublication) TypeName() string { return "tree.AlterPublication" }

func (node *AlterPublication) reset() {
	// if node.AccountsSet != nil {
	// node.AccountsSet.Free()
	// }
	*node = AlterPublication{}
}

type AlterTableModifyColumnClause struct {
	alterOptionImpl
	Typ       AlterTableOptionType
	NewColumn *ColumnTableDef
	Position  *ColumnPosition
}

func NewAlterTableModifyColumnClause(typ AlterTableOptionType, newColumn *ColumnTableDef, position *ColumnPosition) *AlterTableModifyColumnClause {
	a := reuse.Alloc[AlterTableModifyColumnClause](nil)
	a.Typ = typ
	a.NewColumn = newColumn
	a.Position = position
	return a
}

func (node *AlterTableModifyColumnClause) Free() {
	reuse.Free[AlterTableModifyColumnClause](node, nil)
}

func (node *AlterTableModifyColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("modify column ")
	node.NewColumn.Format(ctx)
	node.Position.Format(ctx)
}

func (node AlterTableModifyColumnClause) TypeName() string {
	return "tree.AlterTableModifyColumnClause"
}

func (node *AlterTableModifyColumnClause) reset() {
	if node.NewColumn != nil {
		node.NewColumn.Free()
	}
	if node.Position != nil {
		node.Position.Free()
	}
	*node = AlterTableModifyColumnClause{}
}

type AlterTableChangeColumnClause struct {
	alterOptionImpl
	Typ           AlterTableOptionType
	OldColumnName *UnresolvedName
	NewColumn     *ColumnTableDef
	Position      *ColumnPosition
}

func NewAlterTableChangeColumnClause(typ AlterTableOptionType, oldColumnName *UnresolvedName, newColumn *ColumnTableDef, position *ColumnPosition) *AlterTableChangeColumnClause {
	a := reuse.Alloc[AlterTableChangeColumnClause](nil)
	a.Typ = typ
	a.OldColumnName = oldColumnName
	a.NewColumn = newColumn
	a.Position = position
	return a
}

func (node *AlterTableChangeColumnClause) Free() { reuse.Free[AlterTableChangeColumnClause](node, nil) }

func (node *AlterTableChangeColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("change column")
	ctx.WriteString(" ")
	node.OldColumnName.Format(ctx)
	ctx.WriteString(" ")
	node.NewColumn.Format(ctx)
	node.Position.Format(ctx)
}

func (node AlterTableChangeColumnClause) TypeName() string {
	return "tree.AlterTableChangeColumnClause"
}

func (node *AlterTableChangeColumnClause) reset() {
	// if node.OldColumnName != nil {
	// node.OldColumnName.Free()
	// }
	if node.NewColumn != nil {
		node.NewColumn.Free()
	}
	if node.Position != nil {
		node.Position.Free()
	}
	*node = AlterTableChangeColumnClause{}
}

type AlterTableAddColumnClause struct {
	alterOptionImpl
	Typ        AlterTableOptionType
	NewColumns []*ColumnTableDef
	Position   *ColumnPosition
	// when Position is not none, the len(NewColumns) must be one
}

func NewAlterTableAddColumnClause(typ AlterTableOptionType, newColumns []*ColumnTableDef, position *ColumnPosition) *AlterTableAddColumnClause {
	a := reuse.Alloc[AlterTableAddColumnClause](nil)
	a.Typ = typ
	a.NewColumns = newColumns
	a.Position = position
	return a
}

func (node *AlterTableAddColumnClause) Free() { reuse.Free[AlterTableAddColumnClause](node, nil) }

func (node *AlterTableAddColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("add column ")
	isFirst := true
	for _, column := range node.NewColumns {
		if isFirst {
			column.Format(ctx)
			isFirst = false
		}
		ctx.WriteString(", ")
		column.Format(ctx)
	}
	node.Position.Format(ctx)
}

func (node AlterTableAddColumnClause) TypeName() string { return "tree.AlterTableAddColumnClause" }

func (node *AlterTableAddColumnClause) reset() {
	if node.NewColumns != nil {
		for _, item := range node.NewColumns {
			item.Free()
		}
	}
	if node.Position != nil {
		node.Position.Free()
	}
	*node = AlterTableAddColumnClause{}
}

type AlterTableRenameColumnClause struct {
	alterOptionImpl
	Typ           AlterTableOptionType
	OldColumnName *UnresolvedName
	NewColumnName *UnresolvedName
}

func NewAlterTableRenameColumnClause(typ AlterTableOptionType, oldColumnName *UnresolvedName, newColumnName *UnresolvedName) *AlterTableRenameColumnClause {
	a := reuse.Alloc[AlterTableRenameColumnClause](nil)
	a.Typ = typ
	a.OldColumnName = oldColumnName
	a.NewColumnName = newColumnName
	return a
}

func (node *AlterTableRenameColumnClause) Free() { reuse.Free[AlterTableRenameColumnClause](node, nil) }

func (node *AlterTableRenameColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("rename column ")
	node.OldColumnName.Format(ctx)
	ctx.WriteString(" to ")
	node.NewColumnName.Format(ctx)
}

func (node AlterTableRenameColumnClause) TypeName() string {
	return "tree.AlterTableRenameColumnClause"
}

func (node *AlterTableRenameColumnClause) reset() {
	// if node.OldColumnName != nil {
	// node.OldColumnName.Free()
	// }
	// if node.NewColumnName != nil {
	// node.NewColumnName.Free()
	// }
	*node = AlterTableRenameColumnClause{}
}

type AlterColumnOptionType int

// AlterColumnOptionType types.
const (
	AlterColumnOptionSetDefault AlterColumnOptionType = iota
	AlterColumnOptionSetVisibility
	AlterColumnOptionDropDefault
)

type AlterTableAlterColumnClause struct {
	alterOptionImpl
	Typ         AlterTableOptionType
	ColumnName  *UnresolvedName
	DefalutExpr *AttributeDefault
	Visibility  VisibleType
	OptionType  AlterColumnOptionType
}

func NewAlterTableAlterColumnClause(typ AlterTableOptionType, columnName *UnresolvedName, defalutExpr *AttributeDefault, visibility VisibleType, optionType AlterColumnOptionType) *AlterTableAlterColumnClause {
	a := reuse.Alloc[AlterTableAlterColumnClause](nil)
	a.Typ = typ
	a.ColumnName = columnName
	a.DefalutExpr = defalutExpr
	a.Visibility = visibility
	a.OptionType = optionType
	return a
}

func (node *AlterTableAlterColumnClause) Free() { reuse.Free[AlterTableAlterColumnClause](node, nil) }

func (node *AlterTableAlterColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("alter column ")
	node.ColumnName.Format(ctx)
	if node.OptionType == AlterColumnOptionSetDefault {
		ctx.WriteString(" set ")
		node.DefalutExpr.Format(ctx)
	} else if node.OptionType == AlterColumnOptionSetVisibility {
		ctx.WriteString(" set")
		switch node.Visibility {
		case VISIBLE_TYPE_VISIBLE:
			ctx.WriteString(" visible")
		case VISIBLE_TYPE_INVISIBLE:
			ctx.WriteString(" invisible")
		}
	} else {
		ctx.WriteString(" drop default")
	}
}

func (node AlterTableAlterColumnClause) TypeName() string { return "tree.AlterTableAlterColumnClause" }

func (node *AlterTableAlterColumnClause) reset() {
	// if node.ColumnName != nil {
	// node.ColumnName.Free()
	// }
	// if node.DefalutExpr != nil {
	// node.DefalutExpr.Free()
	// }
	*node = AlterTableAlterColumnClause{}
}

type AlterTableOrderByColumnClause struct {
	alterOptionImpl
	Typ              AlterTableOptionType
	AlterOrderByList []*AlterColumnOrder
}

func NewAlterTableOrderByColumnClause(typ AlterTableOptionType, alterOrderByList []*AlterColumnOrder) *AlterTableOrderByColumnClause {
	a := reuse.Alloc[AlterTableOrderByColumnClause](nil)
	a.Typ = typ
	a.AlterOrderByList = alterOrderByList
	return a
}

func (node *AlterTableOrderByColumnClause) Free() {
	reuse.Free[AlterTableOrderByColumnClause](node, nil)
}

func (node *AlterTableOrderByColumnClause) Format(ctx *FmtCtx) {
	prefix := "order by "
	for _, columnOrder := range node.AlterOrderByList {
		ctx.WriteString(prefix)
		columnOrder.Format(ctx)
		prefix = ", "
	}
}

func (node AlterTableOrderByColumnClause) TypeName() string {
	return "tree.AlterTableOrderByColumnClause"
}

func (node *AlterTableOrderByColumnClause) reset() {
	if node.AlterOrderByList != nil {
		for _, item := range node.AlterOrderByList {
			item.Free()
		}
	}
	*node = AlterTableOrderByColumnClause{}
}

type AlterColumnOrder struct {
	Column    *UnresolvedName
	Direction Direction
}

func NewAlterColumnOrder(column *UnresolvedName, direction Direction) *AlterColumnOrder {
	a := reuse.Alloc[AlterColumnOrder](nil)
	a.Column = column
	a.Direction = direction
	return a
}

func (node *AlterColumnOrder) Free() { reuse.Free[AlterColumnOrder](node, nil) }

func (node *AlterColumnOrder) Format(ctx *FmtCtx) {
	node.Column.Format(ctx)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
}

func (node AlterColumnOrder) TypeName() string { return "tree.AlterColumnOrder" }

func (node *AlterColumnOrder) reset() {
	// if node.Column != nil {
	// node.Column.Free()
	// }
	*node = AlterColumnOrder{}
}

type AlterTableOptionType int

// AlterTable types.
const (
	AlterTableModifyColumn AlterTableOptionType = iota
	AlterTableChangeColumn
	AlterTableRenameColumn
	AlterTableAlterColumn
	AlterTableOrderByColumn
	AlterTableAddConstraint
	AlterTableAddColumn
)

// ColumnPositionType is the type for ColumnPosition.
type ColumnPositionType int

// ColumnPosition Types
// Do not change the value of a constant, as there are external dependencies
const (
	ColumnPositionNone  ColumnPositionType = -1
	ColumnPositionFirst ColumnPositionType = 0
	ColumnPositionAfter ColumnPositionType = 1
)

// ColumnPosition represent the position of the newly added column
type ColumnPosition struct {
	NodeFormatter
	// Tp is either ColumnPositionNone, ColumnPositionFirst or ColumnPositionAfter.
	Typ ColumnPositionType
	// RelativeColumn is the column the newly added column after if type is ColumnPositionAfter
	RelativeColumn *UnresolvedName
}

func NewColumnPosition(typ ColumnPositionType, relativeColumn *UnresolvedName) *ColumnPosition {
	a := reuse.Alloc[ColumnPosition](nil)
	a.Typ = typ
	a.RelativeColumn = relativeColumn
	return a
}

func (node *ColumnPosition) Free() { reuse.Free[ColumnPosition](node, nil) }

func (node *ColumnPosition) Format(ctx *FmtCtx) {
	switch node.Typ {
	case ColumnPositionNone:

	case ColumnPositionFirst:
		ctx.WriteString(" first")
	case ColumnPositionAfter:
		ctx.WriteString(" after ")
		node.RelativeColumn.Format(ctx)
	}
}

func (node ColumnPosition) TypeName() string { return "tree.ColumnPosition" }

func (node *ColumnPosition) reset() {
	// if node.RelativeColumn != nil {
	// node.RelatetionColumn.Free()
	// }
	*node = ColumnPosition{}
}

// AlterPartitionOptionType is the type for Alter Table Partition Option Type.
type AlterPartitionOptionType int

// Alter Table Partition types.
const (
	AlterPartitionAddPartition AlterPartitionOptionType = iota
	AlterPartitionDropPartition
	AlterPartitionDiscardPartition
	AlterPartitionImportPartition
	AlterPartitionTruncatePartition
	AlterPartitionCoalescePartition
	AlterPartitionReorganizePartition
	AlterPartitionExchangePartition
	AlterPartitionAnalyzePartition
	AlterPartitionCheckPartition
	AlterPartitionOptimizePartition
	AlterPartitionRebuildPartition
	AlterPartitionRepairPartition
	AlterPartitionRemovePartitioning
	AlterPartitionRedefinePartition
)

type AlterPartitionOption interface {
	NodeFormatter
}

type alterPartitionOptionImpl struct {
	AlterPartitionOption
}

func (node *alterPartitionOptionImpl) Free() {
	panic("should implement by child")
}

type AlterPartitionRedefinePartitionClause struct {
	alterPartitionOptionImpl
	Typ             AlterPartitionOptionType
	PartitionOption *PartitionOption
}

func NewAlterPartitionRedefinePartitionClause(typ AlterPartitionOptionType, partitionOption *PartitionOption) *AlterPartitionRedefinePartitionClause {
	a := reuse.Alloc[AlterPartitionRedefinePartitionClause](nil)
	a.Typ = typ
	a.PartitionOption = partitionOption
	return a
}

func (node *AlterPartitionRedefinePartitionClause) Free() {
	reuse.Free[AlterPartitionRedefinePartitionClause](node, nil)
}

func (node *AlterPartitionRedefinePartitionClause) Format(ctx *FmtCtx) {
	ctx.WriteString(" ")
	node.PartitionOption.Format(ctx)
}

func (node AlterPartitionRedefinePartitionClause) TypeName() string {
	return "tree.AlterPartitionRedefinePartitionClause"
}

func (node *AlterPartitionRedefinePartitionClause) reset() {
	if node.PartitionOption != nil {
		node.PartitionOption.Free()
	}
	*node = AlterPartitionRedefinePartitionClause{}
}

type AlterPartitionAddPartitionClause struct {
	alterPartitionOptionImpl
	Typ        AlterPartitionOptionType
	Partitions []*Partition
}

func NewAlterPartitionAddPartitionClause(typ AlterPartitionOptionType, partitions []*Partition) *AlterPartitionAddPartitionClause {
	a := reuse.Alloc[AlterPartitionAddPartitionClause](nil)
	a.Typ = typ
	a.Partitions = partitions
	return a
}

func (node *AlterPartitionAddPartitionClause) Free() {
	reuse.Free[AlterPartitionAddPartitionClause](node, nil)
}

func (node *AlterPartitionAddPartitionClause) Format(ctx *FmtCtx) {
	ctx.WriteString(" add partition (")
	isFirst := true
	for _, partition := range node.Partitions {
		if isFirst {
			partition.Format(ctx)
			isFirst = false
		} else {
			ctx.WriteString(", ")
			partition.Format(ctx)
		}
	}
	ctx.WriteString(")")
}

func (node AlterPartitionAddPartitionClause) TypeName() string {
	return "tree.AlterPartitionAddPartitionClause"
}

func (node *AlterPartitionAddPartitionClause) reset() {
	if node.Partitions != nil {
		for _, item := range node.Partitions {
			item.Free()
		}
	}
	*node = AlterPartitionAddPartitionClause{}
}

type AlterPartitionDropPartitionClause struct {
	alterPartitionOptionImpl
	Typ             AlterPartitionOptionType
	PartitionNames  IdentifierList
	OnAllPartitions bool
}

func NewAlterPartitionDropPartitionClause(typ AlterPartitionOptionType, partitionNames IdentifierList) *AlterPartitionDropPartitionClause {
	a := reuse.Alloc[AlterPartitionDropPartitionClause](nil)
	a.Typ = typ
	a.PartitionNames = partitionNames
	return a
}

func (node *AlterPartitionDropPartitionClause) Free() {
	reuse.Free[AlterPartitionDropPartitionClause](node, nil)
}

func (node *AlterPartitionDropPartitionClause) Format(ctx *FmtCtx) {
	ctx.WriteString(" drop partition ")
	node.PartitionNames.Format(ctx)
}

func (node AlterPartitionDropPartitionClause) TypeName() string {
	return "tree.AlterPartitionDropPartitionClause"
}

func (node *AlterPartitionDropPartitionClause) reset() {
	*node = AlterPartitionDropPartitionClause{}
}

type AlterPartitionTruncatePartitionClause struct {
	alterPartitionOptionImpl
	Typ             AlterPartitionOptionType
	PartitionNames  IdentifierList
	OnAllPartitions bool
}

func NewAlterPartitionTruncatePartitionClause(typ AlterPartitionOptionType, partitionNames IdentifierList) *AlterPartitionTruncatePartitionClause {
	a := reuse.Alloc[AlterPartitionTruncatePartitionClause](nil)
	a.Typ = typ
	a.PartitionNames = partitionNames
	return a
}

func (node *AlterPartitionTruncatePartitionClause) Free() {
	reuse.Free[AlterPartitionTruncatePartitionClause](node, nil)
}

func (node *AlterPartitionTruncatePartitionClause) Format(ctx *FmtCtx) {
	ctx.WriteString(" truncate partition ")
	if node.OnAllPartitions {
		ctx.WriteString("all")
	} else {
		node.PartitionNames.Format(ctx)
	}
}

func (node AlterPartitionTruncatePartitionClause) TypeName() string {
	return "tree.AlterPartitionTruncatePartitionClause"
}

func (node *AlterPartitionTruncatePartitionClause) reset() {
	*node = AlterPartitionTruncatePartitionClause{}
}
