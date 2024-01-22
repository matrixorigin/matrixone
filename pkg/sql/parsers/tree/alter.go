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
		func( a *AlterUser) { a.reset() },
		reuse.DefaultOptions[AlterUser]().
			WithEnableChecker())

	reuse.CreatePool[AlterAccountAuthOption](
		func() *AlterAccountAuthOption { return &AlterAccountAuthOption{} },
		func( a *AlterAccountAuthOption) { a.reset() },
		reuse.DefaultOptions[AlterAccountAuthOption]().
			WithEnableChecker())

	reuse.CreatePool[AlterAccount](
		func() *AlterAccount { return &AlterAccount{} },
		func( a *AlterAccount) { a.reset() },
		reuse.DefaultOptions[AlterAccount]().
			WithEnableChecker())

	reuse.CreatePool[AlterView](
		func() *AlterView { return &AlterView{} },
		func( a *AlterView) { a.reset() },
		reuse.DefaultOptions[AlterView]().
			WithEnableChecker())

	reuse.CreatePool[AlterDataBaseConfig](
		func() *AlterDataBaseConfig { return &AlterDataBaseConfig{} },
		func( a *AlterDataBaseConfig) { a.reset() },
		reuse.DefaultOptions[AlterDataBaseConfig]().
			WithEnableChecker())

	reuse.CreatePool[AlterTable](
		func() *AlterTable { return &AlterTable{} },
		func( a *AlterTable) { a.reset() },
		reuse.DefaultOptions[AlterTable]().
			WithEnableChecker())

	reuse.CreatePool[AlterOptionAlterIndex](
		func() *AlterOptionAlterIndex { return &AlterOptionAlterIndex{} },
		func( a *AlterOptionAlterIndex) { a.reset() },
		reuse.DefaultOptions[AlterOptionAlterIndex]().
			WithEnableChecker())

	reuse.CreatePool[AlterOptionAlterReIndex](
		func() *AlterOptionAlterReIndex { return &AlterOptionAlterReIndex{} },
		func( a *AlterOptionAlterReIndex) { a.reset() },
		reuse.DefaultOptions[AlterOptionAlterReIndex]().
			WithEnableChecker())

	reuse.CreatePool[AlterOptionAlterCheck](
		func() *AlterOptionAlterCheck { return &AlterOptionAlterCheck{} },
		func( a *AlterOptionAlterCheck) { a.reset() },
		reuse.DefaultOptions[AlterOptionAlterCheck]().
			WithEnableChecker())

	reuse.CreatePool[AlterOptionAdd](
		func() *AlterOptionAdd { return &AlterOptionAdd{} },
		func( a *AlterOptionAdd) { a.reset() },
		reuse.DefaultOptions[AlterOptionAdd]().
			WithEnableChecker())

	reuse.CreatePool[AlterOptionDrop](
		func() *AlterOptionDrop { return &AlterOptionDrop{} },
		func( a *AlterOptionDrop) { a.reset() },
		reuse.DefaultOptions[AlterOptionDrop]().
			WithEnableChecker())

	reuse.CreatePool[AlterTableName](
		func() *AlterTableName { return &AlterTableName{} },
		func( a *AlterTableName) { a.reset() },
		reuse.DefaultOptions[AlterTableName]().
			WithEnableChecker())

	reuse.CreatePool[AlterAddCol](
		func() *AlterAddCol { return &AlterAddCol{} },
		func( a *AlterAddCol) { a.reset() },
		reuse.DefaultOptions[AlterAddCol]().
			WithEnableChecker())

	reuse.CreatePool[AlterPublication](
		func() *AlterPublication { return &AlterPublication{} },
		func( a *AlterPublication) { a.reset() },
		reuse.DefaultOptions[AlterPublication]().
			WithEnableChecker())

	reuse.CreatePool[AlterTableModifyColumnClause](
		func() *AlterTableModifyColumnClause { return &AlterTableModifyColumnClause{} },
		func( a *AlterTableModifyColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableModifyColumnClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterTableChangeColumnClause](
		func() *AlterTableChangeColumnClause { return &AlterTableChangeColumnClause{} },
		func( a *AlterTableChangeColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableChangeColumnClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterTableAddColumnClause](
		func() *AlterTableAddColumnClause { return &AlterTableAddColumnClause{} },
		func( a *AlterTableAddColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableAddColumnClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterTableRenameColumnClause](
		func() *AlterTableRenameColumnClause { return &AlterTableRenameColumnClause{} },
		func( a *AlterTableRenameColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableRenameColumnClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterTableAlterColumnClause](
		func() *AlterTableAlterColumnClause { return &AlterTableAlterColumnClause{} },
		func( a *AlterTableAlterColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableAlterColumnClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterTableOrderByColumnClause](
		func() *AlterTableOrderByColumnClause { return &AlterTableOrderByColumnClause{} },
		func( a *AlterTableOrderByColumnClause) { a.reset() },
		reuse.DefaultOptions[AlterTableOrderByColumnClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterColumnOrder](
		func() *AlterColumnOrder { return &AlterColumnOrder{} },
		func( a *AlterColumnOrder) { a.reset() },
		reuse.DefaultOptions[AlterColumnOrder]().
			WithEnableChecker())

	reuse.CreatePool[ColumnPosition](
		func() *ColumnPosition { return &ColumnPosition{} },
		func( c *ColumnPosition) { c.reset() },
		reuse.DefaultOptions[ColumnPosition]().
			WithEnableChecker())

	reuse.CreatePool[AlterPartitionRedefinePartitionClause](
		func() *AlterPartitionRedefinePartitionClause { return &AlterPartitionRedefinePartitionClause{} },
		func( a *AlterPartitionRedefinePartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionRedefinePartitionClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterPartitionAddPartitionClause](
		func() *AlterPartitionAddPartitionClause { return &AlterPartitionAddPartitionClause{} },
		func( a *AlterPartitionAddPartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionAddPartitionClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterPartitionDropPartitionClause](
		func() *AlterPartitionDropPartitionClause { return &AlterPartitionDropPartitionClause{} },
		func( a *AlterPartitionDropPartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionDropPartitionClause]().
			WithEnableChecker())

	reuse.CreatePool[AlterPartitionTruncatePartitionClause](
		func() *AlterPartitionTruncatePartitionClause { return &AlterPartitionTruncatePartitionClause{} },
		func( a *AlterPartitionTruncatePartitionClause) { a.reset() },
		reuse.DefaultOptions[AlterPartitionTruncatePartitionClause]().
			WithEnableChecker())

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


func (node *AlterUser) Free() {
	reuse.Free[AlterUser](node, nil)
}

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

func (node *AlterUser) GetStatementType() string { return "Alter User" }
func (node *AlterUser) GetQueryType() string     { return QueryTypeDCL }

func NewAlterUser(ifExists bool, users []*User, role *Role, miscOpt UserMiscOption, commentOrAttribute AccountCommentOrAttribute) *AlterUser {
	alter := reuse.Alloc[AlterUser](nil)
	alter.IfExists = ifExists
	alter.Users = users
	alter.Role = role
	alter.MiscOpt = miscOpt
	alter.CommentOrAttribute = commentOrAttribute

	return alter
}

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

type AlterAccount struct {
	statementImpl
	IfExists   bool
	Name       string
	AuthOption AlterAccountAuthOption
	//status_option or not
	StatusOption AccountStatus
	//comment or not
	Comment AccountComment
}

func (ca *AlterAccount) Format(ctx *FmtCtx) {
	ctx.WriteString("alter account ")
	if ca.IfExists {
		ctx.WriteString("if exists ")
	}
	ctx.WriteString(ca.Name)
	ca.AuthOption.Format(ctx)
	ca.StatusOption.Format(ctx)
	ca.Comment.Format(ctx)
}

func (ca *AlterAccount) GetStatementType() string { return "Alter Account" }
func (ca *AlterAccount) GetQueryType() string     { return QueryTypeDCL }

type AlterView struct {
	statementImpl
	IfExists bool
	Name     *TableName
	ColNames IdentifierList
	AsSource *Select
}

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

// alter configuration for mo_mysql_compatibility_mode
type AlterDataBaseConfig struct {
	statementImpl
	AccountName    string
	DbName         string
	IsAccountLevel bool
	UpdateConfig   string
}

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

// AlterTable
// see https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
type AlterTable struct {
	statementImpl
	Table            *TableName
	Options          AlterTableOptions
	PartitionOptions AlterPartitionOption
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

	if node.PartitionOptions != nil {
		node.PartitionOptions.Format(ctx)
	}

}

func (node *AlterTable) GetStatementType() string { return "Alter Table" }
func (node *AlterTable) GetQueryType() string     { return QueryTypeDDL }

type AlterTableOptions = []AlterTableOption

type AlterTableOption interface {
	NodeFormatter
}

type alterOptionImpl struct {
	AlterTableOption
}

type AlterOptionAlterIndex struct {
	alterOptionImpl
	Name       Identifier
	Visibility VisibleType
}

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

type AlterOptionAlterReIndex struct {
	alterOptionImpl
	Name          Identifier
	KeyType       IndexType
	AlgoParamList int64
}

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

type AlterOptionAlterCheck struct {
	alterOptionImpl
	Type    string
	Enforce bool
}

func (node *AlterOptionAlterCheck) Format(ctx *FmtCtx) {
	ctx.WriteString("alter ")
	ctx.WriteString(node.Type + " ")
	if node.Enforce {
		ctx.WriteString("enforce")
	} else {
		ctx.WriteString("not enforce")
	}
}

type AlterOptionAdd struct {
	alterOptionImpl
	Def TableDef
}

func (node *AlterOptionAdd) Format(ctx *FmtCtx) {
	ctx.WriteString("add ")
	node.Def.Format(ctx)
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

type AlterTableName struct {
	Name *UnresolvedObjectName
}

func (node *AlterTableName) Format(ctx *FmtCtx) {
	ctx.WriteString("rename to ")
	node.Name.ToTableName().Format(ctx)
}

type AlterColPos struct {
	PreColName *UnresolvedName
	Pos        int32
}

// suggest rename: AlterAddColumnPosition
type AlterAddCol struct {
	Column   *ColumnTableDef
	Position *ColumnPosition
}

func (node *AlterAddCol) Format(ctx *FmtCtx) {
	ctx.WriteString("add column ")
	node.Column.Format(ctx)
	node.Position.Format(ctx)
}

type AccountsSetOption struct {
	All          bool
	SetAccounts  IdentifierList
	AddAccounts  IdentifierList
	DropAccounts IdentifierList
}

type AlterPublication struct {
	statementImpl
	IfExists    bool
	Name        Identifier
	AccountsSet *AccountsSetOption
	Comment     string
}

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

type AlterTableModifyColumnClause struct {
	alterOptionImpl
	Typ       AlterTableOptionType
	NewColumn *ColumnTableDef
	Position  *ColumnPosition
}

func (node *AlterTableModifyColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("modify column ")
	node.NewColumn.Format(ctx)
	node.Position.Format(ctx)
}

type AlterTableChangeColumnClause struct {
	alterOptionImpl
	Typ           AlterTableOptionType
	OldColumnName *UnresolvedName
	NewColumn     *ColumnTableDef
	Position      *ColumnPosition
}

func (node *AlterTableChangeColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("change column")
	ctx.WriteString(" ")
	node.OldColumnName.Format(ctx)
	ctx.WriteString(" ")
	node.NewColumn.Format(ctx)
	node.Position.Format(ctx)
}

type AlterTableAddColumnClause struct {
	alterOptionImpl
	Typ        AlterTableOptionType
	NewColumns []*ColumnTableDef
	Position   *ColumnPosition
	// when Position is not none, the len(NewColumns) must be one
}

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

type AlterTableRenameColumnClause struct {
	alterOptionImpl
	Typ           AlterTableOptionType
	OldColumnName *UnresolvedName
	NewColumnName *UnresolvedName
}

func (node *AlterTableRenameColumnClause) Format(ctx *FmtCtx) {
	ctx.WriteString("rename column ")
	node.OldColumnName.Format(ctx)
	ctx.WriteString(" to ")
	node.NewColumnName.Format(ctx)
}

// AlterColumnOptionType is the type for AlterTableAlterColumn
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

type AlterTableOrderByColumnClause struct {
	alterOptionImpl
	Typ              AlterTableOptionType
	AlterOrderByList []*AlterColumnOrder
}

func (node *AlterTableOrderByColumnClause) Format(ctx *FmtCtx) {
	prefix := "order by "
	for _, columnOrder := range node.AlterOrderByList {
		ctx.WriteString(prefix)
		columnOrder.Format(ctx)
		prefix = ", "
	}
}

type AlterColumnOrder struct {
	Column    *UnresolvedName
	Direction Direction
}

func (node *AlterColumnOrder) Format(ctx *FmtCtx) {
	node.Column.Format(ctx)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
}

// AlterTableType is the type for AlterTableOptionType.
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

func (node *ColumnPosition) Format(ctx *FmtCtx) {
	switch node.Typ {
	case ColumnPositionNone:
		// do nothing
	case ColumnPositionFirst:
		ctx.WriteString(" first")
	case ColumnPositionAfter:
		ctx.WriteString(" after ")
		node.RelativeColumn.Format(ctx)
	}
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

type AlterPartitionOptionImpl struct {
	AlterPartitionOption
}

type AlterPartitionRedefinePartitionClause struct {
	AlterPartitionOptionImpl
	Typ             AlterPartitionOptionType
	PartitionOption *PartitionOption
}

func (node *AlterPartitionRedefinePartitionClause) Format(ctx *FmtCtx) {
	ctx.WriteString(" ")
	node.PartitionOption.Format(ctx)
}

type AlterPartitionAddPartitionClause struct {
	AlterPartitionOptionImpl
	Typ        AlterPartitionOptionType
	Partitions []*Partition
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

type AlterPartitionDropPartitionClause struct {
	AlterPartitionOptionImpl
	Typ             AlterPartitionOptionType
	PartitionNames  IdentifierList
	OnAllPartitions bool
}

func (node *AlterPartitionDropPartitionClause) Format(ctx *FmtCtx) {
	ctx.WriteString(" drop partition ")
	node.PartitionNames.Format(ctx)
}

type AlterPartitionTruncatePartitionClause struct {
	AlterPartitionOptionImpl
	Typ             AlterPartitionOptionType
	PartitionNames  IdentifierList
	OnAllPartitions bool
}

func (node *AlterPartitionTruncatePartitionClause) Format(ctx *FmtCtx) {
	ctx.WriteString(" truncate partition ")
	if node.OnAllPartitions {
		ctx.WriteString("all")
	} else {
		node.PartitionNames.Format(ctx)
	}
}

func (node AlterUser) TypeName() string { return "tree.AlterUser" }

func (node AlterAccountAuthOption) TypeName() string { return "tree.AlterAccountAuthOption"}
func (node AlterAccount) TypeName() string { return "tree.AlterAccount"}
func (node AlterView) TypeName() string { return "tree.AlterView"}
func (node AlterDataBaseConfig) TypeName() string { return "tree.AlterDataBaseConfig"}
func (node AlterTable) TypeName() string { return "tree.AlterTable"}
func (node AlterOptionAlterIndex) TypeName() string { return "tree.AlterOptionAlterIndex"}
func (node AlterOptionAlterReIndex) TypeName() string { return "tree.AlterOptionAlterReIndex"}
func (node AlterOptionAlterCheck) TypeName() string { return "tree.AlterOptionAlterCheck"}
func (node AlterOptionAdd) TypeName() string { return "tree.AlterOptionAdd"}
func (node AlterOptionDrop) TypeName() string { return "tree.AlterOptionDrop"}
func (node AlterTableName) TypeName() string { return "tree.AlterTableName"}
func (node AlterAddCol) TypeName() string { return "tree.AlterAddCol"}
func (node AlterPublication) TypeName() string { return "tree.AlterPublication"}
func (node AlterTableModifyColumnClause) TypeName() string { return "tree.AlterTableModifyColumnClause"}
func (node AlterTableChangeColumnClause) TypeName() string { return "tree.AlterTableChangeColumnClause"}
func (node AlterTableAddColumnClause) TypeName() string { return "tree.AlterTableAddColumnClause"}
func (node AlterTableRenameColumnClause) TypeName() string { return "tree.AlterTableRenameColumnClause"}
func (node AlterTableAlterColumnClause) TypeName() string { return "tree.AlterTableAlterColumnClause"}
func (node AlterTableOrderByColumnClause) TypeName() string { return "tree.AlterTableOrderByColumnClause"}
func (node AlterColumnOrder) TypeName() string { return "tree.AlterColumnOrder"}
func (node ColumnPosition) TypeName() string { return "tree.ColumnPosition"}
func (node AlterPartitionRedefinePartitionClause) TypeName() string { return "tree.AlterPartitionRedefinePartitionClause"}
func (node AlterPartitionAddPartitionClause) TypeName() string { return "tree.AlterPartitionAddPartitionClause"}
func (node AlterPartitionDropPartitionClause) TypeName() string { return "tree.AlterPartitionDropPartitionClause"}
func (node AlterPartitionTruncatePartitionClause) TypeName() string { return "tree.AlterPartitionTruncatePartitionClause"}

func (node *AlterUser) reset() {
	// if node.Users != nil {
	// 	for _, item := range node.Users {
	// 		reuse.Free[User](item, nil)
	// 	}
	// }
	// if node.Role != nil {
	// 	reuse.Free[Role](node.Role, nil)
	// }
}


func (node *AlterAccountAuthOption) reset() {
}


func (node *AlterAccount) reset() {
}


func (node *AlterView) reset() {
	// if node.Name != nil {
	// 	reuse.Free[TableName](node.Name, nil)
	// }
	// if node.AsSource != nil {
	// 	reuse.Free[Select](node.AsSource, nil)
	// }
}


func (node *AlterDataBaseConfig) reset() {
}


func (node *AlterTable) reset() {
	// if node.Table != nil {
	// 	reuse.Free[TableName](node.Table, nil)
	// }
}


func (node *AlterOptionAlterIndex) reset() {
}


func (node *AlterOptionAlterReIndex) reset() {
}


func (node *AlterOptionAlterCheck) reset() {
}


func (node *AlterOptionAdd) reset() {
}


func (node *AlterOptionDrop) reset() {
}


func (node *AlterTableName) reset() {
	// if node.Name != nil {
	// 	reuse.Free[UnresolvedObjectName](node.Name, nil)
	// }
}


func (node *AlterAddCol) reset() {
	// if node.Column != nil {
	// 	reuse.Free[ColumnTableDef](node.Column, nil)
	// }
	// if node.Position != nil {
	// 	reuse.Free[ColumnPosition](node.Position, nil)
	// }
}


func (node *AlterPublication) reset() {
	// if node.AccountsSet != nil {
	// 	reuse.Free[AccountsSetOption](node.AccountsSet, nil)
	// }
}


func (node *AlterTableModifyColumnClause) reset() {
	// if node.NewColumn != nil {
	// 	reuse.Free[ColumnTableDef](node.NewColumn, nil)
	// }
	if node.Position != nil {
		reuse.Free[ColumnPosition](node.Position, nil)
	}
}


func (node *AlterTableChangeColumnClause) reset() {
	// if node.OldColumnName != nil {
	// 	reuse.Free[UnresolvedName](node.OldColumnName, nil)
	// }
	// if node.NewColumn != nil {
	// 	reuse.Free[ColumnTableDef](node.NewColumn, nil)
	// }
	if node.Position != nil {
		reuse.Free[ColumnPosition](node.Position, nil)
	}
}


func (node *AlterTableAddColumnClause) reset() {
	// if node.NewColumns != nil {
	// 	for _, item := range node.NewColumns {
	// 		reuse.Free[ColumnTableDef](item, nil)
	// 	}
	// }
	if node.Position != nil {
		reuse.Free[ColumnPosition](node.Position, nil)
	}
}


func (node *AlterTableRenameColumnClause) reset() {
	// if node.OldColumnName != nil {
	// 	reuse.Free[UnresolvedName](node.OldColumnName, nil)
	// }
	// if node.NewColumnName != nil {
	// 	reuse.Free[UnresolvedName](node.NewColumnName, nil)
	// }
}


func (node *AlterTableAlterColumnClause) reset() {
	// if node.ColumnName != nil {
	// 	reuse.Free[UnresolvedName](node.ColumnName, nil)
	// }
	// if node.DefalutExpr != nil {
	// 	reuse.Free[AttributeDefault](node.DefalutExpr, nil)
	// }
}


func (node *AlterTableOrderByColumnClause) reset() {
	if node.AlterOrderByList != nil {
		for _, item := range node.AlterOrderByList {
			reuse.Free[AlterColumnOrder](item, nil)
		}
	}
}


func (node *AlterColumnOrder) reset() {
	// if node.Column != nil {
	// 	reuse.Free[UnresolvedName](node.Column, nil)
	// }
}


func (node *ColumnPosition) reset() {
	// if node.RelativeColumn != nil {
	// 	reuse.Free[UnresolvedName](node.RelativeColumn, nil)
	// }
}


func (node *AlterPartitionRedefinePartitionClause) reset() {
	// if node.PartitionOption != nil {
	// 	reuse.Free[PartitionOption](node.PartitionOption, nil)
	// }
}


func (node *AlterPartitionAddPartitionClause) reset() {
	// if node.Partitions != nil {
	// 	for _, item := range node.Partitions {
	// 		reuse.Free[Partition](item, nil)
	// 	}
	// }
}


func (node *AlterPartitionDropPartitionClause) reset() {
}


func (node *AlterPartitionTruncatePartitionClause) reset() {
}

