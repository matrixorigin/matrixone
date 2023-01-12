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

type Show interface {
	Explain
}

type showImpl struct {
	Show
}

// SHOW CREATE TABLE statement
type ShowCreateTable struct {
	showImpl
	Name *UnresolvedObjectName
}

func (node *ShowCreateTable) Format(ctx *FmtCtx) {
	ctx.WriteString("show create table ")
	node.Name.ToTableName().Format(ctx)
}

func (node *ShowCreateTable) GetStatementType() string { return "Show Create Table" }
func (node *ShowCreateTable) GetQueryType() string     { return QueryTypeDQL }

func NewShowCreate(n *UnresolvedObjectName) *ShowCreateTable {
	return &ShowCreateTable{Name: n}
}

// SHOW CREATE VIEW statement
type ShowCreateView struct {
	showImpl
	Name *UnresolvedObjectName
}

func (node *ShowCreateView) Format(ctx *FmtCtx) {
	ctx.WriteString("show create view ")
	node.Name.ToTableName().Format(ctx)
}
func (node *ShowCreateView) GetStatementType() string { return "Show Create View" }
func (node *ShowCreateView) GetQueryType() string     { return QueryTypeDQL }

func NewShowCreateView(n *UnresolvedObjectName) *ShowCreateView {
	return &ShowCreateView{Name: n}
}

// SHOW CREATE DATABASE statement
type ShowCreateDatabase struct {
	showImpl
	IfNotExists bool
	Name        string
}

func (node *ShowCreateDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("show create database")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	ctx.WriteByte(' ')
	ctx.WriteString(string(node.Name))
}
func (node *ShowCreateDatabase) GetStatementType() string { return "Show Create View" }
func (node *ShowCreateDatabase) GetQueryType() string     { return QueryTypeDQL }

func NewShowCreateDatabase(i bool, n string) *ShowCreateDatabase {
	return &ShowCreateDatabase{IfNotExists: i, Name: n}
}

// SHOW COLUMNS statement.
type ShowColumns struct {
	showImpl
	Ext     bool
	Full    bool
	Table   *UnresolvedObjectName
	ColName *UnresolvedName
	DBName  string
	Like    *ComparisonExpr
	Where   *Where
}

func (node *ShowColumns) Format(ctx *FmtCtx) {
	ctx.WriteString("show")
	if node.Ext {
		ctx.WriteString(" extended")
	}
	if node.Full {
		ctx.WriteString(" full")
	}
	ctx.WriteString(" columns")
	if node.Table != nil {
		ctx.WriteString(" from ")
		node.Table.Format(ctx)
	}
	if node.DBName != "" {
		ctx.WriteString(" from ")
		ctx.WriteString(node.DBName)
	}
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowColumns) GetStatementType() string { return "Show Columns" }
func (node *ShowColumns) GetQueryType() string     { return QueryTypeDQL }

func NewShowColumns(e bool, f bool, t *UnresolvedObjectName, d string, l *ComparisonExpr, w *Where, cn *UnresolvedName) *ShowColumns {
	return &ShowColumns{
		Ext:     e,
		Full:    f,
		Table:   t,
		ColName: cn,
		DBName:  d,
		Like:    l,
		Where:   w,
	}
}

// the SHOW DATABASES statement.
type ShowDatabases struct {
	showImpl
	Like  *ComparisonExpr
	Where *Where
}

func (node *ShowDatabases) Format(ctx *FmtCtx) {
	ctx.WriteString("show databases")
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowDatabases) GetStatementType() string { return "Show Databases" }
func (node *ShowDatabases) GetQueryType() string     { return QueryTypeDQL }

func NewShowDatabases(l *ComparisonExpr, w *Where) *ShowDatabases {
	return &ShowDatabases{
		Like:  l,
		Where: w,
	}
}

type ShowType int

const (
	ShowEngines = iota
	ShowCharset
	ShowCreateUser
	ShowTriggers
	ShowProcedureStatus
	ShowConfig
	ShowEvents
	ShowPlugins
	ShowProfile
	ShowProfiles
	ShowPrivileges
)

func (s ShowType) String() string {
	switch s {
	case ShowEngines:
		return "engines"
	case ShowCharset:
		return "charset"
	case ShowCreateUser:
		return "create user"
	case ShowTriggers:
		return "triggers"
	case ShowProcedureStatus:
		return "procedure status"
	case ShowConfig:
		return "config"
	case ShowEvents:
		return "events"
	case ShowPlugins:
		return "plugins"
	case ShowProfile:
		return "profile"
	case ShowProfiles:
		return "profiles"
	case ShowPrivileges:
		return "privileges"
	default:
		return "not implemented"
	}
}

type ShowTarget struct {
	showImpl
	Global bool
	Type   ShowType
	DbName string
	Like   *ComparisonExpr
	Where  *Where
}

func (node *ShowTarget) Format(ctx *FmtCtx) {
	ctx.WriteString("show ")
	if node.Global {
		ctx.WriteString("global ")
	}
	ctx.WriteString(node.Type.String())
	if node.DbName != "" {
		ctx.WriteString(" from ")
		ctx.WriteString(node.DbName)
	}
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowTarget) GetStatementType() string { return "Show Target" }
func (node *ShowTarget) GetQueryType() string     { return QueryTypeDQL }

type ShowTableStatus struct {
	showImpl
	DbName string
	Like   *ComparisonExpr
	Where  *Where
}

func (node *ShowTableStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("show table status")
	if node.DbName != "" {
		ctx.WriteString(" from ")
		ctx.WriteString(node.DbName)
	}
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowTableStatus) GetStatementType() string { return "Show Table Status" }
func (node *ShowTableStatus) GetQueryType() string     { return QueryTypeDQL }

type ShowGrants struct {
	showImpl
	Username      string
	Hostname      string
	Roles         []*Role
	ShowGrantType ShowGrantType
}

type ShowGrantType int

const (
	GrantForUser = iota
	GrantForRole
)

func (node *ShowGrants) Format(ctx *FmtCtx) {
	if node.ShowGrantType == GrantForRole {
		ctx.WriteString("show grants")
		if node.Roles != nil {
			ctx.WriteString("for")
			ctx.WriteString(" ")
			ctx.WriteString(node.Roles[0].UserName)
		}
	} else {
		ctx.WriteString("show grants")
		if node.Username != "" {
			ctx.WriteString(" for ")
			ctx.WriteString(node.Username)
			if node.Hostname != "" {
				ctx.WriteString("@")
				ctx.WriteString(node.Hostname)
			}
		}
		if node.Roles != nil {
			prefix := ""
			for _, r := range node.Roles {
				ctx.WriteString(prefix)
				r.Format(ctx)
				prefix = ", "
			}
		}
	}
}
func (node *ShowGrants) GetStatementType() string { return "Show Grants" }
func (node *ShowGrants) GetQueryType() string     { return QueryTypeDQL }

// SHOW TABLES statement.
type ShowTables struct {
	showImpl
	Ext    bool
	Open   bool
	Full   bool
	DBName string
	Like   *ComparisonExpr
	Where  *Where
}

func (node *ShowTables) Format(ctx *FmtCtx) {
	ctx.WriteString("show")
	if node.Open {
		ctx.WriteString(" open")
	}
	if node.Full {
		ctx.WriteString(" full")
	}
	ctx.WriteString(" tables")
	if node.DBName != "" {
		ctx.WriteString(" from ")
		ctx.WriteString(node.DBName)
	}
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowTables) GetStatementType() string { return "Show Tables" }
func (node *ShowTables) GetQueryType() string     { return QueryTypeDQL }

func NewShowTables(e bool, f bool, n string, l *ComparisonExpr, w *Where) *ShowTables {
	return &ShowTables{
		Ext:    e,
		Full:   f,
		DBName: n,
		Like:   l,
		Where:  w,
	}
}

// SHOW PROCESSLIST
type ShowProcessList struct {
	showImpl
	Full bool
}

func (node *ShowProcessList) Format(ctx *FmtCtx) {
	ctx.WriteString("show")
	if node.Full {
		ctx.WriteString(" full")
	}
	ctx.WriteString(" processlist")
}
func (node *ShowProcessList) GetStatementType() string { return "Show Processlist" }
func (node *ShowProcessList) GetQueryType() string     { return QueryTypeDQL }

func NewShowProcessList(f bool) *ShowProcessList {
	return &ShowProcessList{Full: f}
}

type ShowErrors struct {
	showImpl
}

func (node *ShowErrors) Format(ctx *FmtCtx) {
	ctx.WriteString("show errors")
}
func (node *ShowErrors) GetStatementType() string { return "Show Errors" }
func (node *ShowErrors) GetQueryType() string     { return QueryTypeDQL }

func NewShowErrors() *ShowErrors {
	return &ShowErrors{}
}

type ShowWarnings struct {
	showImpl
}

func (node *ShowWarnings) Format(ctx *FmtCtx) {
	ctx.WriteString("show warnings")
}
func (node *ShowWarnings) GetStatementType() string { return "Show Warnings" }
func (node *ShowWarnings) GetQueryType() string     { return QueryTypeDQL }

func NewShowWarnings() *ShowWarnings {
	return &ShowWarnings{}
}

// SHOW collation statement
type ShowCollation struct {
	showImpl
	Like  *ComparisonExpr
	Where *Where
}

func (node *ShowCollation) Format(ctx *FmtCtx) {
	ctx.WriteString("show collation")
	if node.Like != nil {
		ctx.WriteString(" like ")
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowCollation) GetStatementType() string { return "Show Collation" }
func (node *ShowCollation) GetQueryType() string     { return QueryTypeDQL }

// SHOW VARIABLES statement
// System Variables
type ShowVariables struct {
	showImpl
	Global bool
	Like   *ComparisonExpr
	Where  *Where
}

func (node *ShowVariables) Format(ctx *FmtCtx) {
	ctx.WriteString("show")
	if node.Global {
		ctx.WriteString(" global")
	}
	ctx.WriteString(" variables")
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowVariables) GetStatementType() string { return "Show Variables" }
func (node *ShowVariables) GetQueryType() string     { return QueryTypeDQL }

func NewShowVariables(g bool, l *ComparisonExpr, w *Where) *ShowVariables {
	return &ShowVariables{
		Global: g,
		Like:   l,
		Where:  w,
	}
}

// SHOW STATUS statement
type ShowStatus struct {
	showImpl
	Global bool
	Like   *ComparisonExpr
	Where  *Where
}

func (node *ShowStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("show")
	if node.Global {
		ctx.WriteString(" global")
	}
	ctx.WriteString(" status")
	if node.Like != nil {
		ctx.WriteString(" like ")
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowStatus) GetStatementType() string { return "Show Status" }
func (node *ShowStatus) GetQueryType() string     { return QueryTypeDQL }

func NewShowStatus(g bool, l *ComparisonExpr, w *Where) *ShowStatus {
	return &ShowStatus{
		Global: g,
		Like:   l,
		Where:  w,
	}
}

// show index statement
type ShowIndex struct {
	showImpl
	TableName TableName
	Where     *Where
}

func (node *ShowIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("show index from ")
	node.TableName.Format(ctx)
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}
func (node *ShowIndex) GetStatementType() string { return "Show Index" }
func (node *ShowIndex) GetQueryType() string     { return QueryTypeDQL }

func NewShowIndex(t TableName, w *Where) *ShowIndex {
	return &ShowIndex{
		TableName: t,
		Where:     w,
	}
}

// show Function statement

type ShowFunctionStatus struct {
	showImpl
	Like  *ComparisonExpr
	Where *Where
}

func (node *ShowFunctionStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("show function status")
	if node.Like != nil {
		ctx.WriteString(" like ")
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}

func (node *ShowFunctionStatus) GetStatementType() string { return "Show Function Status" }
func (node *ShowFunctionStatus) GetQueryType() string     { return QueryTypeDQL }

func NewShowFunctionStatus(l *ComparisonExpr, w *Where) *ShowFunctionStatus {
	return &ShowFunctionStatus{
		Like:  l,
		Where: w,
	}
}

// show node list
type ShowNodeList struct {
	showImpl
}

func (node *ShowNodeList) Format(ctx *FmtCtx) {
	ctx.WriteString("show node list")
}

func (node *ShowNodeList) GetStatementType() string { return "Show Node List" }
func (node *ShowNodeList) GetQueryType() string     { return QueryTypeDQL }

func NewShowNodeList() *ShowNodeList {
	return &ShowNodeList{}
}

// show locks
type ShowLocks struct {
	showImpl
}

func (node *ShowLocks) Format(ctx *FmtCtx) {
	ctx.WriteString("show locks")
}

func (node *ShowLocks) GetStatementType() string { return "Show Locks" }
func (node *ShowLocks) GetQueryType() string     { return QueryTypeDQL }

func NewShowLocks() *ShowLocks {
	return &ShowLocks{}
}

// show table number
type ShowTableNumber struct {
	showImpl
	DbName string
}

func (node *ShowTableNumber) Format(ctx *FmtCtx) {
	ctx.WriteString("show table number")
	if node.DbName != "" {
		ctx.WriteString(" from ")
		ctx.WriteString(node.DbName)
	}
}
func (node *ShowTableNumber) GetStatementType() string { return "Show Table Number" }
func (node *ShowTableNumber) GetQueryType() string     { return QueryTypeDQL }

func NewShowTableNumber(dbname string) *ShowTableNumber {
	return &ShowTableNumber{
		DbName: dbname,
	}
}

// show column number
type ShowColumnNumber struct {
	showImpl
	Table  *UnresolvedObjectName
	DbName string
}

func (node *ShowColumnNumber) Format(ctx *FmtCtx) {
	ctx.WriteString("show column number")
	if node.Table != nil {
		ctx.WriteString(" from ")
		node.Table.Format(ctx)
	}
	if node.DbName != "" {
		ctx.WriteString(" from ")
		ctx.WriteString(node.DbName)
	}
}
func (node *ShowColumnNumber) GetStatementType() string { return "Show Column Number" }
func (node *ShowColumnNumber) GetQueryType() string     { return QueryTypeDQL }

func NewShowColumnNumber(table *UnresolvedObjectName, dbname string) *ShowColumnNumber {
	return &ShowColumnNumber{
		Table:  table,
		DbName: dbname,
	}
}

// show table values
type ShowTableValues struct {
	showImpl
	Table  *UnresolvedObjectName
	DbName string
}

func (node *ShowTableValues) Format(ctx *FmtCtx) {
	ctx.WriteString("show table values")
	if node.Table != nil {
		ctx.WriteString(" from ")
		node.Table.Format(ctx)
	}
	if node.DbName != "" {
		ctx.WriteString(" from ")
		ctx.WriteString(node.DbName)
	}
}
func (node *ShowTableValues) GetStatementType() string { return "Show Table Values" }
func (node *ShowTableValues) GetQueryType() string     { return QueryTypeDQL }

func NewShowTableValues(table *UnresolvedObjectName, dbname string) *ShowTableValues {
	return &ShowTableValues{
		Table:  table,
		DbName: dbname,
	}
}

type ShowAccounts struct {
	showImpl
	Like *ComparisonExpr
}

func (node *ShowAccounts) Format(ctx *FmtCtx) {
	ctx.WriteString("show accounts")
	if node.Like != nil {
		ctx.WriteByte(' ')
		node.Like.Format(ctx)
	}
}

func (node *ShowAccounts) GetStatementType() string { return "Show Accounts" }
func (node *ShowAccounts) GetQueryType() string     { return QueryTypeDQL }
