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

type ShowGrants struct {
	showImpl
	Username string
	Hostname string
	Roles    []*Role
}

func (node *ShowGrants) Format(ctx *FmtCtx) {
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

func NewShowProcessList(f bool) *ShowProcessList {
	return &ShowProcessList{Full: f}
}

type ShowErrors struct {
	showImpl
}

func (node *ShowErrors) Format(ctx *FmtCtx) {
	ctx.WriteString("show errors")
}

func NewShowErrors() *ShowErrors {
	return &ShowErrors{}
}

type ShowWarnings struct {
	showImpl
}

func (node *ShowWarnings) Format(ctx *FmtCtx) {
	ctx.WriteString("show warnings")
}

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
		ctx.WriteString(" like ")
		node.Like.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
}

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

func NewShowIndex(t TableName, w *Where) *ShowIndex {
	return &ShowIndex{
		TableName: t,
		Where:     w,
	}
}
