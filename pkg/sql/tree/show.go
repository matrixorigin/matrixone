package tree

type Show interface {
	Explain
}

type showImpl struct {
	Show
}

//SHOW CREATE TABLE statement
type ShowCreate struct {
	showImpl
	Name *UnresolvedObjectName
}

func NewShowCreate(n *UnresolvedObjectName) *ShowCreate  {
	return &ShowCreate{Name: n}
}

//SHOW CREATE DATABASE statement
type ShowCreateDatabase struct {
	showImpl
	IfNotExists bool
	Name string
}

func NewShowCreateDatabase(i bool,n string) *ShowCreateDatabase  {
	return &ShowCreateDatabase{IfNotExists: i,Name: n}
}

//SHOW COLUMNS statement.
type ShowColumns struct {
	showImpl
	Ext bool
	Full bool
	Table       *UnresolvedObjectName
	ColName *UnresolvedName
	DBName string
	Like *ComparisonExpr
	Where *Where
}

func NewShowColumns(e bool, f bool, t *UnresolvedObjectName, d string, l *ComparisonExpr, w *Where, cn *UnresolvedName) *ShowColumns {
	return &ShowColumns{
		Ext:      e,
		Full:     f,
		Table:    t,
		ColName: cn,
		DBName:   d,
		Like:     l,
		Where:    w,
	}
}

//the SHOW DATABASES statement.
type ShowDatabases struct {
	showImpl
	Like *ComparisonExpr
	Where *Where
}

func NewShowDatabases(l *ComparisonExpr,w *Where) *ShowDatabases  {
	return &ShowDatabases{
		Like:  l,
		Where: w,
	}
}

//SHOW TABLES statement.
type ShowTables struct {
	showImpl
	Ext bool
	Full bool
	DBName string
	Like *ComparisonExpr
	Where *Where
}

func NewShowTables(e bool, f bool, n string, l *ComparisonExpr, w *Where)*ShowTables {
	return &ShowTables{
		Ext:      e,
		Full:     f,
		DBName:   n,
		Like:     l,
		Where:    w,
	}
}

//SHOW PROCESSLIST
type ShowProcessList struct {
	showImpl
	Full bool
}

func NewShowProcessList(f bool) *ShowProcessList {
	return &ShowProcessList{Full: f}
}

type ShowErrors struct {
	showImpl
}

func NewShowErrors() *ShowErrors {
	return &ShowErrors{}
}

type ShowWarnings struct {
	showImpl
}

func NewShowWarnings() *ShowWarnings {
	return &ShowWarnings{}
}

//SHOW VARIABLES statement
//System Variables
type ShowVariables struct {
	showImpl
	Global bool
	Like *ComparisonExpr
	Where *Where
}

func NewShowVariables(g bool,l *ComparisonExpr,w *Where)*ShowVariables  {
	return &ShowVariables{
		Global: g,
		Like: l,
		Where: w,
	}
}

//SHOW STATUS statement
type ShowStatus struct {
	showImpl
	Global bool
	Like *ComparisonExpr
	Where *Where
}

func NewShowStatus(g bool,l *ComparisonExpr,w *Where) *ShowStatus {
	return &ShowStatus{
		Global: g,
		Like: l,
		Where: w,
	}
}

//show index statement
type ShowIndex struct {
	showImpl
	TableName TableName
	Where *Where
}

func NewShowIndex(t TableName,w *Where) *ShowIndex  {
	return &ShowIndex{
		TableName: t,
		Where:     w,
	}
}