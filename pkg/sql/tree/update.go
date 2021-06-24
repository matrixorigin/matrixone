package tree

//update statement
type Update struct {
	statementImpl
	Table     TableExpr
	Exprs     UpdateExprs
	From      TableExprs
	Where     *Where
	OrderBy   OrderBy
	Limit     *Limit
}

func NewUpdate(t TableExpr,e UpdateExprs,f TableExprs,w *Where,o OrderBy,l *Limit)*Update{
	return &Update{
		Table:   t,
		Exprs:   e,
		From:    f,
		Where:   w,
		OrderBy: o,
		Limit:   l,
	}
}

type UpdateExprs []*UpdateExpr

//the update expression.
type UpdateExpr struct {
	NodePrinter
	Tuple bool
	Names []*UnresolvedName
	Expr  Expr
}

func NewUpdateExpr(t bool,n []*UnresolvedName,e Expr)*UpdateExpr{
	return &UpdateExpr{
		Tuple: t,
		Names: n,
		Expr:  e,
	}
}

//Load data statement
type Load struct {
	statementImpl
	Local bool
	File string
	DuplicateHandling DuplicateKey
	Table *TableName
	//Partition
	//Fields
	Fields *Fields
	//Lines
	Lines *Lines
	//Ignored lines
	IgnoredLines uint64
	//col_name_or_user_var
	ColumnList []LoadColumn
	//set col_name
	Assignments UpdateExprs
}

func NewLoad(l bool,f string,d DuplicateKey,t *TableName,
		fie *Fields,li *Lines,il uint64,cl []LoadColumn,
		a UpdateExprs) *Load {
	return &Load{
		Local:             l,
		File:              f,
		DuplicateHandling: d,
		Table:             t,
		Fields:            fie,
		Lines:             li,
		IgnoredLines:      il,
		ColumnList:        cl,
		Assignments:       a,
	}
}

type DuplicateKey interface {}

type duplicateKeyImpl struct {
	DuplicateKey
}

type DuplicateKeyError struct {
	duplicateKeyImpl
}

func NewDuplicateKeyError() *DuplicateKeyError {
	return &DuplicateKeyError{}
}

type DuplicateKeyReplace struct {
	duplicateKeyImpl
}

func NewDuplicateKeyReplace() *DuplicateKeyReplace{
	return &DuplicateKeyReplace{}
}

type DuplicateKeyIgnore struct {
	duplicateKeyImpl
}

func NewDuplicateKeyIgnore() *DuplicateKeyIgnore {
	return &DuplicateKeyIgnore{}
}

type Fields struct {
	Terminated string
	Optionally bool
	EnclosedBy byte
	EscapedBy  byte
}

func NewFields(t string,o bool,en byte,es byte)*Fields{
	return &Fields{
		Terminated: t,
		Optionally: o,
		EnclosedBy: en,
		EscapedBy:  es,
	}
}

type Lines struct {
	StartingBy string
	TerminatedBy string
}

func NewLines(s string, t string)*Lines{
	return &Lines{
		StartingBy:   s,
		TerminatedBy: t,
	}
}

//column element in load data column list
type LoadColumn interface {}

var _ LoadColumn = &UnresolvedName{}
var _ LoadColumn = &VarExpr{}