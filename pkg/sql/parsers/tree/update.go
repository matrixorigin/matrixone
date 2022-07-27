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
	"bufio"
	"os"
	"strconv"
)

//update statement
type Update struct {
	statementImpl
	Tables  TableExprs
	Exprs   UpdateExprs
	Where   *Where
	OrderBy OrderBy
	Limit   *Limit
	With    *With
}

func (node *Update) Format(ctx *FmtCtx) {
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("update")
	if node.Tables != nil {
		ctx.WriteByte(' ')
		node.Tables.Format(ctx)
	}
	ctx.WriteString(" set")
	if node.Exprs != nil {
		ctx.WriteByte(' ')
		node.Exprs.Format(ctx)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		node.OrderBy.Format(ctx)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		node.Limit.Format(ctx)
	}
}

type UpdateExprs []*UpdateExpr

func (node *UpdateExprs) Format(ctx *FmtCtx) {
	prefix := ""
	for _, u := range *node {
		ctx.WriteString(prefix)
		u.Format(ctx)
		prefix = ", "
	}
}

//the update expression.
type UpdateExpr struct {
	NodeFormatter
	Tuple bool
	Names []*UnresolvedName
	Expr  Expr
}

func (node *UpdateExpr) Format(ctx *FmtCtx) {
	prefix := ""
	for _, n := range node.Names {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = " "
	}
	ctx.WriteString(" = ")
	node.Expr.Format(ctx)
}

func NewUpdateExpr(t bool, n []*UnresolvedName, e Expr) *UpdateExpr {
	return &UpdateExpr{
		Tuple: t,
		Names: n,
		Expr:  e,
	}
}

//Load data statement
type Load struct {
	statementImpl
	Local             bool
	File              string
	DuplicateHandling DuplicateKey
	Table             *TableName
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

func (node *Load) Format(ctx *FmtCtx) {
	ctx.WriteString("load data")
	if node.Local {
		ctx.WriteString(" local")
	}
	ctx.WriteString(" infile ")
	ctx.WriteString(node.File)

	switch node.DuplicateHandling.(type) {
	case *DuplicateKeyError:
		break
	case *DuplicateKeyIgnore:
		ctx.WriteString(" ignore")
	case *DuplicateKeyReplace:
		ctx.WriteString(" replace")
	}
	ctx.WriteString(" into table ")
	node.Table.Format(ctx)

	if node.Fields != nil {
		ctx.WriteByte(' ')
		node.Fields.Format(ctx)
	}

	if node.Lines != nil {
		ctx.WriteByte(' ')
		node.Lines.Format(ctx)
	}

	if node.IgnoredLines != 0 {
		ctx.WriteString(" ignore ")
		ctx.WriteString(strconv.FormatUint(node.IgnoredLines, 10))
		ctx.WriteString(" lines")
	}
	if node.ColumnList != nil {
		prefix := " ("
		for _, c := range node.ColumnList {
			ctx.WriteString(prefix)
			c.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.Assignments != nil {
		ctx.WriteString(" set ")
		node.Assignments.Format(ctx)
	}
}

func NewLoad(l bool, f string, d DuplicateKey, t *TableName,
	fie *Fields, li *Lines, il uint64, cl []LoadColumn,
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

type DuplicateKey interface{}

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

func NewDuplicateKeyReplace() *DuplicateKeyReplace {
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

func (node *Fields) Format(ctx *FmtCtx) {
	ctx.WriteString("fields")
	prefix := ""
	if node.Terminated != "" {
		ctx.WriteString(" terminated by ")
		ctx.WriteString(node.Terminated)
		prefix = " "
	}
	if node.Optionally {
		ctx.WriteString(prefix)
		ctx.WriteString("optionally enclosed by ")
		ctx.WriteString(string(node.EnclosedBy))
	} else if node.EnclosedBy != 0 {
		ctx.WriteString(prefix)
		ctx.WriteString("enclosed by ")
		ctx.WriteString(string(node.EnclosedBy))
	}
	if node.EscapedBy != 0 {
		ctx.WriteString(prefix)
		ctx.WriteString("escaped by ")
		ctx.WriteString(string(node.EscapedBy))
	}
}

func NewFields(t string, o bool, en byte, es byte) *Fields {
	return &Fields{
		Terminated: t,
		Optionally: o,
		EnclosedBy: en,
		EscapedBy:  es,
	}
}

type Lines struct {
	StartingBy   string
	TerminatedBy string
}

func (node *Lines) Format(ctx *FmtCtx) {
	ctx.WriteString("lines")
	if node.StartingBy != "" {
		ctx.WriteString(" starting by ")
		ctx.WriteString(node.StartingBy)
	}
	if node.TerminatedBy != "" {
		ctx.WriteString(" terminated by ")
		ctx.WriteString(node.TerminatedBy)
	}
}

func NewLines(s string, t string) *Lines {
	return &Lines{
		StartingBy:   s,
		TerminatedBy: t,
	}
}

//column element in load data column list
type LoadColumn interface {
	NodeFormatter
}

type ExportParam struct {
	// file handler
	File *os.File
	// bufio.writer
	Writer *bufio.Writer
	// outfile flag
	Outfile bool
	// filename path
	FilePath string
	// Fields
	Fields *Fields
	// Lines
	Lines *Lines
	// fileSize
	MaxFileSize uint64
	// curFileSize
	CurFileSize uint64
	Rows        uint64
	FileCnt     uint
	// header flag
	Header     bool
	ForceQuote []string
	ColumnFlag []bool
	Symbol     [][]byte

	// default flush size
	DefaultBufSize int64
	OutputStr      []byte
	LineSize       uint64
}

func (ep *ExportParam) Format(ctx *FmtCtx) {
	if ep.FilePath == "" {
		return
	}
	ctx.WriteString("into outfile " + ep.FilePath)
	if ep.Fields != nil {
		ctx.WriteByte(' ')
		ep.Fields.Format(ctx)
	}
	if ep.Lines != nil {
		ctx.WriteByte(' ')
		ep.Lines.Format(ctx)
	}
	ctx.WriteString(" header ")
	if ep.Header {
		ctx.WriteString("true")
	} else {
		ctx.WriteString("false")
	}
	if ep.MaxFileSize != 0 {
		ctx.WriteString(" max_file_size ")
		ctx.WriteString(strconv.FormatUint(ep.MaxFileSize, 10))
	}
	if len(ep.ForceQuote) > 0 {
		ctx.WriteString(" force_quote")
		prefix := " "
		for i := 0; i < len(ep.ForceQuote); i++ {
			ctx.WriteString(prefix)
			ctx.WriteString(ep.ForceQuote[i])
			prefix = ", "
		}
	}
}

var _ LoadColumn = &UnresolvedName{}
var _ LoadColumn = &VarExpr{}
