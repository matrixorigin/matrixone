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
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// update statement
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

// the update expression.
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

const (
	AUTO       = "auto"
	NOCOMPRESS = "none"
	GZIP       = "gzip"
	BZIP2      = "bz2"
	FLATE      = "flate"
	LZW        = "lzw"
	ZLIB       = "zlib"
	LZ4        = "lz4"
)

// load data fotmat
const (
	CSV      = "csv"
	JSONLINE = "jsonline"
)

// if $format is jsonline
const (
	OBJECT = "object"
	ARRAY  = "array"
)

const (
	S3 = 1
)

type ExternParam struct {
	ScanType     int
	Filepath     string
	CompressType string
	Format       string
	JsonData     string
	Tail         *TailParameter
	FileService  fileservice.FileService
	NullMap      map[string]([]string)
	S3option     []string
	S3Param      *S3Parameter
	Ctx          context.Context
	LoadFile     bool
}

type S3Parameter struct {
	Endpoint  string `json:"s3-test-endpoint"`
	Region    string `json:"s3-test-region"`
	APIKey    string `json:"s3-test-key"`
	APISecret string `json:"s3-test-secret"`
	Bucket    string `json:"s3-test-bucket"`
}

type TailParameter struct {
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

// Load data statement
type Load struct {
	statementImpl
	Local             bool
	DuplicateHandling DuplicateKey
	Table             *TableName
	//Partition
	Param *ExternParam
}

type Import struct {
	statementImpl
	Local             bool
	DuplicateHandling DuplicateKey
	Table             *TableName
	//Partition
	Param *ExternParam
}

func (node *Load) Format(ctx *FmtCtx) {
	ctx.WriteString("load data")
	if node.Local {
		ctx.WriteString(" local")
	}

	if (node.Param.CompressType == AUTO || node.Param.CompressType == NOCOMPRESS) && node.Param.Format == CSV {
		ctx.WriteString(" infile ")
		ctx.WriteString(node.Param.Filepath)
	} else if node.Param.ScanType == S3 {
		ctx.WriteString(" url s3option ")
		ctx.WriteString("{'endpoint'='" + node.Param.S3option[0] + "', 'access_key_id'='" + node.Param.S3option[3] +
			"', 'secret_access_key'='" + node.Param.S3option[5] + "', 'bucket'='" + node.Param.S3option[7] + "', 'filepath'='" +
			node.Param.S3option[9] + "', 'region'='" + node.Param.S3option[11] + "'}")
	} else {
		ctx.WriteString(" infile ")
		ctx.WriteString("{'filepath':'" + node.Param.Filepath + "', 'compression':'" + strings.ToLower(node.Param.CompressType) + "', 'format':'" + strings.ToLower(node.Param.Format) + "'")
		if node.Param.Format == JSONLINE {
			ctx.WriteString(", 'jsondata':'" + node.Param.JsonData + "'")
		}
		ctx.WriteString("}")
	}

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

	if node.Param.Tail.Fields != nil {
		ctx.WriteByte(' ')
		node.Param.Tail.Fields.Format(ctx)
	}

	if node.Param.Tail.Lines != nil {
		ctx.WriteByte(' ')
		node.Param.Tail.Lines.Format(ctx)
	}

	if node.Param.Tail.IgnoredLines != 0 {
		ctx.WriteString(" ignore ")
		ctx.WriteString(strconv.FormatUint(node.Param.Tail.IgnoredLines, 10))
		ctx.WriteString(" lines")
	}
	if node.Param.Tail.ColumnList != nil {
		prefix := " ("
		for _, c := range node.Param.Tail.ColumnList {
			ctx.WriteString(prefix)
			c.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.Param.Tail.Assignments != nil {
		ctx.WriteString(" set ")
		node.Param.Tail.Assignments.Format(ctx)
	}
}

func (node *Import) Format(ctx *FmtCtx) {
	ctx.WriteString("import data")
	if node.Local {
		ctx.WriteString(" local")
	}

	if node.Param.CompressType == AUTO || node.Param.CompressType == NOCOMPRESS {
		ctx.WriteString(" infile ")
		ctx.WriteString(node.Param.Filepath)
	} else {
		ctx.WriteString(" infile ")
		ctx.WriteString("{'filepath':'" + node.Param.Filepath + "', 'compression':'" + strings.ToLower(node.Param.CompressType) + "'}")
	}

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

	if node.Param.Tail.Fields != nil {
		ctx.WriteByte(' ')
		node.Param.Tail.Fields.Format(ctx)
	}

	if node.Param.Tail.Lines != nil {
		ctx.WriteByte(' ')
		node.Param.Tail.Lines.Format(ctx)
	}

	if node.Param.Tail.IgnoredLines != 0 {
		ctx.WriteString(" ignore ")
		ctx.WriteString(strconv.FormatUint(node.Param.Tail.IgnoredLines, 10))
		ctx.WriteString(" lines")
	}
	if node.Param.Tail.ColumnList != nil {
		prefix := " ("
		for _, c := range node.Param.Tail.ColumnList {
			ctx.WriteString(prefix)
			c.Format(ctx)
			prefix = ", "
		}
		ctx.WriteByte(')')
	}
	if node.Param.Tail.Assignments != nil {
		ctx.WriteString(" set ")
		node.Param.Tail.Assignments.Format(ctx)
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

// column element in load data column list
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
