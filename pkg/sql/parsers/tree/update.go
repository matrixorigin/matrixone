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
	"context"
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

func (node *Update) GetStatementType() string { return "Update" }
func (node *Update) GetQueryType() string     { return QueryTypeDML }

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
	if node == nil {
		return
	}
	prefix := ""
	for _, n := range node.Names {
		ctx.WriteString(prefix)
		n.Format(ctx)
		prefix = " "
	}
	ctx.WriteString(" = ")
	if node.Expr != nil {
		node.Expr.Format(ctx)
	}
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
	GZ         = "gz" // alias of gzip
	BZIP2      = "bzip2"
	BZ2        = "bz2" // alias for bzip2
	FLATE      = "flate"
	LZW        = "lzw"
	ZLIB       = "zlib"
	LZ4        = "lz4"
	TAR_GZ     = "tar.gz"
	TAR_BZ2    = "tar.bz2"
)

// load data fotmat
const (
	CSV      = "csv"
	JSONLINE = "jsonline"
	PARQUET  = "parquet"
)

// if $format is jsonline
const (
	OBJECT = "object"
	ARRAY  = "array"
)

const (
	S3     = 1
	INLINE = 2
)

type ExternParam struct {
	// params which come from parser
	ExParamConst
	// params which come from internal construct
	ExParam
}

type ExParamConst struct {
	Init         bool
	ScanType     int
	FileSize     int64
	Filepath     string
	CompressType string
	Format       string
	Option       []string
	Data         string
	Tail         *TailParameter
	StageName    Identifier
}

type ExParam struct {
	JsonData    string
	FileService fileservice.FileService
	NullMap     map[string]([]string)
	S3Param     *S3Parameter
	Ctx         context.Context
	LoadFile    bool
	Local       bool
	QueryResult bool
	SysTable    bool
	Parallel    bool
	Strict      bool
}

type S3Parameter struct {
	Endpoint   string `json:"s3-test-endpoint"`
	Region     string `json:"s3-test-region"`
	APIKey     string `json:"s3-test-key"`
	APISecret  string `json:"s3-test-secret"`
	Bucket     string `json:"s3-test-bucket"`
	Provider   string `json:"s3-test-rovider"`
	RoleArn    string `json:"s3-test-rolearn"`
	ExternalId string `json:"s3-test-externalid"`
}

type TailParameter struct {
	//Charset
	Charset string
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
	Accounts          IdentifierList
	//Partition
	Param *ExternParam
}

func (node *Load) Format(ctx *FmtCtx) {
	ctx.WriteString("load data")
	if node.Local {
		ctx.WriteString(" local")
	}
	if len(node.Param.StageName) != 0 {
		ctx.WriteString(" url from stage ")
		node.Param.StageName.Format(ctx)
	} else {
		if node.Param.ScanType == INLINE {
			ctx.WriteString(" inline format='")
			ctx.WriteString(node.Param.Format)
			ctx.WriteString("', data='")
			ctx.WriteString(node.Param.Data)
			if node.Param.JsonData == "" {
				ctx.WriteString("'")
			} else {
				ctx.WriteString("', jsontype='")
				ctx.WriteString(node.Param.JsonData)
				ctx.WriteString("'")
			}
		} else {
			if len(node.Param.Option) == 0 {
				ctx.WriteString(" infile ")
				ctx.WriteString(node.Param.Filepath)
			} else {
				if node.Param.ScanType == S3 {
					ctx.WriteString(" url s3option ")
				} else {
					ctx.WriteString(" infile ")
				}
				formatS3option(ctx, node.Param.Option)
			}
		}
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

	if node.Accounts != nil {
		ctx.WriteString(" accounts(")
		node.Accounts.Format(ctx)
		ctx.WriteByte(')')
	}

	if len(node.Param.Tail.Charset) != 0 {
		ctx.WriteByte(' ')
		ctx.WriteString("character set ")
		ctx.WriteString(node.Param.Tail.Charset)
	}

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
	if node.Param.Parallel {
		ctx.WriteString(" parallel true ")
		if node.Param.Strict {
			ctx.WriteString("strict true ")
		}
	}
}

func formatS3option(ctx *FmtCtx, option []string) {
	ctx.WriteString("{")
	for i := 0; i < len(option); i += 2 {
		switch strings.ToLower(option[i]) {
		case "endpoint":
			ctx.WriteString("'endpoint'='" + option[i+1] + "'")
		case "region":
			ctx.WriteString("'region'='" + option[i+1] + "'")
		case "access_key_id":
			ctx.WriteString("'access_key_id'='******'")
		case "secret_access_key":
			ctx.WriteString("'secret_access_key'='******'")
		case "bucket":
			ctx.WriteString("'bucket'='" + option[i+1] + "'")
		case "filepath":
			ctx.WriteString("'filepath'='" + option[i+1] + "'")
		case "compression":
			ctx.WriteString("'compression'='" + option[i+1] + "'")
		case "format":
			ctx.WriteString("'format'='" + option[i+1] + "'")
		case "jsondata":
			ctx.WriteString("'jsondata'='" + option[i+1] + "'")
		case "role_arn":
			ctx.WriteString("'role_arn'='" + option[i+1] + "'")
		case "external_id":
			ctx.WriteString("'external_id'='" + option[i+1] + "'")
		}
		if i != len(option)-2 {
			ctx.WriteString(", ")
		}
	}
	ctx.WriteString("}")
}

func (node *Load) GetStatementType() string { return "Load" }
func (node *Load) GetQueryType() string     { return QueryTypeDML }

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

type EscapedBy struct {
	Value byte
}

type EnclosedBy struct {
	Value byte
}

type Terminated struct {
	Value string
}

type Fields struct {
	Terminated *Terminated
	Optionally bool
	EnclosedBy *EnclosedBy
	EscapedBy  *EscapedBy
}

func (node *Fields) Format(ctx *FmtCtx) {
	ctx.WriteString("fields")
	prefix := ""
	if node.Terminated != nil {
		ctx.WriteString(" terminated by ")
		if node.Terminated.Value == "" {
			ctx.WriteString("''")
		} else {
			ctx.WriteStringQuote(node.Terminated.Value)
		}
		prefix = " "
	}
	if node.Optionally {
		ctx.WriteString(prefix)
		ctx.WriteString("optionally enclosed by ")
		if node.EnclosedBy.Value == 0 {
			ctx.WriteString("''")
		} else {
			ctx.WriteStringQuote(string(node.EnclosedBy.Value))
		}
	} else if node.EnclosedBy != nil && node.EnclosedBy.Value != 0 {
		ctx.WriteString(prefix)
		ctx.WriteString("enclosed by ")
		ctx.WriteStringQuote(string(node.EnclosedBy.Value))
	}
	if node.EscapedBy != nil {
		ctx.WriteString(prefix)
		ctx.WriteString("escaped by ")
		if node.EscapedBy.Value == 0 {
			ctx.WriteString("''")
		} else {
			ctx.WriteStringQuote(string(node.EscapedBy.Value))
		}
	}
}

func NewFields(t string, o bool, en byte, es byte) *Fields {
	return &Fields{
		Terminated: &Terminated{
			Value: t,
		},
		Optionally: o,
		EnclosedBy: &EnclosedBy{
			Value: en,
		},
		EscapedBy: &EscapedBy{
			Value: es,
		},
	}
}

type Lines struct {
	StartingBy   string
	TerminatedBy *Terminated
}

func (node *Lines) Format(ctx *FmtCtx) {
	ctx.WriteString("lines")
	if node.StartingBy != "" {
		ctx.WriteString(" starting by ")
		ctx.WriteStringQuote(node.StartingBy)
	}
	if node.TerminatedBy != nil {
		ctx.WriteString(" terminated by ")
		if node.TerminatedBy.Value == "" {
			ctx.WriteString("''")
		} else {
			ctx.WriteStringQuote(node.TerminatedBy.Value)
		}
	}
}

func NewLines(s string, t string) *Lines {
	return &Lines{
		StartingBy: s,
		TerminatedBy: &Terminated{
			Value: t,
		},
	}
}

// column element in load data column list
type LoadColumn interface {
	NodeFormatter
}

type ExportParam struct {
	// outfile flag
	Outfile bool
	// query id
	QueryId string
	// filename path
	FilePath string
	// Fields
	Fields *Fields
	// Lines
	Lines *Lines
	// fileSize
	MaxFileSize uint64
	// header flag
	Header     bool
	ForceQuote []string
	// stage filename path
	StageFilePath string
}

func (ep *ExportParam) Format(ctx *FmtCtx) {
	if ep.FilePath == "" {
		return
	}
	ep.format(ctx, true)
}

func (ep *ExportParam) format(ctx *FmtCtx, withOutfile bool) {
	ctx.WriteString("into")
	if withOutfile {
		ctx.WriteString(" outfile")
	}
	ctx.WriteByte(' ')
	ctx.WriteString(ep.FilePath)
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
