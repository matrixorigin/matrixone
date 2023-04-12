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

package external

import (
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ColumnCntLargerErrorInfo() string {
	return "the table column is larger than input data column"
}

// Use for External table scan param
type ExternalParam struct {
	// Externally passed parameters that will not change
	ExParamConst
	// Inner parameters
	ExParam
}

type ExParamConst struct {
	IgnoreLine    int
	IgnoreLineTag int
	maxBatchSize  uint64
	CreateSql     string
	Attrs         []string
	Cols          []*plan.ColDef
	FileList      []string
	FileSize      []int64
	FileOffset    [][2]int
	Name2ColIndex map[string]int32
	Ctx           context.Context
	Extern        *tree.ExternParam
	tableDef      *plan.TableDef
	ClusterTable  *plan.ClusterTable
}

type ExParam struct {
	prevStr   string
	reader    io.ReadCloser
	plh       *ParseLineHandler
	Fileparam *ExFileparam
	Zoneparam *ZonemapFileparam
	Filter    *FilterParam
}

type ExFileparam struct {
	End       bool
	FileCnt   int
	FileFin   int
	FileIndex int
	Filepath  string
}

type ZonemapFileparam struct {
	bs     []objectio.BlockObject
	offset int
}

type FilterParam struct {
	maxCol      int
	exprMono    bool
	columns     []uint16 // save real index in table to read column's data from files
	defColumns  []uint16 // save col index in tableDef.Cols, cooperate with columnMap
	columnMap   map[int]int
	File2Size   map[string]int64
	FilterExpr  *plan.Expr
	blockReader *blockio.BlockReader
}

type Argument struct {
	Es *ExternalParam
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
}

type ParseLineHandler struct {
	moCsvReader *MOCsvReader
	//csv read put lines into the channel
	moCsvGetParsedLinesChan atomic.Value // chan simdcsv.LineOut
	//batch
	batchSize int
	//mo csv
	moCsvLineArray [][]string
}

type LineOut struct {
	Lines [][]string
	Line  []string
}

type MOCsvReader struct {
	// Comma is the field delimiter.
	// It is set to comma (',') by NewReader.
	// Comma must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	Comma rune

	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	// Comment must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	// It must also not be equal to Comma.
	Comment rune

	// FieldsPerRecord is the number of expected fields per record.
	// If FieldsPerRecord is positive, Read requires each record to
	// have the given number of fields. If FieldsPerRecord is 0, Read sets it to
	// the number of fields in the first record, so that future records must
	// have the same field count. If FieldsPerRecord is negative, no check is
	// made and records may have a variable number of fields.
	FieldsPerRecord int

	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	LazyQuotes bool

	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool

	ReuseRecord   bool // Deprecated: Unused by simdcsv.
	TrailingComma bool // Deprecated: No longer used.

	r *bufio.Reader

	//for ReadOneLine
	first bool
	rCsv  *csv.Reader
}

// NewReader returns a new Reader with options that reads from r.
func NewReaderWithOptions(r io.Reader, cma, cmnt rune, lazyQt, tls bool) *MOCsvReader {
	return &MOCsvReader{
		Comma:            cma,
		Comment:          cmnt,
		FieldsPerRecord:  -1,
		LazyQuotes:       lazyQt,
		TrimLeadingSpace: tls,
		ReuseRecord:      false,
		TrailingComma:    false,
		r:                bufio.NewReader(r),
	}
}
