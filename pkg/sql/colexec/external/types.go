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
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(External)

const (
	ColumnCntLargerErrorInfo = "the table column is larger than input data column"
)

// Use for External table scan param
type ExternalParam struct {
	// Externally passed parameters that will not change
	ExParamConst
	// Inner parameters
	ExParam
}

type ExParamConst struct {
	ParallelLoad  bool
	StrictSqlMode bool
	Close         byte
	maxBatchSize  uint64
	Idx           int
	ColumnListLen int32 // load ...  (col1, col2 , col3), ColumnListLen is 3
	CreateSql     string

	// letter case: origin
	Attrs           []plan.ExternAttr
	Cols            []*plan.ColDef
	FileList        []string
	FileSize        []int64
	FileOffset      []int64
	FileOffsetTotal []*pipeline.FileOffset
	Ctx             context.Context
	Extern          *tree.ExternParam
	ClusterTable    *plan.ClusterTable
}

type ExParam struct {
	Fileparam *ExFileparam
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
	zonemappable bool
	columnMap    map[int]int
	FilterExpr   *plan.Expr
	AuxIdCnt     int32 // saved from AssignAuxIdForExpr in Prepare, reused at runtime
}

// ExternalFileReader is the unified interface for reading external files.
// Each format (CSV/Parquet/ZoneMap) implements this interface independently.
//
// Lifecycle: NewXxxReader() → Open() → ReadBatch()* → Close() → Open() → ...
// Reader saves param reference in Open, subsequent methods access it via r.param.
type ExternalFileReader interface {
	// Open opens a file and initializes internal state.
	// fileEmpty=true means the file is empty (e.g. 0-row Parquet),
	// Call should Close then finishCurrentFile and continue to next file.
	Open(param *ExternalParam, proc *process.Process) (fileEmpty bool, err error)

	// ReadBatch reads one batch of data into buf.
	// Returns true when the current file is fully read.
	ReadBatch(ctx context.Context, buf *batch.Batch, proc *process.Process, analyzer process.Analyzer) (fileFinished bool, err error)

	// Close closes the current file and releases resources.
	Close() error
}

type container struct {
	maxAllocSize int
	buf          *batch.Batch
}

type External struct {
	ctr        container
	Es         *ExternalParam
	reader     ExternalFileReader // unified file reader
	fileOpened bool               // whether a file is currently active

	vm.OperatorBase
	colexec.Projection
}

func (external *External) GetOperatorBase() *vm.OperatorBase {
	return &external.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *External {
			return &External{}
		},
		func(a *External) {
			*a = External{}
		},
		reuse.DefaultOptions[External]().
			WithEnableChecker(),
	)
}

func (external External) TypeName() string {
	return opName
}

func NewArgument() *External {
	return reuse.Alloc[External](nil)
}

func (external *External) WithEs(es *ExternalParam) *External {
	external.Es = es
	return external
}

func (external *External) Release() {
	if external != nil {
		reuse.Free(external, nil)
	}
}

func (external *External) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if external.reader != nil {
		if closeErr := external.reader.Close(); closeErr != nil {
			logutil.Debugf("external reader close on reset: %v", closeErr)
		}
		external.reader = nil
		external.fileOpened = false
	}
	if external.ctr.buf != nil {
		external.ctr.buf.CleanOnlyData()
	}

	allocSize := int64(external.ctr.maxAllocSize)
	if external.ProjectList != nil {
		allocSize += external.ProjectAllocSize
		external.ResetProjection(proc)
	}

	if external.OpAnalyzer != nil {
		external.OpAnalyzer.Alloc(allocSize)
	}
	external.ctr.maxAllocSize = 0
}

func (external *External) Free(proc *process.Process, pipelineFailed bool, err error) {
	if external.reader != nil {
		external.reader.Close()
		external.reader = nil
		external.fileOpened = false
	}
	if external.ctr.buf != nil {
		external.ctr.buf.Clean(proc.Mp())
		external.ctr.buf = nil
	}
	external.FreeProjection(proc)
}

func (external *External) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	batch := input
	var err error
	if external.ProjectList != nil {
		batch, err = external.EvalProjection(input, proc)
	}
	return batch, err
}

type ParseLineHandler struct {
	csvReader *csvparser.CSVParser
}

// newCSVParserFromReader is a stateless pure function that creates a CSV parser.
// It only depends on config and io.Reader, not on ExParam.
// Used by both CsvReader.Open and getTailSizeStrict.
func newCSVParserFromReader(extern *tree.ExternParam, r io.Reader) (*csvparser.CSVParser, error) {
	fieldsTerminatedBy := tree.DefaultFieldsTerminated
	fieldsEnclosedBy := tree.DefaultFieldsEnclosedBy
	fieldsEscapedBy := tree.DefaultFieldsEscapedBy

	linesTerminatedBy := "\n"
	linesStartingBy := ""

	if extern.Tail.Fields != nil {
		if terminated := extern.Tail.Fields.Terminated; terminated != nil && terminated.Value != "" {
			fieldsTerminatedBy = terminated.Value
		}
		if enclosed := extern.Tail.Fields.EnclosedBy; enclosed != nil && enclosed.Value != 0 {
			fieldsEnclosedBy = string(enclosed.Value)
		}
		if escaped := extern.Tail.Fields.EscapedBy; escaped != nil {
			if escaped.Value == 0 {
				fieldsEscapedBy = ""
			} else {
				fieldsEscapedBy = string(escaped.Value)
			}
		}
	}

	if extern.Tail.Lines != nil {
		if terminated := extern.Tail.Lines.TerminatedBy; terminated != nil && terminated.Value != "" {
			linesTerminatedBy = extern.Tail.Lines.TerminatedBy.Value
		}
		if extern.Tail.Lines.StartingBy != "" {
			linesStartingBy = extern.Tail.Lines.StartingBy
		}
	}

	if extern.Format == tree.JSONLINE {
		fieldsTerminatedBy = "\t"
		fieldsEscapedBy = ""
	}

	config := csvparser.CSVConfig{
		FieldsTerminatedBy: fieldsTerminatedBy,
		FieldsEnclosedBy:   fieldsEnclosedBy,
		FieldsEscapedBy:    fieldsEscapedBy,
		LinesTerminatedBy:  linesTerminatedBy,
		LinesStartingBy:    linesStartingBy,
		NotNull:            false,
		Null:               []string{`\N`},
		UnescapedQuote:     true,
		Comment:            '#',
	}

	return csvparser.NewCSVParser(&config, bufio.NewReader(r), csvparser.ReadBlockSize, false)
}

type ParquetHandler struct {
	file        *parquet.File
	offset      int64
	batchCnt    int64
	cols        []*parquet.Column
	mappers     []*columnMapper
	pages       []parquet.Pages // cached pages iterators for each column
	currentPage []parquet.Page  // cached current page for each column
	pageOffset  []int64         // current offset within each cached page

	// for nested types support
	hasNestedCols bool
	rowReader     *parquet.Reader
}

type columnMapper struct {
	srcNull, dstNull   bool
	maxDefinitionLevel byte

	mapper func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error
}
