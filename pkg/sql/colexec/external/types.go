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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
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
	IgnoreLine    int
	IgnoreLineTag int
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
	tableDef        *plan.TableDef
	ClusterTable    *plan.ClusterTable
}

type ExParam struct {
	prevStr   string
	reader    io.ReadCloser
	plh       *ParseLineHandler
	Fileparam *ExFileparam
	Zoneparam *ZonemapFileparam
	Filter    *FilterParam
	parqh     *ParquetHandler
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
	blockReader  *ioutil.BlockReader
}

type container struct {
	maxAllocSize int
	buf          *batch.Batch
}
type External struct {
	ctr container
	Es  *ExternalParam

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

func newReaderWithParam(param *ExternalParam) (*csvparser.CSVParser, error) {
	fieldsTerminatedBy := tree.DefaultFieldsTerminated
	fieldsEnclosedBy := tree.DefaultFieldsEnclosedBy
	fieldsEscapedBy := tree.DefaultFieldsEscapedBy

	linesTerminatedBy := "\n"
	linesStartingBy := ""

	if param.Extern.Tail.Fields != nil {
		if terminated := param.Extern.Tail.Fields.Terminated; terminated != nil && terminated.Value != "" {
			fieldsTerminatedBy = terminated.Value
		}
		if enclosed := param.Extern.Tail.Fields.EnclosedBy; enclosed != nil && enclosed.Value != 0 {
			fieldsEnclosedBy = string(enclosed.Value)
		}
		if escaped := param.Extern.Tail.Fields.EscapedBy; escaped != nil {
			if escaped.Value == 0 {
				fieldsEscapedBy = ""
			} else {
				fieldsEscapedBy = string(escaped.Value)
			}
		}
	}

	if param.Extern.Tail.Lines != nil {
		if terminated := param.Extern.Tail.Lines.TerminatedBy; terminated != nil && terminated.Value != "" {
			linesTerminatedBy = param.Extern.Tail.Lines.TerminatedBy.Value
		}
		if param.Extern.Tail.Lines.StartingBy != "" {
			linesStartingBy = param.Extern.Tail.Lines.StartingBy
		}
	}

	if param.Extern.Format == tree.JSONLINE {
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

	return csvparser.NewCSVParser(&config, bufio.NewReader(param.reader), csvparser.ReadBlockSize, false)
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
