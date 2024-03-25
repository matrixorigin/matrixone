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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

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
	IgnoreLine      int
	IgnoreLineTag   int
	ParallelLoad    bool
	maxBatchSize    uint64
	Idx             int
	CreateSql       string
	Close           byte
	Attrs           []string
	Cols            []*plan.ColDef
	FileList        []string
	FileSize        []int64
	FileOffset      []int64
	FileOffsetTotal []*pipeline.FileOffset
	Name2ColIndex   map[string]int32
	Ctx             context.Context
	Extern          *tree.ExternParam
	tableDef        *plan.TableDef
	ClusterTable    *plan.ClusterTable
}

type ExParam struct {
	prevStr        string
	reader         io.ReadCloser
	plh            *ParseLineHandler
	Fileparam      *ExFileparam
	Zoneparam      *ZonemapFileparam
	Filter         *FilterParam
	MoCsvLineArray [][]string
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
	blockReader  *blockio.BlockReader
}

type Argument struct {
	Es *ExternalParam

	info     *vm.OperatorInfo
	children []vm.Operator
	buf      *batch.Batch

	maxAllocSize int
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.children = append(arg.children, child)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.buf != nil {
		arg.buf.Clean(proc.Mp())
		arg.buf = nil
	}
	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	anal.Alloc(int64(arg.maxAllocSize))
}

type ParseLineHandler struct {
	csvReader *csv.Reader
	//batch
	batchSize int
	//mo csv
	moCsvLineArray [][]string
}

// NewReader returns a new Reader with options that reads from r.
func newReaderWithOptions(r io.Reader, cma, cmnt rune, lazyQt, tls bool) *csv.Reader {
	rCsv := csv.NewReader(bufio.NewReader(r))
	rCsv.Comma = cma
	rCsv.Comment = cmnt
	rCsv.LazyQuotes = lazyQt
	rCsv.TrimLeadingSpace = tls
	rCsv.FieldsPerRecord = -1
	return rCsv
}
