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
	"context"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/simdcsv"
)

func ColumnCntLargerErrorInfo() string {
	return "the table column is larger than input data column"
}

// Use for External table scan param
type ExternalParam struct {
	Attrs         []string
	Cols          []*plan.ColDef
	Name2ColIndex map[string]int32
	CreateSql     string
	Ctx           context.Context
	plh           *ParseLineHandler
	extern        *tree.ExternParam
	IgnoreLine    int
	IgnoreLineTag int
	// tag indicate the fileScan is finished
	Fileparam    *ExternalFileparam
	Zoneparam    *ZonemapFileparam
	Filter       *FilterParam
	FileList     []string
	reader       io.ReadCloser
	maxBatchSize uint64
	tableDef     *plan.TableDef
	ClusterTable *plan.ClusterTable
	prevStr      string
}

type ExternalFileparam struct {
	End       bool
	FileCnt   int
	FileFin   int
	FileIndex int
}

type ZonemapFileparam struct {
	bs     []objectio.BlockObject
	offset int
}

type FilterParam struct {
	maxCol       int
	exprMono     bool
	columns      []uint16
	columnMap    map[int]int
	File2Size    map[string]int64
	FilterExpr   *plan.Expr
	objectReader objectio.Reader
}

type Argument struct {
	Es *ExternalParam
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
}

type ParseLineHandler struct {
	simdCsvReader *simdcsv.Reader
	//csv read put lines into the channel
	simdCsvGetParsedLinesChan atomic.Value // chan simdcsv.LineOut
	//batch
	batchSize int
	//simd csv
	simdCsvLineArray [][]string
}
