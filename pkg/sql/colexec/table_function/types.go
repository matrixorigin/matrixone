// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	dataProducing = iota
	dataFinished
)

type Argument struct {
	ctr *container

	Rets      []*plan.ColDef
	Args      []*plan.Expr
	Attrs     []string
	Params    []byte
	FuncName  string
	retSchema []types.Type

	buf            *batch.Batch
	generateSeries *generateSeriesArg

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

type container struct {
	state int

	executorsForArgs []colexec.ExpressionExecutor
}

type generateSeriesState int

var (
	initArg   generateSeriesState = 0
	genBatch  generateSeriesState = 1
	genFinish generateSeriesState = 2
)

type generateSeriesArg struct {
	state        generateSeriesState
	startVecType *types.Type
	start        any
	end          any
	last         any
	step         any
	scale        int32 // used by handleDateTime
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.ctr != nil {
		arg.ctr.cleanExecutors()
	}
	if arg.buf != nil {
		arg.buf.Clean(proc.Mp())
		arg.buf = nil
	}
}

func (ctr *container) cleanExecutors() {
	for i := range ctr.executorsForArgs {
		if ctr.executorsForArgs[i] != nil {
			ctr.executorsForArgs[i].Free()
		}
	}
	ctr.executorsForArgs = nil
}

type unnestParam struct {
	FilterMap map[string]struct{} `json:"filterMap"`
	ColName   string              `json:"colName"`
}

var (
	unnestDeniedFilters = []string{"col", "seq"}
	defaultFilterMap    = map[string]struct{}{
		"key":   {},
		"path":  {},
		"index": {},
		"value": {},
		"this":  {},
	}
)

const (
	unnestMode      = "both"
	unnestRecursive = false
)

type generateSeriesNumber interface {
	int32 | int64 | types.Datetime
}
