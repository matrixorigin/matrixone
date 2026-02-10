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

package output

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Output)

const (
	stepInValid = iota
	stepCollect
	stepSend
	stepEnd
)

// for the case like `select for update`, need to lock whole batches before send it to next operator
// currently, this is implemented by blocking a output operator below, instead of calling func callBlocking
type container struct {
	currentIdx    int
	blockStep     int
	block         bool
	cachedBatches []*batch.Batch
	rowCount      int64
}

type Output struct {
	ctr container

	Data interface{}
	Func func(*batch.Batch, *perfcounter.CounterSet) error

	// IsAdaptive enables the adaptive vector search fallback mechanism.
	// When set to true and the query completes with zero results (rowCount == 0),
	// the Output operator will return ErrVectorNeedRetryWithPreMode error,
	// signaling that the query should be retried with 'pre' (pre-filter) mode.
	//
	// This is part of the vector search adaptive mode optimization (Phase 5),
	// which automatically falls back from 'post' mode to 'pre' mode when
	// post-filtering returns empty results due to high filter selectivity.
	//
	// IMPORTANT: We only trigger retry when rowCount == 0 (not when rowCount < limit)
	// to avoid merging partial results from the original query with retry results,
	// which would cause duplicate rows in the final output.
	IsAdaptive bool

	vm.OperatorBase
}

func (output *Output) GetOperatorBase() *vm.OperatorBase {
	return &output.OperatorBase
}

func init() {
	reuse.CreatePool[Output](
		func() *Output {
			return &Output{}
		},
		func(a *Output) {
			*a = Output{}
		},
		reuse.DefaultOptions[Output]().
			WithEnableChecker(),
	)
}

func (output Output) TypeName() string {
	return opName
}

func NewArgument() *Output {
	return reuse.Alloc[Output](nil)
}

func (output *Output) WithData(data interface{}) *Output {
	output.Data = data
	return output
}

func (output *Output) WithFunc(Func func(*batch.Batch, *perfcounter.CounterSet) error) *Output {
	output.Func = Func
	return output
}

// WithBlocck set the output is blocked. If true output will block the current pipeline, and cache
// all input batches. And wait for all the input's batch to be locked before outputting the cached batch
// to the downstream operator.
// E.g. select for update, only we get all lock result, then select can be
// performed, otherwise, if we need retry in RC mode, we may get wrong result.
func (output *Output) WithBlock(block bool) *Output {
	output.ctr.block = block
	return output
}

// WithAdaptive sets the adaptive vector search fallback mode.
// When enabled, the Output operator will trigger a retry with pre-filter mode
// if the query completes with zero results.
// See IsAdaptive field documentation for details.
func (output *Output) WithAdaptive(isAdaptive bool) *Output {
	output.IsAdaptive = isAdaptive
	return output
}

func (output *Output) shouldRetryWithPreMode() bool {
	return output.IsAdaptive && output.ctr.rowCount == 0
}

func (output *Output) Release() {
	if output != nil {
		reuse.Free[Output](output, nil)
	}
}

func (output *Output) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if !pipelineFailed {
		_ = output.Func(nil, nil)
	}

	if output.ctr.block {
		output.ctr.currentIdx = 0
		output.ctr.blockStep = stepCollect
		output.cleanCachedBatch(proc)
	}
}

func (output *Output) Free(proc *process.Process, pipelineFailed bool, err error) {
	if output.ctr.block {
		output.ctr.currentIdx = -1
		output.ctr.blockStep = stepInValid
		output.cleanCachedBatch(proc)
		output.ctr.cachedBatches = nil
	}
}

func (output *Output) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (output *Output) cleanCachedBatch(proc *process.Process) {
	for _, bat := range output.ctr.cachedBatches {
		bat.Clean(proc.Mp())
	}
	output.ctr.cachedBatches = output.ctr.cachedBatches[:0]
}
