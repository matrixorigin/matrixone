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

package indexbuild

import (
	"bytes"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "index_build"

func (indexBuild *IndexBuild) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": index build ")
}

func (indexBuild *IndexBuild) OpType() vm.OpType {
	return vm.IndexBuild
}

func (indexBuild *IndexBuild) Prepare(proc *process.Process) (err error) {
	if indexBuild.RuntimeFilterSpec == nil {
		panic("there must be runtime filter in index build!")
	}
	indexBuild.ctr = new(container)
	return nil
}

func (indexBuild *IndexBuild) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(indexBuild.GetIdx(), indexBuild.GetParallelIdx(), indexBuild.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	result := vm.NewCallResult()
	ctr := indexBuild.ctr
	for {
		switch ctr.state {
		case ReceiveBatch:
			if err := ctr.build(indexBuild, proc, anal, indexBuild.GetIsFirst()); err != nil {
				return result, err
			}
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(indexBuild, proc); err != nil {
				return result, err
			}
			ctr.state = End
		default:
			if ctr.batch != nil {
				proc.PutBatch(ctr.batch)
				ctr.batch = nil
			}
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) collectBuildBatches(indexBuild *IndexBuild, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var currentBatch *batch.Batch
	for {
		result, err := vm.ChildrenCall(indexBuild.GetChildren(0), proc, anal)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		currentBatch = result.Batch
		atomic.AddInt64(&currentBatch.Cnt, 1)
		if currentBatch.IsEmpty() {
			proc.PutBatch(currentBatch)
			continue
		}

		anal.Input(currentBatch, isFirst)
		anal.Alloc(int64(currentBatch.Size()))
		ctr.batch, err = ctr.batch.AppendWithCopy(proc.Ctx, proc.Mp(), currentBatch)
		if err != nil {
			return err
		}
		proc.PutBatch(currentBatch)

		// If read index table data exceeds the UpperLimit, abandon reading data from index table
		if ctr.batch.RowCount() > int(indexBuild.RuntimeFilterSpec.UpperLimit) {
			// for index build, can exit early
			return nil
		}
	}
	return nil
}

func (ctr *container) build(ap *IndexBuild, proc *process.Process, anal process.Analyze, isFirst bool) error {
	err := ctr.collectBuildBatches(ap, proc, anal, isFirst)
	if err != nil {
		return err
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *IndexBuild, proc *process.Process) error {
	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = ap.RuntimeFilterSpec.Tag

	if ap.RuntimeFilterSpec.Expr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else if ctr.batch == nil || ctr.batch.RowCount() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	}

	inFilterCardLimit := ap.RuntimeFilterSpec.UpperLimit

	if ctr.batch.RowCount() > int(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else {
		if len(ctr.batch.Vecs) != 1 {
			panic("there must be only 1 vector in index build batch")
		}
		vec := ctr.batch.Vecs[0]
		vec.InplaceSort()
		data, err := vec.MarshalBinary()
		if err != nil {
			return err
		}

		runtimeFilter.Typ = message.RuntimeFilter_IN
		runtimeFilter.Card = int32(vec.Length())
		runtimeFilter.Data = data
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
	}
	return nil
}
