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

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
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
	indexBuild.OpAnalyzer = process.NewAnalyzer(indexBuild.GetIdx(), indexBuild.IsFirst, indexBuild.IsLast, "index build")
	if indexBuild.RuntimeFilterSpec == nil {
		panic("there must be runtime filter in index build!")
	}
	return nil
}

func (indexBuild *IndexBuild) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(indexBuild.GetIdx(), indexBuild.GetParallelIdx(), indexBuild.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := indexBuild.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result := vm.NewCallResult()
	ctr := &indexBuild.ctr
	for {
		switch ctr.state {
		case ReceiveBatch:
			if err := ctr.build(indexBuild, proc, analyzer); err != nil {
				return result, err
			}
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(indexBuild, proc); err != nil {
				return result, err
			}
			ctr.state = End
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			analyzer.Output(result.Batch)
			return result, nil
		}
	}
}

func (ctr *container) collectBuildBatches(indexBuild *IndexBuild, proc *process.Process, analyzer process.Analyzer) error {
	for {
		result, err := vm.ChildrenCall(indexBuild.GetChildren(0), proc, analyzer)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}

		//anal.Input(result.Batch, isFirst)
		//anal.Alloc(int64(result.Batch.Size()))
		analyzer.Alloc(int64(result.Batch.Size()))
		ctr.buf, err = ctr.buf.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
		if err != nil {
			return err
		}

		// If read index table data exceeds the UpperLimit, abandon reading data from index table
		if ctr.buf.RowCount() > int(indexBuild.RuntimeFilterSpec.UpperLimit) {
			// for index build, can exit early
			return nil
		}
	}
	return nil
}

func (ctr *container) build(ap *IndexBuild, proc *process.Process, anal process.Analyzer) error {
	err := ctr.collectBuildBatches(ap, proc, anal)
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
	} else if ctr.buf == nil || ctr.buf.RowCount() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	}

	inFilterCardLimit := ap.RuntimeFilterSpec.UpperLimit

	if ctr.buf.RowCount() > int(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else {
		if len(ctr.buf.Vecs) != 1 {
			panic("there must be only 1 vector in index build batch")
		}
		vec := ctr.buf.Vecs[0]
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
