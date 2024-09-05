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

package shufflebuild

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "shuffle_build"

func (shuffleBuild *ShuffleBuild) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": shuffle build ")
}

func (shuffleBuild *ShuffleBuild) OpType() vm.OpType {
	return vm.ShuffleBuild
}

func (shuffleBuild *ShuffleBuild) Prepare(proc *process.Process) (err error) {
	if shuffleBuild.RuntimeFilterSpec == nil {
		panic("there must be runtime filter in shuffle build!")
	}
	return shuffleBuild.ctr.hashmapBuilder.Prepare(shuffleBuild.Conditions, proc)
}

func (shuffleBuild *ShuffleBuild) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(shuffleBuild.GetIdx(), shuffleBuild.GetParallelIdx(), shuffleBuild.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	ap := shuffleBuild
	ctr := &ap.ctr
	for {
		switch ctr.state {
		case ReceiveBatch:
			err := ctr.collectBuildBatches(ap, proc, anal, shuffleBuild.GetIsFirst())
			if err != nil {
				return result, err
			}
			if err = ctr.handleRuntimeFilter(ap, proc); err != nil {
				return result, err
			}
			ctr.state = BuildHashMap
		case BuildHashMap:
			err := ctr.hashmapBuilder.BuildHashmap(ap.HashOnPK, ap.NeedAllocateSels, ap.RuntimeFilterSpec, proc)
			if err != nil {
				ctr.hashmapBuilder.Free(proc)
				return result, err
			}
			if !ap.NeedBatches {
				// if do not need merged batch, free it now to save memory
				ctr.hashmapBuilder.Batches.Clean(proc.Mp())
			}
			anal.Alloc(ctr.hashmapBuilder.GetSize())
			ctr.state = SendJoinMap

		case SendJoinMap:
			if ap.JoinMapTag <= 0 {
				panic("wrong joinmap message tag!")
			}
			var jm *message.JoinMap
			if ctr.hashmapBuilder.InputBatchRowCount > 0 {
				jm = message.NewJoinMap(ctr.hashmapBuilder.MultiSels, ctr.hashmapBuilder.IntHashMap, ctr.hashmapBuilder.StrHashMap, ctr.hashmapBuilder.Batches.Buf, proc.Mp())
				if ap.NeedBatches {
					jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
				}
				jm.IncRef(1)
			}
			message.SendMessage(message.JoinMapMsg{JoinMapPtr: jm, IsShuffle: true, ShuffleIdx: ap.ShuffleIdx, Tag: ap.JoinMapTag}, proc.GetMessageBoard())

			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) collectBuildBatches(shuffleBuild *ShuffleBuild, proc *process.Process, anal process.Analyze, isFirst bool) error {
	for {
		result, err := vm.ChildrenCall(shuffleBuild.Children[0], proc, anal)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}
		anal.Input(result.Batch, isFirst)
		anal.Alloc(int64(result.Batch.Size()))
		ctr.hashmapBuilder.InputBatchRowCount += result.Batch.RowCount()
		err = ctr.hashmapBuilder.Batches.CopyIntoBatches(result.Batch, proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *ShuffleBuild, proc *process.Process) error {
	if ap.RuntimeFilterSpec == nil {
		panic("there must be runtime filter in shuffle build!")
	}
	//only support runtime filter pass for now in shuffle join
	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = ap.RuntimeFilterSpec.Tag
	runtimeFilter.Typ = message.RuntimeFilter_PASS
	message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
	return nil
}
