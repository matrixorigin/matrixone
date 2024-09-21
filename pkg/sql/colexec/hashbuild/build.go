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

package hashbuild

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "hash_build"

func (hashBuild *HashBuild) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": hash build ")
}

func (hashBuild *HashBuild) OpType() vm.OpType {
	return vm.HashBuild
}

func (hashBuild *HashBuild) Prepare(proc *process.Process) (err error) {
	if hashBuild.OpAnalyzer == nil {
		hashBuild.OpAnalyzer = process.NewAnalyzer(hashBuild.GetIdx(), hashBuild.IsFirst, hashBuild.IsLast, "hash build")
	} else {
		hashBuild.OpAnalyzer.Reset()
	}

	if hashBuild.NeedHashMap {
		return hashBuild.ctr.hashmapBuilder.Prepare(hashBuild.Conditions, proc)
	}
	return nil
}

func (hashBuild *HashBuild) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := hashBuild.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result := vm.NewCallResult()
	ap := hashBuild
	ctr := &ap.ctr
	for {
		switch ctr.state {
		case BuildHashMap:
			if err := ctr.build(ap, proc, analyzer); err != nil {
				analyzer.Output(result.Batch)
				return result, err
			}
			analyzer.Alloc(ctr.hashmapBuilder.GetSize())
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(ap, proc); err != nil {
				analyzer.Output(result.Batch)
				return result, err
			}
			ctr.state = SendJoinMap

		case SendJoinMap:
			var jm *message.JoinMap
			if ctr.hashmapBuilder.InputBatchRowCount > 0 {
				jm = message.NewJoinMap(ctr.hashmapBuilder.MultiSels, ctr.hashmapBuilder.IntHashMap, ctr.hashmapBuilder.StrHashMap, ctr.hashmapBuilder.Batches.Buf, proc.Mp())
				jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
				if ap.NeedBatches {
					jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
				}
				jm.IncRef(ap.JoinMapRefCnt)
			}
			if ap.JoinMapTag <= 0 {
				panic("wrong joinmap message tag!")
			}
			message.SendMessage(message.JoinMapMsg{JoinMapPtr: jm, Tag: ap.JoinMapTag}, proc.GetMessageBoard())

			result.Batch = nil
			result.Status = vm.ExecStop
			analyzer.Output(result.Batch)
			return result, nil
		}
	}
}

func (ctr *container) collectBuildBatches(hashBuild *HashBuild, proc *process.Process, analyzer process.Analyzer) error {
	for {
		result, err := vm.ChildrenCall(hashBuild.GetChildren(0), proc, analyzer)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}

		analyzer.Alloc(int64(result.Batch.Size()))
		ctr.hashmapBuilder.InputBatchRowCount += result.Batch.RowCount()
		err = ctr.hashmapBuilder.Batches.CopyIntoBatches(result.Batch, proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) build(ap *HashBuild, proc *process.Process, analyzer process.Analyzer) error {
	err := ctr.collectBuildBatches(ap, proc, analyzer)
	if err != nil {
		return err
	}
	if ap.NeedHashMap {
		err = ctr.hashmapBuilder.BuildHashmap(ap.HashOnPK, ap.NeedAllocateSels, ap.RuntimeFilterSpec, proc)
	}
	if err != nil {
		return err
	}
	if !ap.NeedBatches {
		// if do not need merged batch, free it now to save memory
		ctr.hashmapBuilder.Batches.Clean(proc.Mp())
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *HashBuild, proc *process.Process) error {
	if ap.RuntimeFilterSpec == nil {
		return nil
	}

	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = ap.RuntimeFilterSpec.Tag

	if ap.RuntimeFilterSpec.Expr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else if ctr.hashmapBuilder.InputBatchRowCount == 0 || len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 || ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	}

	hashmapCount := ctr.hashmapBuilder.GetGroupCount()

	inFilterCardLimit := ap.RuntimeFilterSpec.UpperLimit
	//inFilterCardLimit := plan.GetInFilterCardLimit()
	//bloomFilterCardLimit := int64(plan.BloomFilterCardLimit)
	//v, ok = runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_bloom_filter")
	//if ok {
	//	bloomFilterCardLimit = v.(int64)
	//}

	vec := ctr.hashmapBuilder.UniqueJoinKeys[0]

	defer func() {
		vec.Free(proc.Mp())
		ctr.hashmapBuilder.UniqueJoinKeys = nil
	}()

	if hashmapCount > uint64(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else {
		// Composite primary key
		if ap.RuntimeFilterSpec.Expr.GetF() != nil {
			bat := batch.NewWithSize(len(ctr.hashmapBuilder.UniqueJoinKeys))
			bat.SetRowCount(vec.Length())
			copy(bat.Vecs, ctr.hashmapBuilder.UniqueJoinKeys)

			newVec, err := colexec.EvalExpressionOnce(proc, ap.RuntimeFilterSpec.Expr, []*batch.Batch{bat})
			if err != nil {
				return err
			}

			for i := range ctr.hashmapBuilder.UniqueJoinKeys {
				ctr.hashmapBuilder.UniqueJoinKeys[i].Free(proc.Mp())
			}
			vec = newVec
		}

		vec.InplaceSort()
		data, err := vec.MarshalBinary()
		if err != nil {
			return err
		}

		runtimeFilter.Typ = message.RuntimeFilter_IN
		runtimeFilter.Card = int32(vec.Length())
		runtimeFilter.Data = data
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterIn = true
	}
	return nil
}
