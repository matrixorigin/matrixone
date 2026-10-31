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

package mergerecursive

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_recursive"

func (mergeRecursive *MergeRecursive) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": merge recursive ")
}

func (mergeRecursive *MergeRecursive) OpType() vm.OpType {
	return vm.MergeRecursive
}

func (mergeRecursive *MergeRecursive) Prepare(proc *process.Process) error {
	if mergeRecursive.OpAnalyzer == nil {
		mergeRecursive.OpAnalyzer = process.NewAnalyzer(mergeRecursive.GetIdx(), mergeRecursive.IsFirst, mergeRecursive.IsLast, "merge recursive")
	} else {
		mergeRecursive.OpAnalyzer.Reset()
	}

	return mergeRecursive.ctr.bindMemory(proc)
}

func (mergeRecursive *MergeRecursive) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := mergeRecursive.OpAnalyzer

	ctr := &mergeRecursive.ctr

	result := vm.NewCallResult()
	var err error
	for !ctr.last {
		//result, err = mergeRecursive.GetChildren(0).Call(proc)
		result, err = vm.ChildrenCall(mergeRecursive.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}
		bat := result.Batch
		if bat == nil || bat.End() {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		if bat.Last() {
			ctr.last = true
		}

		appBat, err := ctr.cacheBatch(proc, analyzer, result.Batch)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		mergeRecursive.ctr.bats = append(mergeRecursive.ctr.bats, appBat)
	}
	mergeRecursive.ctr.buf = mergeRecursive.ctr.bats[0]
	mergeRecursive.ctr.bats = mergeRecursive.ctr.bats[1:]

	if mergeRecursive.ctr.buf.Last() {
		mergeRecursive.ctr.last = false
	}

	if mergeRecursive.ctr.buf.End() {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	result.Batch = mergeRecursive.ctr.buf
	result.Status = vm.ExecHasMore
	return result, nil
}

func (ctr *container) cacheBatch(proc *process.Process, analyzer process.Analyzer, src *batch.Batch) (*batch.Batch, error) {
	appBat, err := ctr.memory.CacheBatch(proc, analyzer, &ctr.freeBats, ctr.i, src, nil)
	if err != nil {
		return nil, err
	}
	ctr.i++
	return appBat, nil
}

func (ctr *container) bindMemory(proc *process.Process) error {
	err := ctr.memory.Bind(proc, ctr.freeBats)
	if err == nil || !moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded) {
		return err
	}
	for _, bat := range ctr.freeBats {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
	ctr.bats = nil
	ctr.buf = nil
	ctr.freeBats = nil
	ctr.i = 0
	ctr.memory.Release()
	return ctr.memory.Bind(proc, nil)
}
