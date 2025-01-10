// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mergeblock

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_block"

func (mergeBlock *MergeBlock) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": MergeS3BlocksMetaLoc ")
}

func (mergeBlock *MergeBlock) OpType() vm.OpType {
	return vm.MergeBlock
}

func (mergeBlock *MergeBlock) Prepare(proc *process.Process) error {
	if mergeBlock.OpAnalyzer == nil {
		mergeBlock.OpAnalyzer = process.NewAnalyzer(mergeBlock.GetIdx(), mergeBlock.IsFirst, mergeBlock.IsLast, "merge_block")
	} else {
		mergeBlock.OpAnalyzer.Reset()
	}

	if mergeBlock.container.mp == nil {
		mergeBlock.container.mp = make(map[int]*batch.Batch)
		mergeBlock.container.mp2 = make(map[int][]*batch.Batch)
	}

	ref := mergeBlock.Ref
	eng := mergeBlock.Engine
	rel, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref)
	if err != nil {
		return err
	}
	mergeBlock.container.source = rel
	mergeBlock.container.affectedRows = 0
	return nil
}

func (mergeBlock *MergeBlock) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := mergeBlock.OpAnalyzer

	var err error
	input, err := vm.ChildrenCall(mergeBlock.GetChildren(0), proc, analyzer)
	if err != nil {
		return input, err
	}

	if input.Batch == nil {
		return vm.CancelResult, nil
	}
	if input.Batch.IsEmpty() {
		return input, nil
	}
	bat := input.Batch
	if err := mergeBlock.Split(proc, bat, analyzer); err != nil {
		return input, err
	}

	// handle origin/main table.
	if mergeBlock.container.mp[0].RowCount() > 0 {
		crs := analyzer.GetOpCounterSet()
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

		//batches in mp will be deeply copied into txn's workspace.
		if err = mergeBlock.container.source.Write(newCtx, mergeBlock.container.mp[0]); err != nil {
			return input, err
		}
		analyzer.AddWrittenRows(int64(mergeBlock.container.mp[0].RowCount()))
		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)
	}

	for _, bat := range mergeBlock.container.mp2[0] {
		crs := analyzer.GetOpCounterSet()
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
		//batches in mp2 will be deeply copied into txn's workspace.
		if err = mergeBlock.container.source.Write(newCtx, bat); err != nil {
			return input, err
		}
		analyzer.AddWrittenRows(int64(bat.RowCount()))
		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)

		bat.Clean(proc.GetMPool())
	}
	mergeBlock.container.mp2[0] = mergeBlock.container.mp2[0][:0]

	return input, nil
}
