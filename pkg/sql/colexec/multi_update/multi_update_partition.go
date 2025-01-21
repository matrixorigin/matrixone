// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PartitionMultiUpdate struct {
	vm.OperatorBase

	raw     *MultiUpdate
	tableID uint64
}

func NewPartitionMultiUpdate(
	raw *MultiUpdate,
	tableID uint64,
) vm.Operator {
	return &PartitionMultiUpdate{
		raw:     raw,
		tableID: tableID,
	}
}

func NewPartitionMultiUpdateFrom(
	from *PartitionMultiUpdate,
) vm.Operator {
	op := NewArgument()
	op.MultiUpdateCtx = from.raw.MultiUpdateCtx
	op.Action = from.raw.Action
	op.IsOnduplicateKeyUpdate = from.raw.IsOnduplicateKeyUpdate
	op.Engine = from.raw.Engine
	return NewPartitionMultiUpdate(op, from.tableID)
}

func (op *PartitionMultiUpdate) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": partition_multi_update")
}

func (op *PartitionMultiUpdate) OpType() vm.OpType {
	return vm.PartitionMultiUpdate
}

func (op *PartitionMultiUpdate) Prepare(
	proc *process.Process,
) error {
	if op.OpAnalyzer == nil {
		op.OpAnalyzer = process.NewAnalyzer(op.GetIdx(), op.IsFirst, op.IsLast, "partition_multi_update")
	} else {
		op.OpAnalyzer.Reset()
	}

	op.raw.OperatorBase = op.OperatorBase
	return op.raw.Prepare(proc)
}

func (op *PartitionMultiUpdate) Call(
	proc *process.Process,
) (vm.CallResult, error) {
	input, err := vm.ChildrenCall(
		op.GetChildren(0),
		proc,
		op.raw.OpAnalyzer,
	)
	if err != nil {
		return input, err
	}
	if input.Batch == nil || input.Batch.IsEmpty() {
		return input, nil
	}

	op.raw.delegated = true
	op.raw.input = input

	ps := proc.GetPartitionService()

	res, err := ps.Prune(
		proc.Ctx,
		op.tableID,
		input.Batch,
		proc.GetTxnOperator(),
	)
	if err != nil {
		return vm.CallResult{}, err
	}
	defer res.Close()
	if res.Empty() {
		panic("Prune result is empty")
	}

	var rel engine.Relation
	res.Iter(
		func(
			partition partition.Partition,
			bat *batch.Batch,
		) bool {
			for _, c := range op.raw.MultiUpdateCtx {
				c.ObjRef.ObjName = partition.PartitionTableName
				rel, err = colexec.GetRelAndPartitionRelsByObjRef(
					proc.Ctx,
					proc,
					op.raw.Engine,
					c.ObjRef,
				)
				if err != nil {
					return false
				}

				c.ObjRef.ObjName = partition.PartitionTableName
				c.TableDef = rel.GetTableDef(proc.Ctx)
			}
			op.raw.resetMultiUpdateCtxs()
			if err = op.raw.resetMultiSources(proc); err != nil {
				return false
			}

			_, err = op.raw.Call(proc)
			return err == nil
		},
	)
	if err != nil {
		return vm.CallResult{}, err
	}
	return input, nil
}

func (op *PartitionMultiUpdate) ExecProjection(
	proc *process.Process,
	input *batch.Batch,
) (*batch.Batch, error) {
	return input, nil
}

func (op *PartitionMultiUpdate) Free(
	proc *process.Process,
	pipelineFailed bool,
	err error,
) {
	op.raw.Free(proc, pipelineFailed, err)
	*op = PartitionMultiUpdate{}
}

func (op *PartitionMultiUpdate) Release() {
	op.raw.Release()
}

func (op *PartitionMultiUpdate) Reset(
	proc *process.Process,
	pipelineFailed bool,
	err error,
) {
	op.raw.Reset(proc, pipelineFailed, err)
}

func (op *PartitionMultiUpdate) GetOperatorBase() *vm.OperatorBase {
	return &op.OperatorBase
}
