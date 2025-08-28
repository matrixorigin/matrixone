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

package insert

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/partitionprune"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PartitionInsert struct {
	vm.OperatorBase

	raw     *Insert
	tableID uint64
	meta    partition.PartitionMetadata
}

func NewPartitionInsert(
	raw *Insert,
	tableID uint64,
) vm.Operator {
	return &PartitionInsert{
		raw:     raw,
		tableID: tableID,
	}
}

func NewPartitionInsertFrom(
	ps *PartitionInsert,
) vm.Operator {
	raw := NewArgument()
	raw.InsertCtx = ps.raw.InsertCtx
	raw.ToWriteS3 = ps.raw.ToWriteS3
	return NewPartitionInsert(raw, ps.tableID)
}

func (op *PartitionInsert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": partition_insert")
}

func (op *PartitionInsert) OpType() vm.OpType {
	return vm.PartitionInsert
}

func (op *PartitionInsert) Prepare(
	proc *process.Process,
) error {
	if op.OpAnalyzer == nil {
		op.OpAnalyzer = process.NewAnalyzer(op.GetIdx(), op.IsFirst, op.IsLast, "partition_insert")
	} else {
		op.OpAnalyzer.Reset()
	}

	var err error
	op.meta, _, err = proc.GetPartitionService().GetStorage().GetMetadata(proc.Ctx, op.tableID, proc.GetTxnOperator())
	if err != nil {
		return err
	}
	op.raw.OperatorBase = op.OperatorBase
	return op.raw.Prepare(proc)
}

func (op *PartitionInsert) Call(
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

	res, err := partitionprune.Prune(proc, input.Batch, op.meta, -1)
	if err != nil {
		return vm.CallResult{}, err
	}
	defer res.Close()
	if res.Empty() {
		panic("Prune result is empty")
	}

	ref := op.raw.InsertCtx.Ref
	eng := op.raw.InsertCtx.Engine
	oldName := ref.ObjName
	defer func() {
		ref.ObjName = oldName
	}()

	var rel engine.Relation
	res.Iter(
		func(
			partition partition.Partition,
			bat *batch.Batch,
		) bool {
			ref.ObjName = partition.PartitionTableName
			rel, err = colexec.GetRelAndPartitionRelsByObjRef(
				proc.Ctx,
				proc,
				eng,
				ref,
			)
			if err != nil {
				return false
			}
			op.raw.ctr.source = rel
			op.raw.input = vm.CallResult{Batch: bat}
			_, e := op.raw.Call(proc)
			if e != nil {
				err = e
				return false
			}
			return true
		},
	)
	if err != nil {
		return vm.CallResult{}, err
	}
	return input, nil
}

func (op *PartitionInsert) ExecProjection(
	proc *process.Process,
	input *batch.Batch,
) (*batch.Batch, error) {
	return input, nil
}

func (op *PartitionInsert) Free(
	proc *process.Process,
	pipelineFailed bool,
	err error,
) {
	op.raw.Free(proc, pipelineFailed, err)
}

func (op *PartitionInsert) Release() {
	op.raw.Release()
}

func (op *PartitionInsert) Reset(
	proc *process.Process,
	pipelineFailed bool,
	err error,
) {
	op.raw.Reset(proc, pipelineFailed, err)
}

func (op *PartitionInsert) GetOperatorBase() *vm.OperatorBase {
	return &op.OperatorBase
}
