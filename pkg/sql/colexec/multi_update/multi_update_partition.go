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
	"github.com/matrixorigin/matrixone/pkg/partitionprune"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PartitionMultiUpdate struct {
	vm.OperatorBase

	raw              *MultiUpdate
	tableID          uint64
	meta             partition.PartitionMetadata
	mainIndexes      []uint64
	partitionIndexes map[uint64][]engine.Relation
	rawTableIDs      []uint64
	rawTableFlags    []uint64
	writers          map[uint64]*s3WriterDelegate
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

	var err error
	op.meta, _, err = proc.GetPartitionService().GetStorage().GetMetadata(
		proc.Ctx,
		op.tableID,
		proc.GetTxnOperator(),
	)
	if err != nil {
		return err
	}
	_, _, r, err := op.raw.Engine.GetRelationById(
		proc.Ctx,
		proc.GetTxnOperator(),
		op.tableID,
	)
	if err != nil {
		return err
	}
	if len(r.GetExtraInfo().IndexTables) > 0 {
		op.mainIndexes = r.GetExtraInfo().IndexTables
		op.partitionIndexes = make(map[uint64][]engine.Relation, len(op.meta.Partitions))
	}

	op.raw.OperatorBase = op.OperatorBase
	if err = op.raw.Prepare(proc); err != nil {
		return err
	}

	op.rawTableIDs = make([]uint64, 0, len(op.raw.MultiUpdateCtx))
	op.rawTableFlags = make([]uint64, 0, len(op.raw.MultiUpdateCtx))
	for _, c := range op.raw.MultiUpdateCtx {
		op.rawTableIDs = append(op.rawTableIDs, c.TableDef.TblId)
		op.rawTableFlags = append(op.rawTableFlags, c.TableDef.FeatureFlag)
	}
	return nil
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

	pos := int32(-1)
	if len(op.raw.MultiUpdateCtx[0].PartitionCols) > 0 {
		pos = int32(op.raw.MultiUpdateCtx[0].PartitionCols[0])
	}
	res, err := partitionprune.Prune(proc, input.Batch, op.meta, pos)
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
			_, _, rel, err = op.raw.Engine.GetRelationById(
				proc.Ctx,
				proc.GetTxnOperator(),
				partition.PartitionID,
			)
			if err != nil {
				return false
			}

			// mapping all main table and index table to partition's.
			for i, c := range op.raw.MultiUpdateCtx {
				r := rel
				if features.IsIndexTable(op.rawTableFlags[i]) {
					r, err = op.getPartitionIndex(
						proc,
						op.rawTableIDs[i],
						partition.PartitionID,
						rel,
					)
					if err != nil {
						return false
					}
				}

				c.ObjRef.ObjName = r.GetTableName()
				c.TableDef = r.GetTableDef(proc.Ctx)
			}
			op.raw.resetMultiUpdateCtxs()
			if err = op.raw.resetMultiSources(proc); err != nil {
				return false
			}
			op.raw.input = vm.CallResult{Batch: bat}

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

func (op *PartitionMultiUpdate) getPartitionIndex(
	proc *process.Process,
	tableID uint64,
	partitionID uint64,
	partitionRel engine.Relation,
) (engine.Relation, error) {
	for i, id := range op.mainIndexes {
		if id == tableID {
			indexes, ok := op.partitionIndexes[partitionID]
			if ok {
				return indexes[i], nil
			}

			relations := make([]engine.Relation, 0, len(op.meta.Partitions))
			for _, index := range partitionRel.GetExtraInfo().IndexTables {
				_, _, rel, err := op.raw.Engine.GetRelationById(
					proc.Ctx,
					proc.GetTxnOperator(),
					index,
				)
				if err != nil {
					return nil, err
				}
				relations = append(relations, rel)
			}
			op.partitionIndexes[partitionID] = relations

			return relations[i], nil
		}
	}

	panic("BUG")
}

func (op *PartitionMultiUpdate) getS3Writer(
	id uint64,
) (*s3WriterDelegate, error) {
	var err error
	w, ok := op.writers[id]
	if !ok {
		w, err = newS3Writer(op.raw)
		if err != nil {
			return nil, err
		}
		op.writers[id] = w
	}
	return w, nil
}

func (op *PartitionMultiUpdate) getFlushableS3Writer() *s3WriterDelegate {
	for k, w := range op.writers {
		delete(op.writers, k)
		return w
	}
	return nil
}
