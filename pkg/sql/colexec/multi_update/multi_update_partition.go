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

	raw          *MultiUpdate
	rawContexts  []*MultiUpdateCtx
	targets      []*partitionUpdateTarget
	affectedRows uint64
	writers      map[uint64]*s3WriterDelegate
	freeWriters  []*s3WriterDelegate
	nextWriterID uint64
}

type partitionUpdateTarget struct {
	contexts         []*MultiUpdateCtx
	tableID          uint64
	meta             partition.PartitionMetadata
	mainIndexes      []uint64
	partitionIndexes map[uint64][]engine.Relation
	writerIDs        map[uint64]uint64
}

func NewPartitionMultiUpdate(
	raw *MultiUpdate,
) vm.Operator {
	if raw.Action == UpdateFlushS3Info {
		return raw
	}

	return &PartitionMultiUpdate{
		raw: raw,
	}
}

func NewPartitionMultiUpdateFrom(
	from *PartitionMultiUpdate,
) vm.Operator {
	op := NewArgument()
	op.MultiUpdateCtx = from.raw.MultiUpdateCtx
	op.Action = from.raw.Action
	op.IsOnduplicateKeyUpdate = from.raw.IsOnduplicateKeyUpdate
	op.CountDeleteAffectRows = from.raw.CountDeleteAffectRows
	op.RejectZeroTemporal = from.raw.RejectZeroTemporal
	op.Engine = from.raw.Engine
	return NewPartitionMultiUpdate(op)
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
	if err := op.raw.Prepare(proc); err != nil {
		return err
	}

	op.rawContexts = op.raw.MultiUpdateCtx
	op.targets = buildPartitionUpdateTargets(op.rawContexts)
	for _, target := range op.targets {
		if !features.IsPartitioned(target.contexts[0].TableDef.FeatureFlag) {
			continue
		}

		var err error
		target.meta, _, err = proc.GetPartitionService().GetStorage().GetMetadata(
			proc.Ctx,
			target.tableID,
			proc.GetTxnOperator(),
		)
		if err != nil {
			return err
		}
		_, _, r, err := op.raw.Engine.GetRelationById(
			proc.Ctx,
			proc.GetTxnOperator(),
			target.tableID,
		)
		if err != nil {
			return err
		}
		if len(r.GetExtraInfo().IndexTables) > 0 {
			target.mainIndexes = r.GetExtraInfo().IndexTables
			target.partitionIndexes = make(map[uint64][]engine.Relation, len(target.meta.Partitions))
		}
	}

	op.affectedRows = 0
	op.raw.getS3WriterFunc = op.getS3Writer
	op.raw.getFlushableS3WriterFunc = op.getFlushableS3Writer
	op.raw.addAffectedRowsFunc = op.doAddAffectedRows
	op.writers = make(map[uint64]*s3WriterDelegate)
	op.nextWriterID = 0
	return nil
}

func buildPartitionUpdateTargets(contexts []*MultiUpdateCtx) []*partitionUpdateTarget {
	targetsByMain := make(map[int]*partitionUpdateTarget)
	targets := make([]*partitionUpdateTarget, 0, len(contexts))
	for i, ctx := range contexts {
		if features.IsIndexTable(ctx.TableDef.FeatureFlag) {
			continue
		}
		target := &partitionUpdateTarget{
			contexts:  []*MultiUpdateCtx{cloneTargetContext(ctx)},
			tableID:   ctx.TableDef.TblId,
			writerIDs: make(map[uint64]uint64),
		}
		targetsByMain[i] = target
		targets = append(targets, target)
	}
	for _, ctx := range contexts {
		if !features.IsIndexTable(ctx.TableDef.FeatureFlag) {
			continue
		}
		if target := targetsByMain[ctx.TargetUpdateCtxIdx]; target != nil {
			target.contexts = append(target.contexts, cloneTargetContext(ctx))
		}
	}
	return targets
}

func cloneTargetContext(ctx *MultiUpdateCtx) *MultiUpdateCtx {
	cloned := ctx.clone()
	cloned.TargetUpdateCtxIdx = 0
	return cloned
}

func (op *PartitionMultiUpdate) Call(
	proc *process.Process,
) (vm.CallResult, error) {
	if op.raw.Action == UpdateWriteTable {
		return op.writeTable(proc)
	}
	return op.writeS3(proc)
}

func (op *PartitionMultiUpdate) writeTable(
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

	for _, target := range op.targets {
		if err := op.writeTarget(proc, target, input.Batch); err != nil {
			return vm.CallResult{}, err
		}
	}
	return input, nil
}

func (op *PartitionMultiUpdate) writeS3(
	proc *process.Process,
) (vm.CallResult, error) {
	for {
		input, err := vm.ChildrenCall(
			op.GetChildren(0),
			proc,
			op.raw.OpAnalyzer,
		)
		if err != nil {
			return input, err
		}

		op.raw.delegated = true
		op.raw.input = input

		if input.Batch == nil {
			if _, err := op.raw.Call(proc); err != nil {
				return input, err
			}
			return op.raw.Call(proc)
		}

		if input.Batch.IsEmpty() {
			continue
		}

		for _, target := range op.targets {
			if err := op.writeTarget(proc, target, input.Batch); err != nil {
				return vm.CallResult{}, err
			}
		}
	}
}

func (op *PartitionMultiUpdate) writeTarget(
	proc *process.Process,
	target *partitionUpdateTarget,
	input *batch.Batch,
) error {
	if !features.IsPartitioned(target.contexts[0].TableDef.FeatureFlag) {
		return op.callRawTarget(proc, target, target.contexts, target.tableID, input)
	}
	if len(target.contexts[0].PartitionCols) > 1 {
		return op.writePartitionKeyUpdate(proc, target, input)
	}

	pos := int32(-1)
	if len(target.contexts[0].PartitionCols) > 0 {
		pos = int32(target.contexts[0].PartitionCols[0])
	}
	res, err := partitionprune.Prune(proc, input, target.meta, pos)
	if err != nil {
		return err
	}
	defer res.Close()
	if res.Empty() {
		panic("Prune result is empty")
	}

	res.Iter(func(p partition.Partition, bat *batch.Batch) bool {
		contexts, resolveErr := op.resolvePartitionContexts(proc, target, target.contexts, p)
		if resolveErr != nil {
			err = resolveErr
			return false
		}
		err = op.callRawTarget(proc, target, contexts, p.PartitionID, bat)
		return err == nil
	})
	return err
}

func (op *PartitionMultiUpdate) resolvePartitionContexts(
	proc *process.Process,
	target *partitionUpdateTarget,
	contexts []*MultiUpdateCtx,
	p partition.Partition,
) ([]*MultiUpdateCtx, error) {
	_, _, rel, err := op.raw.Engine.GetRelationById(
		proc.Ctx,
		proc.GetTxnOperator(),
		p.PartitionID,
	)
	if err != nil {
		return nil, err
	}

	resolved := make([]*MultiUpdateCtx, len(contexts))
	for i, ctx := range contexts {
		r := rel
		if features.IsIndexTable(ctx.TableDef.FeatureFlag) {
			r, err = op.getPartitionIndex(
				proc,
				target,
				ctx.TableDef.TblId,
				p.PartitionID,
				rel,
			)
			if err != nil {
				return nil, err
			}
		}
		resolved[i] = ctx.clone()
		resolved[i].ObjRef.ObjName = r.GetTableName()
		resolved[i].TableDef = r.GetTableDef(proc.Ctx)
	}
	return resolved, nil
}

func (op *PartitionMultiUpdate) writePartitionKeyUpdate(
	proc *process.Process,
	target *partitionUpdateTarget,
	input *batch.Batch,
) error {
	mainCtx := target.contexts[0]
	filtered, owned, duplicateRows, err := filterTargetRows(
		proc,
		mainCtx,
		input,
		op.raw.ctr.seenTargetRows[targetTableID(mainCtx)],
	)
	if err != nil {
		return err
	}
	if owned {
		defer filtered.Clean(proc.Mp())
	}
	op.raw.addAffectedRowsFunc(duplicateRows)
	if filtered.RowCount() == 0 {
		return nil
	}

	deleteContexts := clonePartitionPhaseContexts(target.contexts, true)
	if err = op.writePartitionPhase(
		proc, target, deleteContexts, mainCtx.PartitionCols[0], filtered,
	); err != nil {
		return err
	}
	insertContexts := clonePartitionPhaseContexts(target.contexts, false)
	return op.writePartitionPhase(
		proc, target, insertContexts, mainCtx.PartitionCols[1], filtered,
	)
}

func clonePartitionPhaseContexts(
	contexts []*MultiUpdateCtx,
	deletePhase bool,
) []*MultiUpdateCtx {
	cloned := make([]*MultiUpdateCtx, len(contexts))
	for i, ctx := range contexts {
		cloned[i] = ctx.clone()
		cloned[i].DedupByTargetRowID = false
		if deletePhase {
			cloned[i].InsertCols = nil
		} else {
			cloned[i].DeleteCols = nil
		}
	}
	return cloned
}

func (op *PartitionMultiUpdate) writePartitionPhase(
	proc *process.Process,
	target *partitionUpdateTarget,
	contexts []*MultiUpdateCtx,
	partitionCol int,
	input *batch.Batch,
) error {
	res, err := partitionprune.Prune(proc, input, target.meta, int32(partitionCol))
	if err != nil {
		return err
	}
	defer res.Close()
	if res.Empty() {
		return nil
	}

	res.Iter(func(p partition.Partition, bat *batch.Batch) bool {
		partitionContexts, resolveErr := op.resolvePartitionContexts(proc, target, contexts, p)
		if resolveErr != nil {
			err = resolveErr
			return false
		}
		err = op.callRawTarget(proc, target, partitionContexts, p.PartitionID, bat)
		return err == nil
	})
	return err
}

func (op *PartitionMultiUpdate) callRawTarget(
	proc *process.Process,
	target *partitionUpdateTarget,
	contexts []*MultiUpdateCtx,
	mainTable uint64,
	input *batch.Batch,
) error {
	op.raw.MultiUpdateCtx = contexts
	if op.raw.Action == UpdateWriteTable {
		op.raw.cleanTargetBuffers(proc)
	}
	op.raw.resetMultiUpdateCtxs()
	if err := op.raw.resetMultiSources(proc); err != nil {
		return err
	}
	op.raw.mainTable = mainTable
	if op.raw.Action == UpdateWriteS3 {
		op.raw.mainTable = op.writerID(target, mainTable)
	}
	op.raw.input = vm.CallResult{Batch: input}
	_, err := op.raw.Call(proc)
	return err
}

func (update *MultiUpdate) cleanTargetBuffers(proc *process.Process) {
	for _, buf := range update.ctr.insertBuf {
		if buf != nil {
			buf.Clean(proc.Mp())
		}
	}
	update.ctr.insertBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
	for _, buf := range update.ctr.deleteBuf {
		if buf != nil {
			buf.Clean(proc.Mp())
		}
	}
	update.ctr.deleteBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
}

func (op *PartitionMultiUpdate) writerID(target *partitionUpdateTarget, physicalTableID uint64) uint64 {
	if id, ok := target.writerIDs[physicalTableID]; ok {
		return id
	}
	op.nextWriterID++
	target.writerIDs[physicalTableID] = op.nextWriterID
	return op.nextWriterID
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
	op.freePartitionWriters(proc)
}

func (op *PartitionMultiUpdate) Release() {
	op.raw.Release()
}

func (op *PartitionMultiUpdate) Reset(
	proc *process.Process,
	pipelineFailed bool,
	err error,
) {
	op.raw.MultiUpdateCtx = op.rawContexts
	op.raw.Reset(proc, pipelineFailed, err)
	op.freePartitionWriters(proc)
	for _, target := range op.targets {
		clear(target.writerIDs)
	}
	op.nextWriterID = 0
}

func (op *PartitionMultiUpdate) freePartitionWriters(proc *process.Process) {
	for id, writer := range op.writers {
		_ = writer.free(proc)
		delete(op.writers, id)
	}
	for _, writer := range op.freeWriters {
		_ = writer.free(proc)
	}
	op.freeWriters = nil
}

func (op *PartitionMultiUpdate) GetOperatorBase() *vm.OperatorBase {
	return &op.OperatorBase
}

func (op *PartitionMultiUpdate) SetRejectZeroTemporal(reject bool) {
	op.raw.SetRejectZeroTemporal(reject)
	for _, writer := range op.writers {
		writer.rejectZeroTemporal = reject
	}
	for _, writer := range op.freeWriters {
		writer.rejectZeroTemporal = reject
	}
}

func (op *PartitionMultiUpdate) getPartitionIndex(
	proc *process.Process,
	target *partitionUpdateTarget,
	tableID uint64,
	partitionID uint64,
	partitionRel engine.Relation,
) (engine.Relation, error) {
	for i, id := range target.mainIndexes {
		if id == tableID {
			indexes, ok := target.partitionIndexes[partitionID]
			if ok {
				return indexes[i], nil
			}

			relations := make([]engine.Relation, 0, len(target.meta.Partitions))
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
			target.partitionIndexes[partitionID] = relations

			return relations[i], nil
		}
	}

	panic("BUG")
}

func (op *PartitionMultiUpdate) getS3Writer(
	sid string,
	id uint64,
) (*s3WriterDelegate, error) {
	var err error
	w, ok := op.writers[id]
	if !ok {
		w, err = newS3Writer(sid, op.raw)
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
		op.freeWriters = append(op.freeWriters, w)
		return w
	}
	return nil
}

func (op *PartitionMultiUpdate) doAddAffectedRows(affectedRows uint64) {
	op.affectedRows += affectedRows
}

func (op *PartitionMultiUpdate) GetAffectedRows() uint64 {
	return op.affectedRows
}

func (op *PartitionMultiUpdate) SetAffectedRows(affectedRows uint64) {
	op.affectedRows = affectedRows
}

func (ctx *MultiUpdateCtx) clone() *MultiUpdateCtx {
	v := &MultiUpdateCtx{
		InsertCols:         ctx.InsertCols,
		DeleteCols:         ctx.DeleteCols,
		PartitionCols:      ctx.PartitionCols,
		SkipInsertOnNullPk: ctx.SkipInsertOnNullPk,
		InsertPkColIdx:     ctx.InsertPkColIdx,
		IgnoreAffectedRows: ctx.IgnoreAffectedRows,
		DedupByTargetRowID: ctx.DedupByTargetRowID,
		TargetUpdateCtxIdx: ctx.TargetUpdateCtxIdx,
		TargetTableID:      ctx.TargetTableID,
	}
	objRef := *ctx.ObjRef
	def := *ctx.TableDef
	v.ObjRef = &objRef
	v.TableDef = &def
	return v
}
