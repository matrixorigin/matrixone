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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MergeBlock)

type Container struct {
	// mp[0] collects the main table's [BlockMeta_BlockInfo,
	// ObjectMeta_ObjectStats] rows; its memory comes from the process pool.
	mp           map[int]*batch.Batch
	source       engine.Relation
	affectedRows uint64
}

type MergeBlock struct {
	AddAffectedRows bool
	Engine          engine.Engine
	Ref             *plan.ObjectRef
	container       Container

	vm.OperatorBase
}

func (mergeBlock *MergeBlock) GetOperatorBase() *vm.OperatorBase {
	return &mergeBlock.OperatorBase
}

func init() {
	reuse.CreatePool[MergeBlock](
		func() *MergeBlock {
			return &MergeBlock{}
		},
		func(a *MergeBlock) {
			*a = MergeBlock{}
		},
		reuse.DefaultOptions[MergeBlock]().
			WithEnableChecker(),
	)
}

func (mergeBlock MergeBlock) TypeName() string {
	return opName
}

func NewArgument() *MergeBlock {
	return reuse.Alloc[MergeBlock](nil)
}

func (mergeBlock *MergeBlock) WithObjectRef(ref *plan.ObjectRef) *MergeBlock {
	mergeBlock.Ref = ref
	return mergeBlock
}

func (mergeBlock *MergeBlock) WithEngine(eng engine.Engine) *MergeBlock {
	mergeBlock.Engine = eng
	return mergeBlock
}

func (mergeBlock *MergeBlock) WithAddAffectedRows(addAffectedRows bool) *MergeBlock {
	mergeBlock.AddAffectedRows = addAffectedRows
	return mergeBlock
}

func (mergeBlock *MergeBlock) Release() {
	if mergeBlock != nil {
		reuse.Free[MergeBlock](mergeBlock, nil)
	}
}

func (mergeBlock *MergeBlock) Reset(proc *process.Process, pipelineFailed bool, err error) {
	//can not reset affectRows, because MO need get affectRows after reset
	mergeBlock.resetMp(proc)
}

func (mergeBlock *MergeBlock) Free(proc *process.Process, pipelineFailed bool, err error) {
	mergeBlock.cleanMp(proc)
}

// metaLocBat returns mp[0], emptied, creating it on first use.
func (mergeBlock *MergeBlock) metaLocBat(proc *process.Process) *batch.Batch {
	if bat := mergeBlock.container.mp[0]; bat != nil {
		bat.CleanOnlyData()
		return bat
	}
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType(), proc.Mp())
	bat.Vecs[1] = vector.NewVec(types.T_binary.ToType(), proc.Mp())
	mergeBlock.container.mp[0] = bat
	return bat
}

// Split copies the block infos and object stats of bat, the insert S3
// writer's [BlockMeta_BlockInfo, ObjectMeta_ObjectStats] result, into mp[0]:
// every block info, and each object's stats once, at its first block.
func (mergeBlock *MergeBlock) Split(proc *process.Process, bat *batch.Batch, analyzer process.Analyzer) error {
	if len(bat.Vecs) != 2 ||
		bat.Vecs[0].GetType().Oid != types.T_text ||
		bat.Vecs[1].GetType().Oid != types.T_binary {
		return moerr.NewInternalErrorNoCtx("merge block input is not the S3 insert result layout")
	}
	dst := mergeBlock.metaLocBat(proc)
	blkInfosVec, statsVec := bat.Vecs[0], bat.Vecs[1]
	objDataMeta := objectio.BuildObjectMeta(uint16(blkInfosVec.Length()))
	var (
		statsIdx        int
		affectedRowsInc uint64
	)
	for i := range blkInfosVec.Length() {
		blkInfoBytes := blkInfosVec.GetBytesAt(i)
		if err := vector.AppendBytes(dst.Vecs[0], blkInfoBytes, false, proc.GetMPool()); err != nil {
			return err
		}
		blkInfo := objectio.DecodeBlockInfo(blkInfoBytes)
		if mergeBlock.AddAffectedRows {
			affectedRowsInc += uint64(blkInfo.MetaLocation().Rows())
		}
		// The stats column holds one entry per object, at its first block.
		if objectio.IsSameObjectLocVsMeta(blkInfo.MetaLocation(), objDataMeta) {
			continue
		}
		if err := vector.AppendBytes(
			dst.Vecs[1], statsVec.GetBytesAt(statsIdx), false, proc.GetMPool(),
		); err != nil {
			return err
		}
		objDataMeta.BlockHeader().SetBlockID(&blkInfo.BlockID)
		statsIdx++
	}
	dst.SetRowCount(dst.Vecs[0].Length())

	// Split can still fail while appending object stats after accepting block
	// infos. Publish the count only after the entire batch is ready so callers
	// never observe rows from a batch that the pipeline will roll back.
	mergeBlock.container.affectedRows += affectedRowsInc
	return nil
}

func (mergeBlock *MergeBlock) resetMp(proc *process.Process) {
	for k := range mergeBlock.container.mp {
		mergeBlock.container.mp[k].CleanOnlyData()
	}
}

func (mergeBlock *MergeBlock) cleanMp(proc *process.Process) {
	for k := range mergeBlock.container.mp {
		mergeBlock.container.mp[k].Clean(proc.GetMPool())
		mergeBlock.container.mp[k] = nil
	}
	mergeBlock.container.mp = nil
}

func (mergeBlock *MergeBlock) GetAffectedRows() uint64 {
	return mergeBlock.container.affectedRows
}
