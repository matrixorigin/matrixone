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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

var _ vm.Operator = new(Argument)

type Container struct {
	// mp is used to store the metaLoc Batch.
	// Notice that batches in mp should be free, since the memory of these batches be allocated from mpool.
	mp map[int]*batch.Batch
	// mp2 is used to store the normal data batches
	mp2 map[int][]*batch.Batch
}

type Argument struct {
	// 1. main table
	Tbl engine.Relation
	// 2. partition sub tables
	PartitionSources []engine.Relation
	AddAffectedRows  bool
	affectedRows     uint64
	container        *Container

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) WithTbl(tbl engine.Relation) *Argument {
	arg.Tbl = tbl
	return arg
}

func (arg *Argument) WithPartitionSources(partitionSources []engine.Relation) *Argument {
	arg.PartitionSources = partitionSources
	return arg
}

func (arg *Argument) WithAddAffectedRows(addAffectedRows bool) *Argument {
	arg.AddAffectedRows = addAffectedRows
	return arg
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	// for k := range arg.container.mp {
	// 	arg.container.mp[k].Clean(proc.GetMPool())
	// 	arg.container.mp[k] = nil
	// }
}

func (arg *Argument) GetMetaLocBat(src *batch.Batch, proc *process.Process) {
	var typs []types.Type
	// exclude the table id column
	attrs := src.Attrs[1:]

	for idx := 1; idx < len(src.Vecs); idx++ {
		typs = append(typs, *src.Vecs[idx].GetType())
	}

	// src comes from old CN which haven't object stats column
	if src.Attrs[len(src.Attrs)-1] != catalog.ObjectMeta_ObjectStats {
		attrs = append(attrs, catalog.ObjectMeta_ObjectStats)
		typs = append(typs, types.T_binary.ToType())
	}

	// If the target is a partition table
	if len(arg.PartitionSources) > 0 {
		// 'i' aligns with partition number
		for i := range arg.PartitionSources {
			bat := batch.NewWithSize(len(attrs))
			bat.Attrs = attrs
			bat.Cnt = 1
			for idx := 0; idx < len(attrs); idx++ {
				bat.Vecs[idx] = proc.GetVector(typs[idx])
			}
			arg.container.mp[i] = bat
		}
	} else {
		bat := batch.NewWithSize(len(attrs))
		bat.Attrs = attrs
		bat.Cnt = 1
		for idx := 0; idx < len(attrs); idx++ {
			bat.Vecs[idx] = proc.GetVector(typs[idx])
		}
		arg.container.mp[0] = bat
	}
}

func splitObjectStats(arg *Argument, proc *process.Process,
	bat *batch.Batch, blkVec *vector.Vector, tblIdx []int16,
) error {
	// bat comes from old CN, no object stats vec in it.
	// to ensure all bats the TN received contain the object stats column, we should
	// construct the object stats from block info here.
	needLoad := bat.Attrs[len(bat.Attrs)-1] != catalog.ObjectMeta_ObjectStats

	fs, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		logutil.Error("get fs failed when split object stats. ", zap.Error(err))
		return err
	}

	objDataMeta := objectio.BuildObjectMeta(uint16(blkVec.Length()))
	var objStats objectio.ObjectStats
	statsVec := bat.Vecs[2]
	statsIdx := 0

	for idx := 0; idx < len(tblIdx); idx++ {
		if tblIdx[idx] < 0 {
			// will the data and blk infos mixed together in one batch?
			// batch [ data | data | blk info | blk info | .... ]
			continue
		}

		blkInfo := objectio.DecodeBlockInfo(blkVec.GetBytesAt(idx))
		if objectio.IsSameObjectLocVsMeta(blkInfo.MetaLocation(), objDataMeta) {
			continue
		}

		destVec := arg.container.mp[int(tblIdx[idx])].Vecs[1]

		if needLoad {
			// comes from old version cn
			objStats, objDataMeta, err = disttae.ConstructObjStatsByLoadObjMeta(proc.Ctx, blkInfo.MetaLocation(), fs)
			if err != nil {
				return err
			}

			vector.AppendBytes(destVec, objStats.Marshal(), false, proc.GetMPool())
		} else {
			// not comes from old version cn
			vector.AppendBytes(destVec, statsVec.GetBytesAt(statsIdx), false, proc.GetMPool())
			objDataMeta.BlockHeader().SetBlockID(&blkInfo.BlockID)
			statsIdx++
		}
	}

	return nil
}

func (arg *Argument) Split(proc *process.Process, bat *batch.Batch) error {
	// meta loc and object stats
	arg.GetMetaLocBat(bat, proc)
	tblIdx := vector.MustFixedCol[int16](bat.GetVector(0))
	blkInfosVec := bat.GetVector(1)

	hasObject := false
	for i := range tblIdx { // append s3 writer returned blk info
		if tblIdx[i] >= 0 {
			if arg.AddAffectedRows {
				blkInfo := objectio.DecodeBlockInfo(blkInfosVec.GetBytesAt(i))
				arg.affectedRows += uint64(blkInfo.MetaLocation().Rows())
			}
			vector.AppendBytes(arg.container.mp[int(tblIdx[i])].Vecs[0],
				blkInfosVec.GetBytesAt(i), false, proc.GetMPool())
			hasObject = true
		} else { // append data
			idx := int(-(tblIdx[i] + 1))
			newBat := &batch.Batch{}
			if err := newBat.UnmarshalBinary(blkInfosVec.GetBytesAt(i)); err != nil {
				return err
			}
			newBat.Cnt = 1
			if arg.AddAffectedRows {
				arg.affectedRows += uint64(newBat.RowCount())
			}
			arg.container.mp2[idx] = append(arg.container.mp2[idx], newBat)
		}
	}

	// exist blk info, split it
	if hasObject {
		if err := splitObjectStats(arg, proc, bat, blkInfosVec, tblIdx); err != nil {
			return err
		}
	}

	for _, b := range arg.container.mp {
		b.SetRowCount(b.Vecs[0].Length())
	}
	return nil
}

func (arg *Argument) AffectedRows() uint64 {
	return arg.affectedRows
}
