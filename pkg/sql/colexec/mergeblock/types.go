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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	affectedRows     uint64
	container        *Container

	info     *vm.OperatorInfo
	children []vm.Operator
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.children = append(arg.children, child)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	// for k := range arg.container.mp {
	// 	arg.container.mp[k].Clean(proc.GetMPool())
	// 	arg.container.mp[k] = nil
	// }
}

func (arg *Argument) GetMetaLocBat(name string, proc *process.Process) {
	// If the target is a partition table
	if len(arg.PartitionSources) > 0 {
		// 'i' aligns with partition number
		for i := range arg.PartitionSources {
			bat := batch.NewWithSize(1)
			bat.Attrs = []string{name}
			bat.Cnt = 1
			bat.Vecs[0] = proc.GetVector(types.New(types.T_text, 0, 0))
			arg.container.mp[i] = bat
		}
	} else {
		bat := batch.NewWithSize(1)
		bat.Attrs = []string{name}
		bat.Cnt = 1
		bat.Vecs[0] = proc.GetVector(types.New(types.T_text, 0, 0))
		arg.container.mp[0] = bat
	}
}

func (arg *Argument) Split(proc *process.Process, bat *batch.Batch) error {
	arg.GetMetaLocBat(bat.Attrs[1], proc)
	tblIdx := vector.MustFixedCol[int16](bat.GetVector(0))
	blockInfos := vector.MustBytesCol(bat.GetVector(1))
	for i := range tblIdx {
		if tblIdx[i] >= 0 {
			blkInfo := catalog.DecodeBlockInfo(blockInfos[i])
			arg.affectedRows += uint64(blkInfo.MetaLocation().Rows())
			vector.AppendBytes(arg.container.mp[int(tblIdx[i])].Vecs[0],
				blockInfos[i], false, proc.GetMPool())
		} else {
			idx := int(-(tblIdx[i] + 1))
			newBat := &batch.Batch{}
			if err := newBat.UnmarshalBinary(blockInfos[i]); err != nil {
				return err
			}
			newBat.Cnt = 1
			arg.affectedRows += uint64(newBat.RowCount())
			arg.container.mp2[idx] = append(arg.container.mp2[idx], newBat)
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
