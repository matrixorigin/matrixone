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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

func (arg *Argument) GetMetaLocBat(src *batch.Batch, proc *process.Process) {
	var typs []types.Type
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

func splitObjectStats(arg *Argument, proc *process.Process, bat *batch.Batch, blkByte []byte) error {
	var statsBytes []byte

	// bat comes from old CN, no object stats vec in it.
	// to ensure all bats the TN received contain the object stats column, we should
	// construct the object stats from block info here.
	if bat.Attrs[len(bat.Attrs)-1] != catalog.ObjectMeta_ObjectStats {
		logutil.Info("found blk info bat comes from old CN.")
		// 1. load object meta

		blk := catalog.DecodeBlockInfo(blkByte)
		loc := blk.MetaLocation()

		fs, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
		if err != nil {
			logutil.Error("get fs failed when split object stats. ", zap.Error(err))
			return err
		}

		var meta objectio.ObjectMeta
		if meta, err = objectio.FastLoadObjectMeta(proc.Ctx, &loc, false, fs); err != nil {
			logutil.Error("fast load object meta failed when split object stats. ", zap.Error(err))
			return err
		}
		dataMeta := meta.MustDataMeta()

		// 2. construct an object stats
		stats := objectio.NewObjectStats()
		objectio.SetObjectStatsObjectName(stats, loc.Name())
		objectio.SetObjectStatsExtent(stats, loc.Extent())
		objectio.SetObjectStatsBlkCnt(stats, dataMeta.BlockCount())

		sortKeyIdx := dataMeta.BlockHeader().SortKey()
		objectio.SetObjectStatsSortKeyZoneMap(stats, dataMeta.MustGetColumn(sortKeyIdx).ZoneMap())

		totalRows := uint32(0)
		for idx := 0; idx < bat.Vecs[0].Length(); idx++ {
			totalRows += dataMeta.GetBlockMeta(uint32(idx)).GetRows()
		}

		objectio.SetObjectStatsRowCnt(stats, totalRows)
		statsBytes = stats.Marshal()

	} else { // bat contains object stats vec
		// x ns/op
		statsBytes = bat.Vecs[2].GetBytesAt(0)
	}

	// append object stats to the container, may exist multiple partitions (tbl id)
	for _, tarBat := range arg.container.mp {
		vector.SetConstBytes(tarBat.Vecs[1], statsBytes, 1, proc.GetMPool())
	}
	return nil
}

func (arg *Argument) Split(proc *process.Process, bat *batch.Batch) error {
	// meta loc and object stats
	arg.GetMetaLocBat(bat, proc)
	tblIdx := vector.MustFixedCol[int16](bat.GetVector(0))
	blockInfos := vector.MustBytesCol(bat.GetVector(1))

	onlyData := true
	for i := range tblIdx { // append s3 writer returned blk info
		if tblIdx[i] >= 0 {
			blkInfo := catalog.DecodeBlockInfo(blockInfos[i])
			arg.affectedRows += uint64(blkInfo.MetaLocation().Rows())
			vector.AppendBytes(arg.container.mp[int(tblIdx[i])].Vecs[0],
				blockInfos[i], false, proc.GetMPool())
			onlyData = false
		} else { // append data
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

	// exist s3 returned blk info, split it
	if !onlyData {
		if err := splitObjectStats(arg, proc, bat, blockInfos[0]); err != nil {
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
