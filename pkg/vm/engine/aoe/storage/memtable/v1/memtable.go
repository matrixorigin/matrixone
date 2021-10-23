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

package memtable

import (
	"fmt"
	gBatch "matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
)

type memTable struct {
	sync.RWMutex
	common.RefHelper

	// opts is the options of aoe
	opts *storage.Options

	// TableData is Table's metadata in memory
	tableData iface.ITableData

	// full represents whether the current memTable is full
	full bool

	// ibat is all batch data of the current Block
	ibat batch.IBatch

	// BlkMeta is the metadata of the Block, which is
	// created and registered during NewCreateBlkEvent
	meta *metadata.Block

	// tableMeta is the metadata of the table, which is
	// created and registered during NewCreateTableEvent
	tableMeta *metadata.Table

	// iblk is an instance registered to segment, which is
	// created and registered during NewCreateSegBlkEvent
	iblk iface.IBlock
}

var (
	_ imem.IMemTable = (*memTable)(nil)
)

func NewMemTable(opts *storage.Options, tableData iface.ITableData, data iface.IBlock) imem.IMemTable {

	mt := &memTable{
		opts:      opts,
		tableData: tableData,
		ibat:      data.GetFullBatch(),
		iblk:      data,
		meta:      data.GetMeta(),
		tableMeta: tableData.GetMeta(),
	}

	for idx, colIdx := range mt.ibat.GetAttrs() {
		vec, err := mt.ibat.GetVectorByAttr(colIdx)
		if err != nil {
			// TODO: returns error
			panic(err)
		}
		vec.PlacementNew(mt.meta.Segment.Table.Schema.ColDefs[idx].Type)
	}

	mt.OnZeroCB = mt.close
	mt.Ref()
	return mt
}

func (mt *memTable) GetID() common.ID {
	return mt.meta.AsCommonID().AsBlockID()
}

func (mt *memTable) String() string {
	mt.RLock()
	defer mt.RUnlock()
	id := mt.GetID()
	bat := mt.ibat
	length := -1
	if bat != nil {
		length = bat.Length()
	}
	s := fmt.Sprintf("<MT[%s]>(RefCount=%d)(Count=%d)", id.BlockString(), mt.RefCount(), length)
	return s
}

func (mt *memTable) Append(bat *gBatch.Batch, offset uint64, index *metadata.LogIndex) (n uint64, err error) {
	mt.Lock()
	defer mt.Unlock()
	var na int
	for idx, attr := range mt.ibat.GetAttrs() {
		for i, a := range bat.Attrs {
			if a == mt.tableMeta.Schema.ColDefs[idx].Name {
				vec, err := mt.ibat.GetVectorByAttr(attr)
				if err != nil {
					return 0, err
				}
				if na, err = vec.AppendVector(bat.Vecs[i], int(offset)); err != nil {
					return n, err
				}
			}
		}
	}
	n = uint64(na)
	index.Count = n
	if err = mt.meta.CommitInfo.SetIndex(*index); err != nil {
		return 0, err
	}
	// log.Infof("1. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.meta.GetCount())
	if _, err = mt.meta.AddCount(n); err != nil {
		return 0, err
	}
	mt.tableData.AddRows(n)
	// log.Infof("2. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.meta.GetCount())
	if uint64(mt.ibat.Length()) == mt.meta.Segment.Table.Schema.BlockMaxRows {
		mt.full = true
	}
	mt.meta.AppendIndex(index)
	return n, err
}

// A flush worker call this Flush API. When a memTable is ready to flush. It immutable.
// Steps:
// 1. Serialize mt.DataSource to block_file (dir/$table_id_$segment_id_$block_id.blk)
// 2. Create a UpdateBlockOp and excute it
// 3. Start a checkpoint job
// If crashed before Step 1, all data from last checkpoint will be restored from WAL
// If crashed before Step 2, the untracked block file will be cleanup at startup.
// If crashed before Step 3, same as above.
func (mt *memTable) Flush() error {
	mt.opts.EventListener.FlushBlockBeginCB(mt)
	writer := &memTableWriter{
		memTable: mt,
	}
	err := writer.Flush()
	if err != nil {
		mt.opts.EventListener.BackgroundErrorCB(err)
		return err
	}
	mt.opts.EventListener.FlushBlockEndCB(mt)
	// err = mt.scheduleEvents()
	return err
}

func (mt *memTable) GetMeta() *metadata.Block {
	return mt.meta
}

func (mt *memTable) Unpin() {
	if mt.ibat != nil {
		mt.ibat.Close()
		mt.ibat = nil
	}
}

func (mt *memTable) close() {
	if mt.ibat != nil {
		mt.ibat.Close()
		mt.ibat = nil
	}
	if mt.iblk != nil {
		mt.iblk.Unref()
		mt.iblk = nil
	}
	if mt.tableData != nil {
		mt.tableData.Unref()
		mt.tableData = nil
	}
}

func (mt *memTable) IsFull() bool {
	return mt.full
}
