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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type tableHandle struct {
	table    *dataTable
	block    *dataBlock
	appender data.BlockAppender
}

func newHandle(table *dataTable, block *dataBlock) *tableHandle {
	h := &tableHandle{
		table: table,
		block: block,
	}
	if block != nil {
		h.appender, _ = block.MakeAppender()
	}
	return h
}

func (h *tableHandle) SetAppender(id *common.ID) (appender data.BlockAppender) {
	tableMeta := h.table.meta
	segMeta, _ := tableMeta.GetSegmentByID(id.SegmentID)
	blkMeta, _ := segMeta.GetBlockEntryByID(id.BlockID)
	h.block = blkMeta.GetBlockData().(*dataBlock)
	h.appender, _ = h.block.MakeAppender()

	return h.appender
}

func (h *tableHandle) GetAppender() (appender data.BlockAppender, err error) {
	var segEntry *catalog.SegmentEntry
	if h.appender == nil {
		segEntry = h.table.meta.LastAppendableSegmemt()
		if segEntry == nil {
			err = data.ErrAppendableSegmentNotFound
			return
		}
		blkEntry := segEntry.LastAppendableBlock()
		h.block = blkEntry.GetBlockData().(*dataBlock)
		h.appender, err = h.block.MakeAppender()
		if err != nil {
			panic(err)
		}
	}
	if !h.appender.IsAppendable() {
		id := h.appender.GetID()
		segEntry, _ = h.table.meta.GetSegmentByID(id.SegmentID)
		if segEntry.GetAppendableBlockCnt() >= int(segEntry.GetTable().GetSchema().SegmentMaxBlocks) {
			err = data.ErrAppendableSegmentNotFound
		} else {
			err = data.ErrAppendableBlockNotFound

			appender = h.appender
		}
		h.block = nil
		h.appender = nil
		return
	}
	appender = h.appender
	return
}
