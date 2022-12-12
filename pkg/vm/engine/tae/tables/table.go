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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type dataTable struct {
	meta   *catalog.TableEntry
	bufMgr base.INodeManager
	aBlk   *ablock
	//non-appendable segment into which being added non-appendable blocks.
	nseg *dataSegment
}

func newTable(meta *catalog.TableEntry, bufMgr base.INodeManager) *dataTable {
	return &dataTable{
		meta:   meta,
		bufMgr: bufMgr,
	}
}

func (table *dataTable) GetHandle() data.TableHandle {
	return newHandle(table, table.aBlk)
}

func (table *dataTable) ApplyHandle(h data.TableHandle) {
	handle := h.(*tableHandle)
	table.aBlk = handle.block
}

func (table *dataTable) GetLastNonAppendableSeg() (seg *common.ID, err error) {
	if table.nseg == nil {
		segEntry := table.meta.LastNonAppendableSegmemt()
		if segEntry == nil {
			err = data.ErrNonAppendableSegmentNotFound
			return
		}
		table.nseg = segEntry.GetSegmentData().(*dataSegment)
	}
	//check whether segment has dropped by background merging task, although
	// merging non-appendable segments into one is not supported yet now.
	dropped := table.nseg.meta.HasDropCommitted()
	//TODO::if segment is frozen by background merging task?
	if dropped ||
		table.nseg.meta.GetNonAppendableBlockCnt() >=
			int(table.nseg.meta.GetTable().GetSchema().SegmentMaxBlocks) {
		err = data.ErrNonAppendableSegmentNotFound
		return
	}
	//forbidden background task to drop the segment for supporting merging non-appendable segments in the future.
	table.nseg.Ref()
	//double check
	dropped = table.nseg.meta.HasDropCommitted()
	//TODO::if segment is frozen by background merging task?
	if dropped ||
		table.nseg.meta.GetNonAppendableBlockCnt() >=
			int(table.nseg.meta.GetTable().GetSchema().SegmentMaxBlocks) {
		table.nseg.Unref()
		err = data.ErrNonAppendableSegmentNotFound
		return
	}
	return table.nseg.meta.AsCommonID(), nil
}

func (table *dataTable) CloseLastNonAppendableSeg() {
	if table.nseg != nil {
		table.nseg.Unref()
	}
}

func (table *dataTable) SetLastNonAppendableSeg(id *common.ID) {
	tableMeta := table.meta
	segMeta, _ := tableMeta.GetSegmentByID(id.SegmentID)
	table.nseg = segMeta.GetSegmentData().(*dataSegment)
	table.nseg.Ref()
}
