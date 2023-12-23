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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type tableHandle struct {
	table    *dataTable
	block    *ablock
	appender data.BlockAppender
}

func newHandle(table *dataTable, block *ablock) *tableHandle {
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
	objMeta, _ := tableMeta.GetObjectByID(id.ObjectID())
	blkMeta, _ := objMeta.GetBlockEntryByID(&id.BlockID)
	h.block = blkMeta.GetBlockData().(*ablock)
	h.appender, _ = h.block.MakeAppender()
	h.block.Ref()
	return h.appender
}

func (h *tableHandle) ThrowAppenderAndErr() (appender data.BlockAppender, err error) {
	err = data.ErrAppendableObjectNotFound
	h.block = nil
	h.appender = nil
	return
}

func (h *tableHandle) GetAppender() (appender data.BlockAppender, err error) {
	var objEntry *catalog.ObjectEntry
	if h.appender == nil {
		objEntry = h.table.meta.LastAppendableObject()
		if objEntry == nil {
			err = data.ErrAppendableObjectNotFound
			return
		}
		blkEntry := objEntry.LastAppendableBlock()
		if blkEntry == nil {
			err = data.ErrAppendableObjectNotFound
			return
		}
		h.block = blkEntry.GetBlockData().(*ablock)
		h.appender, err = h.block.MakeAppender()
		if err != nil {
			panic(err)
		}
	}

	// Instead in ThrowAppenderAndErr, check object index here because
	// it is better to create new appendable early in some busy update workload case
	if obj := h.block.meta.GetObject(); obj.GetNextObjectIndex() >= options.DefaultObjectPerSegment {
		logutil.Infof("%s create new obj due to large object index %d",
			obj.ID.String(), obj.GetNextObjectIndex())
		return nil, data.ErrAppendableObjectNotFound
	}

	dropped := h.block.meta.HasDropCommitted()
	if !h.appender.IsAppendable() || !h.block.IsAppendable() || dropped {
		return h.ThrowAppenderAndErr()
	}
	h.block.Ref()
	// Similar to optimistic locking
	dropped = h.block.meta.HasDropCommitted()
	if !h.appender.IsAppendable() || !h.block.IsAppendable() || dropped {
		h.block.Unref()
		return h.ThrowAppenderAndErr()
	}
	appender = h.appender
	return
}
