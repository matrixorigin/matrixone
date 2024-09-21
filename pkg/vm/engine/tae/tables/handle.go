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
	table       *dataTable
	object      *aobject
	appender    data.ObjectAppender
	isTombstone bool
}

func newHandle(table *dataTable, object *aobject, isTombstone bool) *tableHandle {
	h := &tableHandle{
		table:       table,
		object:      object,
		isTombstone: isTombstone,
	}
	if object != nil {
		h.appender, _ = object.MakeAppender()
	}
	return h
}

func (h *tableHandle) SetAppender(id *common.ID) (appender data.ObjectAppender) {
	tableMeta := h.table.meta
	objMeta, _ := tableMeta.GetObjectByID(id.ObjectID(), h.isTombstone)
	h.object = objMeta.GetObjectData().(*aobject)
	h.appender, _ = h.object.MakeAppender()
	h.object.Ref()
	return h.appender
}

func (h *tableHandle) ThrowAppenderAndErr() (appender data.ObjectAppender, err error) {
	err = data.ErrAppendableObjectNotFound
	h.object = nil
	h.appender = nil
	return
}

func (h *tableHandle) GetAppender() (appender data.ObjectAppender, err error) {
	var objEntry *catalog.ObjectEntry
	if h.appender == nil {
		objEntry = h.table.meta.LastAppendableObject(h.isTombstone)
		if objEntry == nil {
			err = data.ErrAppendableObjectNotFound
			return
		}
		h.object = objEntry.GetObjectData().(*aobject)
		h.appender, err = h.object.MakeAppender()
		if err != nil {
			panic(err)
		}
	}

	dropped := h.object.meta.Load().HasDropCommitted()
	if !h.appender.IsAppendable() || !h.object.IsAppendable() || dropped {
		return h.ThrowAppenderAndErr()
	}
	h.object.Ref()
	// Similar to optimistic locking
	dropped = h.object.meta.Load().HasDropCommitted()
	if !h.appender.IsAppendable() || !h.object.IsAppendable() || dropped {
		h.object.Unref()
		return h.ThrowAppenderAndErr()
	}
	appender = h.appender
	return
}
