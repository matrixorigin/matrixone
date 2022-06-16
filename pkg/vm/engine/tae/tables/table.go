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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

type dataTable struct {
	meta        *catalog.TableEntry
	fileFactory file.SegmentFactory
	bufMgr      base.INodeManager
	aBlk        *dataBlock
}

func newTable(meta *catalog.TableEntry, fileFactory file.SegmentFactory, bufMgr base.INodeManager) *dataTable {
	return &dataTable{
		meta:        meta,
		fileFactory: fileFactory,
		bufMgr:      bufMgr,
	}
}

func (table *dataTable) GetHandle() data.TableHandle {
	return newHandle(table, table.aBlk)
}

func (table *dataTable) ApplyHandle(h data.TableHandle) {
	handle := h.(*tableHandle)
	table.aBlk = handle.block
}
