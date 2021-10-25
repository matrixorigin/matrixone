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

package factories

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type mutNodeFactory struct {
	host  *mutFactory
	tdata iface.ITableData
}

func newMutNodeFactory(host *mutFactory, tdata iface.ITableData) *mutNodeFactory {
	f := &mutNodeFactory{
		tdata: tdata,
		host:  host,
	}
	return f
}

func (f *mutNodeFactory) GetManager() bb.INodeManager {
	return f.host.mgr
}

func (f *mutNodeFactory) CreateNode(segfile base.ISegmentFile, meta *metadata.Block, mockSize *mb.MockSize) bb.INode {
	blkfile := dataio.NewTBlockFile(segfile, *meta.AsCommonID())
	nodeSize := uint64(0)
	if mockSize != nil {
		nodeSize = mockSize.Size()
	} else {
		nodeSize = metadata.EstimateBlockSize(meta)
	}
	n := mutation.NewMutableBlockNode(f.host.mgr, blkfile, f.tdata, meta, f.host.flusher, nodeSize)
	f.host.mgr.RegisterNode(n)
	return n
}
