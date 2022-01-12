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

package table

import (
	bmgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type sllnode = common.SLLNode

type baseBlock struct {
	sllnode
	meta        *metadata.Block
	host        iface.ISegment
	typ         base.BlockType
	indexholder *index.BlockIndexHolder
}

func newBaseBlock(host iface.ISegment, meta *metadata.Block) *baseBlock {
	blk := &baseBlock{
		host:    host,
		meta:    meta,
		sllnode: *common.NewSLLNode(nil),
	}
	if meta.CommitInfo.Op < metadata.OpUpgradeFull {
		blk.typ = base.TRANSIENT_BLK
	} else if host.GetType() == base.UNSORTED_SEG {
		blk.typ = base.PERSISTENT_BLK
		blk.indexholder = host.GetIndexHolder().RegisterBlock(meta.AsCommonID().AsBlockID(), base.PERSISTENT_BLK, nil)
	} else {
		blk.typ = base.PERSISTENT_SORTED_BLK
	}
	return blk
}

func (blk *baseBlock) GetMeta() *metadata.Block { return blk.meta }
func (blk *baseBlock) GetType() base.BlockType  { return blk.typ }
func (blk *baseBlock) GetRowCount() uint64      { return blk.meta.GetCountLocked() }
func (blk *baseBlock) IsMutable() bool {
	if blk.typ >= base.PERSISTENT_BLK {
		return false
	}
	return true
}

func (blk *baseBlock) WeakRefSegment() iface.ISegment      { return blk.host }
func (blk *baseBlock) GetMTBufMgr() bmgrif.IBufferManager  { return blk.host.GetMTBufMgr() }
func (blk *baseBlock) GetSSTBufMgr() bmgrif.IBufferManager { return blk.host.GetSSTBufMgr() }
func (blk *baseBlock) GetFsManager() base.IManager         { return blk.host.GetFsManager() }
func (blk *baseBlock) GetSegmentFile() base.ISegmentFile   { return blk.host.GetSegmentFile() }

func (blk *baseBlock) GetIndexHolder() *index.BlockIndexHolder {
	//if blk.indexholder != nil {
	//	blk.indexholder.Ref()
	//}
	return blk.indexholder
}

func (blk *baseBlock) SetNext(next iface.IBlock) {
	blk.sllnode.SetNextNode(next)
}

func (blk *baseBlock) GetNext() iface.IBlock {
	next := blk.sllnode.GetNextNode()
	if next == nil {
		return nil
	}
	return next.(iface.IBlock)
}

func (blk *baseBlock) release() {
	if blk.indexholder != nil {
		blk.indexholder.Unref()
	}
	blk.sllnode.ReleaseNextNode()
}

func (blk *baseBlock) preUpgradeAssert() {
	if blk.typ == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if blk.meta.CommitInfo.Op != metadata.OpUpgradeFull {
		panic("blk not upgraded")
	}
}

func (blk *baseBlock) upgrade(host iface.ISegment, meta *metadata.Block) (*baseBlock, error) {
	blk.preUpgradeAssert()

	id := meta.AsCommonID().AsBlockID()
	upgraded := &baseBlock{
		meta:    meta,
		host:    host,
		sllnode: *common.NewSLLNode(nil),
	}

	switch blk.typ {
	case base.TRANSIENT_BLK:
		upgraded.typ = base.PERSISTENT_BLK
		if host.GetIndexHolder().HolderType() == base.UNSORTED_SEG {
			upgraded.indexholder = host.GetIndexHolder().RegisterBlock(id, upgraded.typ, nil)
		}
	case base.PERSISTENT_BLK:
		upgraded.typ = base.PERSISTENT_SORTED_BLK
	default:
		panic("logic error")
	}

	return upgraded, nil
}
