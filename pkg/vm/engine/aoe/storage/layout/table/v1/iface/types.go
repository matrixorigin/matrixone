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

package iface

import (
	"bytes"
	"io"
	"matrixone/pkg/container/vector"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	svec "matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type ITableData interface {
	common.IRef
	GetID() uint64
	GetName() string
	GetBlockFactory() IBlockFactory
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetSegmentCount() uint32
	GetSegmentedIndex() (uint64, bool)
	GetIndexHolder() *index.TableHolder
	InitReplay()
	RegisterSegment(meta *md.Segment) (seg ISegment, err error)
	RegisterBlock(meta *md.Block) (blk IBlock, err error)
	StrongRefSegment(id uint64) ISegment
	WeakRefSegment(id uint64) ISegment
	StrongRefBlock(segId, blkId uint64) IBlock
	WeakRefBlock(segId, blkId uint64) IBlock
	String() string
	UpgradeSegment(id uint64) (ISegment, error)
	UpgradeBlock(*md.Block) (IBlock, error)
	SegmentIds() []uint64
	StongRefRoot() ISegment
	WeakRefRoot() ISegment
	GetRowCount() uint64
	AddRows(uint64) uint64
	GetMeta() *md.Table
	Size(string) uint64
	StrongRefLastBlock() IBlock
}

type ISegment interface {
	common.IRef
	CanUpgrade() bool
	GetReplayIndex() *md.LogIndex
	GetMeta() *md.Segment
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetIndexHolder() *index.SegmentHolder
	GetSegmentFile() base.ISegmentFile
	GetSegmentedIndex() (uint64, bool)
	GetType() base.SegmentType
	RegisterBlock(*md.Block) (blk IBlock, err error)
	StrongRefBlock(id uint64) IBlock
	WeakRefBlock(id uint64) IBlock
	GetNext() ISegment
	SetNext(ISegment)
	String() string
	GetRowCount() uint64
	Size(string) uint64
	CloneWithUpgrade(ITableData, *md.Segment) (ISegment, error)
	UpgradeBlock(*md.Block) (IBlock, error)
	BlockIds() []uint64
	StrongRefLastBlock() IBlock
}

type IBlock interface {
	common.MVCC
	common.IRef
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetIndexHolder() *index.BlockHolder
	GetSegmentedIndex() (uint64, bool)
	GetMeta() *md.Block
	GetType() base.BlockType
	CloneWithUpgrade(ISegment, *md.Block) (IBlock, error)
	GetSegmentFile() base.ISegmentFile
	String() string
	GetFullBatch() batch.IBatch
	GetBatch(attrs []int) dbi.IBatchReader
	GetVectorWrapper(col int) (*svec.VectorWrapper, error)
	GetVectorCopy(attr string, compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*vector.Vector, error)
	Prefetch(attr string) error
	WeakRefSegment() ISegment
	GetRowCount() uint64
	GetNext() IBlock
	SetNext(next IBlock)
	Size(string) uint64
}

type IBlockFactory interface {
	CreateBlock(ISegment, *metadata.Block) (IBlock, error)
}

type IMutBlock interface {
	IBlock
	WithPinedContext(func(mb.IMutableBlock) error) error
	MakeHandle() bb.INodeHandle
}

type IColBlockHandle interface {
	io.Closer
	GetPageNode(int) bmgrif.MangaedNode
}
