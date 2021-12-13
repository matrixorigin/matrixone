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

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	bmgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	svec "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

type MutationHandle interface {
	io.Closer
	Append(bat *gbat.Batch, index *shard.SliceIndex) (err error)
	Flush() error
	String() string
	GetMeta() *metadata.Table
	RefCount() int64
}

type ITableData interface {
	common.IRef

	// GetID to get the id of the table
	GetID() uint64

	// GetName to get the name of the table
	GetName() string

	// GetBlockFactory to get the factory
	// that produces different types of the Block
	GetBlockFactory() IBlockFactory

	// GetMTBufMgr to get the MTBufMgr of the DB
	GetMTBufMgr() bmgrif.IBufferManager

	// GetSSTBufMgr to get the SSTBufMgr of the DB
	GetSSTBufMgr() bmgrif.IBufferManager

	// GetFsManager to get the FsMgr(file manager) of the DB
	GetFsManager() base.IManager

	// GetSegmentCount to get the segment count of the table
	GetSegmentCount() uint32

	GetIndexHolder() *index.TableHolder

	// init ReplayIndex and rowCount
	InitReplay()
	InitAppender()

	// RegisterSegment creates and registers a logical segment
	RegisterSegment(meta *metadata.Segment) (seg ISegment, err error)

	// RegisterBlock uses GetBlockFactory() to create a
	// Block and append to the blocks of the segment
	RegisterBlock(meta *metadata.Block) (blk IBlock, err error)

	// StrongRefSegment requires UnRef segment
	StrongRefSegment(id uint64) ISegment

	// WeakRefSegment does not require UnRef Segment
	WeakRefSegment(id uint64) ISegment

	// StrongRefBlock requires UnRef Block
	StrongRefBlock(segId, blkId uint64) IBlock

	// WeakRefBlock does not require UnRef Block
	WeakRefBlock(segId, blkId uint64) IBlock
	String() string

	// UpgradeSegment to upgrade the UNSORTED type of segment to SORTED,
	// upgrade various information of metadata in TableData, and it will
	// be called after the new segment file has been flushed.
	UpgradeSegment(id uint64) (ISegment, error)

	// UpgradeBlock upgrade various information of metadata in segment,
	// and it will be called after the new Block file has been flushed.
	UpgradeBlock(*metadata.Block) (IBlock, error)

	// SegmentIds returns the id of all the segments in the TableData
	SegmentIds() []uint64

	// StrongRefRoot requires UnRef root segment
	StongRefRoot() ISegment

	// WeakRefRoot does not require UnRef root segment
	WeakRefRoot() ISegment

	// GetRowCount to get the row count of the table
	GetRowCount() uint64
	AddRows(uint64) uint64

	MakeMutationHandle() MutationHandle

	// GetMeta to get the Table's metadate when the Table is created
	GetMeta() *metadata.Table

	// Size is the size of all segments in TableData
	Size(string) uint64

	// StrongRefLastBlock Ref to the last Block in TableData
	StrongRefLastBlock() IBlock

	CopyTo(dir string) error
	LinkTo(dir string) error
}

type ISegment interface {
	common.IRef

	// Whether it can be CanUpgrade.
	// The type of the segment is UNSORTED_SEG and the type of the Blocki
	// in the blocks is PERSISTENT_BLK to return true
	CanUpgrade() bool

	// GetMeta gets the metadata of the segment
	GetMeta() *metadata.Segment

	// GetMTBufMgr to get the MTBufMgr of the DB
	GetMTBufMgr() bmgrif.IBufferManager

	// GetSSTBufMgr to get the SSTBufMgr of the DB
	GetSSTBufMgr() bmgrif.IBufferManager

	// GetFsManager to get the FsMgr(file manager) of the DB
	GetFsManager() base.IManager
	GetIndexHolder() index.SegmentIndexHolder

	// GetSegmentFile gets the segment file,
	// the newly created segments are all UNSORTED_SEG
	GetSegmentFile() base.ISegmentFile

	// GetType gets the segment type, UNSORTED_SEG or SORTED_SEG
	GetType() base.SegmentType

	// RegisterBlock uses GetBlockFactory() to create a
	// Block and append to the blocks of the segment
	RegisterBlock(*metadata.Block) (blk IBlock, err error)

	// StrongRefBlock requires UnRef Block
	StrongRefBlock(id uint64) IBlock

	// WeakRefSegment does not require UnRef Block
	WeakRefBlock(id uint64) IBlock

	// GetNext gets the next node of the current
	// segment in TableData.tree.segments
	GetNext() ISegment

	// SetNext sets the next node of the current
	// segment in TableData.tree.segments
	SetNext(ISegment)
	String() string

	// GetRowCount to get the row count of the segment
	GetRowCount() uint64
	Size(string) uint64

	// CloneWithUpgrade clones a segment and to upgrade
	// the UNSORTED type of segment to SORTED
	CloneWithUpgrade(ITableData, *metadata.Segment) (ISegment, error)

	// UpgradeBlock upgrade various information of metadata in segment,
	// and it will be called after the new Block file has been flushed.
	UpgradeBlock(*metadata.Block) (IBlock, error)

	// BlockIds returns the id of all the Block in the segment
	BlockIds() []uint64

	//  StrongRefLastBlock Ref to the last Block in segment
	StrongRefLastBlock() IBlock
}

type IBlock interface {
	common.MVCC
	common.IRef

	// GetMTBufMgr to get the MTBufMgr of the DB
	GetMTBufMgr() bmgrif.IBufferManager

	// GetSSTBufMgr to get the SSTBufMgr of the DB
	GetSSTBufMgr() bmgrif.IBufferManager

	// GetFsManager to get the FsMgr(file manager) of the DB
	GetFsManager() base.IManager
	GetIndexHolder() *index.BlockIndexHolder

	// GetMeta to get the metadata of the Block, the metadate is
	// created and registered during NewCreateBlkEvent
	GetMeta() *metadata.Block

	// GetType gets the Block type,
	// TRANSIENT_BLK , PERSISTENT_BLK or PERSISTENT_SORTED_BLK
	GetType() base.BlockType

	// CloneWithUpgrade clones a Block and to upgrade it
	CloneWithUpgrade(ISegment, *metadata.Block) (IBlock, error)

	// GetSegmentFile gets the segment file of the Block
	GetSegmentFile() base.ISegmentFile
	String() string

	// GetFullBatch gets all Batch data of the Block
	GetFullBatch() batch.IBatch

	// GetBatch gets attrs's Batch data of the Block
	GetBatch(attrs []int) dbi.IBatchReader

	// GetVectorWrapper gets col's vector data of the Block
	GetVectorWrapper(col int) (*svec.VectorWrapper, error)
	GetVectorCopy(attr string, compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*vector.Vector, error)
	Prefetch(attr string) error

	// WeakRefSegment does not require UnRef Segment
	WeakRefSegment() ISegment

	// GetRowCount to get the row count of the Block
	GetRowCount() uint64

	// GetNext gets the next node of the current
	// Block in segment.tree.blocks
	GetNext() IBlock

	// SetNext sets the next node of the current
	// Block in segment.tree.blocks
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
