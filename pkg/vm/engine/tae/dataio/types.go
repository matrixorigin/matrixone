package dataio

import (
	"io"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
)

type SegmentFileFactory = func(dir string, id uint64) SegmentFile

type SegmentFile interface {
	common.IRef
	io.Closer
	Destory() error
	WriteBlock(id uint64, data batch.IBatch, maxIndex *shard.Index, ts *gvec.Vector) error
	LoadBlock(id uint64) (batch.IBatch, error)
	GetBlockMaxIndex(id uint64) *shard.Index
	LoadBlockTimeStamps(id uint64) (*gvec.Vector, error)
	GetBlockFile(id uint64) BlockFile
	IsSorted() bool
	// MakeColumnBlockFile(id uint64) common.IVFile
	// GetColumnBlockStat(id uint64, idx uint16) common.FileInfo
	// UpdateColumnBlock(id uint64, idx uint16, col vector.IVector, logIndex shard.Index) error
	// LoadBlockData(id uint64) (batch.IBatch, error)
	// LoadColumnBlockData(id uint64, idx uint16) (vector.IVector, error)
	// RemoveBlock(id uint64) error
}

type BlockFile interface {
	common.IRef
	io.Closer
	Destory() error
	Rows() uint32
	GetSegmentFile() SegmentFile
	WriteData(batch.IBatch, *shard.Index, *gvec.Vector) error
	LoadData() (batch.IBatch, error)
	Sync() error
	GetMaxIndex() *shard.Index
	GetTimeStamps() (*gvec.Vector, error)
	// GetColumnFile(idx uint16) common.IVFile
	// IsSorted() bool
	// GetColumnStat(idx uint16) common.FileInfo
}

type BlockInfo interface {
	Rows() uint32
	TSRange() (uint64, uint64)
}
