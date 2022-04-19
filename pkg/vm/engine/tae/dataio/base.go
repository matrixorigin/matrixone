package dataio

import (
	"github.com/RoaringBitmap/roaring"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
)

type NoopSegmentFile struct {
	common.RefHelper
}

func (sf *NoopSegmentFile) Close() (err error)   { return }
func (sf *NoopSegmentFile) IsSorted() bool       { return false }
func (sf *NoopSegmentFile) Destory() (err error) { return }

func (sf *NoopSegmentFile) GetBlockFile(uint64) (bf BlockFile)                      { return }
func (sf *NoopSegmentFile) LoadBlock(uint64) (data batch.IBatch, err error)         { return }
func (sf *NoopSegmentFile) GetBlockMaxIndex(uint64) (index *shard.Index)            { return }
func (sf *NoopSegmentFile) LoadBlockTimeStamps(uint64) (ts *gvec.Vector, err error) { return }
func (sf *NoopSegmentFile) WriteBlock(uint64, batch.IBatch, uint64, map[uint16]*roaring.Bitmap, map[uint16]map[uint32]interface{}, *roaring.Bitmap) (err error) {
	return
}

type NoopBlockFile struct {
	common.RefHelper
}

func (bf *NoopBlockFile) Close() (err error)   { return }
func (bf *NoopBlockFile) Destory() (err error) { return }

func (bf *NoopBlockFile) IsSorted() bool { return false }
func (bf *NoopBlockFile) Rows() uint32   { return 0 }

func (bf *NoopBlockFile) GetSegmentFile() (sf SegmentFile) { return }
func (bf *NoopBlockFile) WriteData(batch.IBatch, uint64, map[uint16]*roaring.Bitmap, map[uint16]map[uint32]interface{}, *roaring.Bitmap) (err error) {
	return
}
func (bf *NoopBlockFile) LoadData() (bat batch.IBatch, err error) { return }
func (bf *NoopBlockFile) Sync() (err error)                       { return }
func (bf *NoopBlockFile) GetMaxVisble() uint64                    { return 0 }

func (bf *NoopBlockFile) GetColumnStat(idx uint16) (info common.FileInfo) { return }
