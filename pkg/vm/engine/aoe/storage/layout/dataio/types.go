package dataio

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

type Key struct {
	Col uint64
	ID  common.ID
	// BlockID uint64
	// PartID  uint32
}

type IManager interface {
	RegisterSortedFiles(common.ID) (ISegmentFile, error)
	RegisterUnsortedFiles(common.ID) (ISegmentFile, error)
	UpgradeFile(common.ID) ISegmentFile
	GetSortedFile(common.ID) ISegmentFile
	GetUnsortedFile(common.ID) ISegmentFile
	String() string
}

type IHostFile interface {
	io.Closer
	ReadPoint(ptr *base.Pointer, buf []byte)
	ReadPart(colIdx uint64, id common.ID, buf []byte)
	Destory()
}

type ISegmentFile interface {
	IHostFile
	RefBlock(blkId common.ID)
	UnrefBlock(blkId common.ID)
	MakeColPartFile(id *common.ID) IColPartFile
	ReadBlockPoint(id common.ID, ptr *base.Pointer, buf []byte)

	// MakeVirtualBlkIndexFile(id *common.ID) base.IVirtaulFile
	// MakeVirtualSegFile(id *common.ID) base.IVirtaulFile
	MakeVirtualPartFile(id *common.ID) base.IVirtaulFile
}

type IBlockFile interface {
	IHostFile
}

type IColSegmentFile interface {
	ReadPart(id common.ID, buf []byte)
}

type IColPartFile interface {
	io.Reader
}

type IIndexFile interface {
	io.Reader
}
