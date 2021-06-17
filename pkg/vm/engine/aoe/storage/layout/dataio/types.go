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

type IBaseFile interface {
	io.Closer
	GetIndexMeta() *base.IndexesMeta
	ReadPoint(ptr *base.Pointer, buf []byte)
	ReadPart(colIdx uint64, id common.ID, buf []byte)
	Destory()
}

type ISegmentFile interface {
	IBaseFile
	Ref()
	Unref()
	RefBlock(blkId common.ID)
	UnrefBlock(blkId common.ID)
	ReadBlockPoint(id common.ID, ptr *base.Pointer, buf []byte)
	GetBlockIndexMeta(id common.ID) *base.IndexesMeta

	// MakeVirtualBlkIndexFile(id *common.ID) base.IVirtaulFile
	MakeVirtualSegmentIndexFile(*base.IndexMeta) base.IVirtaulFile
	MakeVirtualPartFile(id *common.ID) base.IVirtaulFile
}

type IBlockFile interface {
	IBaseFile
}
