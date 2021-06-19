package base

import (
	"fmt"
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"

	"github.com/pilosa/pilosa/roaring"
)

type IVirtaulFile interface {
	io.Reader
	// io.Writer
	Ref()
	Unref()
}

type Pointer struct {
	Offset int64
	Len    uint64
}

type IndexesMeta struct {
	Data []*IndexMeta
}

func (m *IndexesMeta) String() string {
	s := fmt.Sprintf("<IndexesMeta>[Cnt=%d]", len(m.Data))
	for _, meta := range m.Data {
		s = fmt.Sprintf("%s\n\t%s", s, meta.String())
	}
	return s
}

type IndexMeta struct {
	Type IndexType
	Cols *roaring.Bitmap
	Ptr  *Pointer
}

func NewIndexesMeta() *IndexesMeta {
	return &IndexesMeta{
		Data: make([]*IndexMeta, 0),
	}
}

func (m *IndexMeta) String() string {
	s := fmt.Sprintf("<IndexMeta>[Ty=%d](Off: %d, Len:%d)", m.Type, m.Ptr.Offset, m.Ptr.Len)
	return s
}

type Key struct {
	Col uint64
	ID  common.ID
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
	GetIndexMeta() *IndexesMeta
	ReadPoint(ptr *Pointer, buf []byte)
	ReadPart(colIdx uint64, id common.ID, buf []byte)
	Destory()
}

type ISegmentFile interface {
	IBaseFile
	Ref()
	Unref()
	RefBlock(blkId common.ID)
	UnrefBlock(blkId common.ID)
	ReadBlockPoint(id common.ID, ptr *Pointer, buf []byte)
	GetBlockIndexMeta(id common.ID) *IndexesMeta

	// MakeVirtualBlkIndexFile(id *common.ID) base.IVirtaulFile
	MakeVirtualSegmentIndexFile(*IndexMeta) IVirtaulFile
	MakeVirtualPartFile(id *common.ID) IVirtaulFile
}

type IBlockFile interface {
	IBaseFile
}
