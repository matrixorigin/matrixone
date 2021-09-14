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

package base

import (
	"fmt"
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"

	"github.com/RoaringBitmap/roaring"
)

type Pointer struct {
	// Offset type is int64 because the parameter of ReadAt is int64
	Offset int64

	// Len is the length of Column storage is generally compressed
	Len uint64

	// OriginLen is the original length of Column and has not been compressed
	OriginLen uint64
}

type IndicesMeta struct {
	Data []*IndexMeta
}

func (m *IndicesMeta) String() string {
	s := fmt.Sprintf("<IndicesMeta>[Cnt=%d]", len(m.Data))
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

func NewIndicesMeta() *IndicesMeta {
	return &IndicesMeta{
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

// IManager is a segment file manager that maintains SortedFiles[]&UnsortedFiles[]
// and can create a physical segment file
type IManager interface {
	// RegisterSortedFiles is add physical segment file(SORTED_SEG)
	// to Manager.SortedFiles[]
	RegisterSortedFiles(common.ID) (ISegmentFile, error)

	// RegisterUnsortedFiles is add logical segment file(UNSORTED_SEG)
	// to Manager.UnsortedFiles[]
	RegisterUnsortedFiles(common.ID) (ISegmentFile, error)

	// UnregisterUnsortedFile is delete a logical segment file
	// from Manager.UnsortedFiles[]
	UnregisterUnsortedFile(common.ID)

	// UnregisterSortedFile is delete a physical segment file
	// from Manager.SortedFiles[]
	UnregisterSortedFile(common.ID)

	// UpgradeFile is create a physical segment file based on common.ID and
	// delete it from Manager.UnsortedFiles[] and add it to Manager.SortedFiles[]
	UpgradeFile(common.ID) ISegmentFile

	GetSortedFile(common.ID) ISegmentFile
	GetUnsortedFile(common.ID) ISegmentFile

	// String is to Sprintf every item of Manager.SortedFiles[]&Manager.UnsortedFiles[]
	String() string
}

// IBaseFile is block&segment file interface, cannot provide external services,
// need New a ColumnPart provide external services.
// When New a block&segment object, initPointers is called to initialize
// the Pointers of CloumnPart
type IBaseFile interface {
	io.Closer
	common.IRef
	GetIndicesMeta() *IndicesMeta

	// ReadPoint is reads Pointer to buf, and detects Pointer.Len
	// in aoe, the Pointer is Column
	ReadPoint(ptr *Pointer, buf []byte)

	// ReadPart is to use part[colIdx] to find Pointer and then call ReadPoint
	// the external Read interface is implemented through ReadPart
	ReadPart(colIdx uint64, id common.ID, buf []byte)

	// PrefetchPart is prefetch a Pointer on a file using system calls
	PrefetchPart(colIdx uint64, id common.ID) error

	// PartSize is return a Pointer Len ro OriginLen
	PartSize(colIdx uint64, id common.ID, isOrigin bool) int64

	// DataCompressAlgo is return the compress type of the BaseFIle
	DataCompressAlgo(common.ID) int

	// Stat is retrun FileInfo of the BaseFile
	// initialize at the time of new(BaseFIle)
	Stat() common.FileInfo
	MakeVirtualIndexFile(*IndexMeta) common.IVFile
	GetDir() string
}

type ISegmentFile interface {
	IBaseFile

	// RefBlock is ref SegmentFile, UnsortedSegmentFile adds
	// a .blk file to UnsortedSegmentFile.Blocks[]
	RefBlock(blkId common.ID)

	// UnrefBlock is unref SegmentFile, UnsortedSegmentFile is unref all block file.
	// TODO: unref block file of the blkId
	UnrefBlock(blkId common.ID)

	// ReadBlockPoint is reads Pointer to buf. Index will be use.
	ReadBlockPoint(id common.ID, ptr *Pointer, buf []byte)
	GetBlockIndicesMeta(id common.ID) *IndicesMeta

	MakeVirtualBlkIndexFile(id *common.ID, meta *IndexMeta) common.IVFile

	// MakeVirtualPartFile is New a ColumnPart, ColumnPart provides external Read services
	MakeVirtualPartFile(id *common.ID) common.IVFile
}

type IBlockFile interface {
	IBaseFile
}
