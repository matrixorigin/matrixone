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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"io"
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
	// RegisterSortedFiles adds physical segment file(SORTED_SEG)
	// to Manager.SortedFiles[]
	//RegisterSortedFiles(common.ID) (ISegmentFile, error)

	// RegisterUnsortedFiles adds logical segment file(UNSORTED_SEG)
	// to Manager.UnsortedFiles[]
	//RegisterUnsortedFiles(common.ID) (ISegmentFile, error)

	RegisterSortedFile(id common.ID) (ISegmentFile, error)
	RegisterUnsortedFile(id common.ID) (ISegmentFile, error)

	// UnregisterUnsortedFile deletes a logical segment file
	// from Manager.UnsortedFiles[]
	UnregisterUnsortedFile(common.ID)

	// UnregisterSortedFile deletes a physical segment file
	// from Manager.SortedFiles[]
	UnregisterSortedFile(common.ID)

	// UpgradeFile creates a physical segment file based on common.ID and
	// delete it from Manager.UnsortedFiles[] and add it to Manager.SortedFiles[]
	UpgradeFile(id common.ID) ISegmentFile

	GetSortedFile(common.ID) ISegmentFile
	GetUnsortedFile(common.ID) ISegmentFile

	// String print every item of Manager.SortedFiles[]&Manager.UnsortedFiles[]
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

	// ReadPoint reads Part to buf, and detects Pointer.Len
	// in aoe, the Pointer is Column
	ReadPoint(ptr *Pointer, buf []byte)

	// ReadPart is to use part[colIdx] to find Pointer and then call ReadPoint
	// the external Read interface is implemented through ReadPart
	ReadPart(colIdx uint64, id common.ID, buf []byte)

	// PrefetchPart calls readahead in Linux
	PrefetchPart(colIdx uint64, id common.ID) error

	// PartSize returns a Pointer Len or OriginLen
	PartSize(colIdx uint64, id common.ID, isOrigin bool) int64

	// DataCompressAlgo returns the compress type of the BaseFIle
	DataCompressAlgo(common.ID) int

	// Stat retruns FileInfo of the BaseFile
	// initialize at the time of new(BaseFIle)
	Stat() common.FileInfo
	MakeVirtualIndexFile(*IndexMeta) common.IVFile
	GetDir() string
}

type ISegmentFile interface {
	IBaseFile

	// RefBlock acquires a reference to the underlying block.
	RefBlock(blkId common.ID)

	// UnrefBlock releases the reference to the underlying block
	UnrefBlock(blkId common.ID)

	GetBlockSize(blkId common.ID) int64

	// ReadBlockPoint reads a Pointer data to buf,
	// which called by EmbedBlockIndexFile.
	ReadBlockPoint(id common.ID, ptr *Pointer, buf []byte)
	GetBlockIndicesMeta(id common.ID) *IndicesMeta

	MakeVirtualBlkIndexFile(id *common.ID, meta *IndexMeta) common.IVFile

	// MakeVirtualPartFile creates a new column part. ColumnPart provides external Read services
	MakeVirtualPartFile(id *common.ID) common.IVFile
}

type IBlockFile interface {
	IBaseFile
}
