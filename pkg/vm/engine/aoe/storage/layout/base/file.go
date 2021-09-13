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
	Offset    int64
	Len       uint64
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

type IManager interface {
	RegisterSortedFiles(common.ID) (ISegmentFile, error)
	RegisterUnsortedFiles(common.ID) (ISegmentFile, error)
	UnregisterUnsortedFile(common.ID)
	UnregisterSortedFile(common.ID)
	UpgradeFile(common.ID) ISegmentFile
	GetSortedFile(common.ID) ISegmentFile
	GetUnsortedFile(common.ID) ISegmentFile
	String() string
}

type IBaseFile interface {
	io.Closer
	common.IRef
	GetIndicesMeta() *IndicesMeta
	ReadPoint(ptr *Pointer, buf []byte)
	ReadPart(colIdx uint64, id common.ID, buf []byte)
	PrefetchPart(colIdx uint64, id common.ID) error
	PartSize(colIdx uint64, id common.ID, isOrigin bool) int64
	DataCompressAlgo(common.ID) int
	Stat() common.FileInfo
	MakeVirtualIndexFile(*IndexMeta) common.IVFile
	GetDir() string
}

type ISegmentFile interface {
	IBaseFile
	RefBlock(blkId common.ID)
	UnrefBlock(blkId common.ID)
	ReadBlockPoint(id common.ID, ptr *Pointer, buf []byte)
	GetBlockIndicesMeta(id common.ID) *IndicesMeta

	MakeVirtualBlkIndexFile(id *common.ID, meta *IndexMeta) common.IVFile
	MakeVirtualPartFile(id *common.ID) common.IVFile
}

type IBlockFile interface {
	IBaseFile
}
