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

package dataio

import (
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

type MockSegmentFile struct {
	common.RefHelper
	FileName string
	FileType FileType
	TypeName string
	Refs     int32
	ID       common.ID
	Info     *fileStat
}

func NewMockSegmentFile(dirname string, ft FileType, id common.ID) base.ISegmentFile {
	msf := new(MockSegmentFile)
	msf.FileType = ft
	msf.ID = id
	if ft == SortedSegFile {
		msf.TypeName = "MockSortedSegmentFile"
	} else if ft == UnsortedSegFile {
		msf.TypeName = "MockUnsortedSegmentFile"
	} else {
		panic("unspported")
	}
	msf.Info = &fileStat{
		name: id.ToSegmentFilePath(),
	}
	msf.FileName = common.MakeSegmentFileName(dirname, id.ToSegmentFileName(), id.TableID, false)
	logutil.Debugf("%s:%s | Created", msf.TypeName, msf.FileName)
	msf.OnZeroCB = msf.close
	return msf
}

func (msf *MockSegmentFile) Stat() common.FileInfo {
	return msf.Info
}

func (bf *MockSegmentFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (msf *MockSegmentFile) GetIndicesMeta() *base.IndicesMeta {
	return nil
}

func (msf *MockSegmentFile) GetBlockIndicesMeta(id common.ID) *base.IndicesMeta {
	return nil
}

func (msf *MockSegmentFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	logutil.Debugf("(%s:%s) | ReadPoint (Off: %d, Len: %d) size: %d cap: %d", msf.TypeName, msf.FileName, ptr.Offset, ptr.Len, len(buf), cap(buf))
}

func (sf *MockSegmentFile) GetBlockSize(_ common.ID) int64 {
	return 0
}

func (msf *MockSegmentFile) ReadBlockPoint(id common.ID, ptr *base.Pointer, buf []byte) {
	logutil.Debugf("(%s:%s) | ReadBlockPoint[%s] (Off: %d, Len: %d) size: %d cap: %d", msf.TypeName, msf.FileName, id.BlockString(), ptr.Offset, ptr.Len, len(buf), cap(buf))
}

func (sf *MockSegmentFile) DataCompressAlgo(id common.ID) int {
	return 0
}

func (sf *MockSegmentFile) PrefetchPart(colIdx uint64, id common.ID) error {
	return nil
}

func (msf *MockSegmentFile) PartSize(colIdx uint64, id common.ID, _ bool) int64 {
	return 0
}

func (msf *MockSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	logutil.Debugf("(%s:%s) | ReadPart %d %s size: %d cap: %d", msf.TypeName, msf.FileName, colIdx, id.SegmentString(), len(buf), cap(buf))
}

func (msf *MockSegmentFile) Close() error {
	logutil.Debugf("%s:%s | Close", msf.TypeName, msf.FileName)
	return nil
}

func (msf *MockSegmentFile) Destory() {
	logutil.Debugf("%s:%s | Destory", msf.TypeName, msf.FileName)
}

func (msf *MockSegmentFile) Ref() {
	logutil.Debugf("%s:%s | Ref All", msf.TypeName, msf.FileName)
	msf.RefHelper.Ref()
}

func (msf *MockSegmentFile) close() {
	msf.Close()
	msf.Destory()
}

func (msf *MockSegmentFile) Unref() {
	logutil.Debugf("%s:%s | Unref All", msf.TypeName, msf.FileName)
	msf.RefHelper.Unref()
}

func (msf *MockSegmentFile) RefBlock(id common.ID) {
	if !id.IsSameSegment(msf.ID) {
		panic("logic error")
	}
	logutil.Debugf("%s:%s | Ref %s", msf.TypeName, msf.FileName, id.BlockString())
	msf.RefHelper.Ref()
}

func (msf *MockSegmentFile) UnrefBlock(id common.ID) {
	if !id.IsSameSegment(msf.ID) {
		panic("logic error")
	}
	logutil.Debugf("%s:%s | Unref %s", msf.TypeName, msf.FileName, id.BlockString())
	msf.RefHelper.Unref()
}

func (msf *MockSegmentFile) GetDir() string {
	return filepath.Dir(msf.FileName)
}

func (msf *MockSegmentFile) MakeVirtualIndexFile(meta *base.IndexMeta) common.IVFile {
	return nil
}

func (msf *MockSegmentFile) MakeVirtualBlkIndexFile(id *common.ID, meta *base.IndexMeta) common.IVFile {
	return nil
}

func (msf *MockSegmentFile) MakeVirtualPartFile(id *common.ID) common.IVFile {
	return newPartFile(id, msf, true)
}
