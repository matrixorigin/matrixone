package dataio

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type MockSegmentFile struct {
	FileName string
	FileType FileType
	TypeName string
	Refs     int32
	ID       common.ID
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
	msf.FileName = e.MakeFilename(dirname, e.FTSegment, id.ToSegmentFileName(), false)
	log.Infof("%s:%s | Created", msf.TypeName, msf.FileName)
	return msf
}

func (msf *MockSegmentFile) GetIndexMeta() *base.IndexesMeta {
	return nil
}

func (msf *MockSegmentFile) GetBlockIndexMeta(id common.ID) *base.IndexesMeta {
	return nil
}

func (msf *MockSegmentFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	log.Infof("(%s:%s) | ReadPoint (Off: %d, Len: %d) size: %d cap: %d", msf.TypeName, msf.FileName, ptr.Offset, ptr.Len, len(buf), cap(buf))
}

func (msf *MockSegmentFile) ReadBlockPoint(id common.ID, ptr *base.Pointer, buf []byte) {
	log.Infof("(%s:%s) | ReadBlockPoint[%s] (Off: %d, Len: %d) size: %d cap: %d", msf.TypeName, msf.FileName, id.BlockString(), ptr.Offset, ptr.Len, len(buf), cap(buf))
}

func (msf *MockSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	log.Infof("(%s:%s) | ReadPart %d %s size: %d cap: %d", msf.TypeName, msf.FileName, colIdx, id.SegmentString(), len(buf), cap(buf))
}

func (msf *MockSegmentFile) Close() error {
	log.Infof("%s:%s | Close", msf.TypeName, msf.FileName)
	return nil
}

func (msf *MockSegmentFile) Destory() {
	log.Infof("%s:%s | Destory", msf.TypeName, msf.FileName)
}

func (msf *MockSegmentFile) Ref() {
	log.Infof("%s:%s | Ref All", msf.TypeName, msf.FileName)
	atomic.AddInt32(&msf.Refs, int32(1))
}

func (msf *MockSegmentFile) Unref() {
	log.Infof("%s:%s | Unref All", msf.TypeName, msf.FileName)
	v := atomic.AddInt32(&msf.Refs, int32(-1))
	if v < int32(0) {
		panic("logic error")
	}
	if v == int32(0) {
		msf.Close()
		msf.Destory()
	}
}

func (msf *MockSegmentFile) RefBlock(id common.ID) {
	if !id.IsSameSegment(msf.ID) {
		panic("logic error")
	}
	log.Infof("%s:%s | Ref %s", msf.TypeName, msf.FileName, id.BlockString())
	atomic.AddInt32(&msf.Refs, int32(1))
}

func (msf *MockSegmentFile) UnrefBlock(id common.ID) {
	if !id.IsSameSegment(msf.ID) {
		panic("logic error")
	}
	log.Infof("%s:%s | Unref %s", msf.TypeName, msf.FileName, id.BlockString())
	v := atomic.AddInt32(&msf.Refs, int32(-1))
	if v < int32(0) {
		panic("logic error")
	}
	if v == int32(0) {
		msf.Close()
		msf.Destory()
	}
}

func (msf *MockSegmentFile) MakeVirtualSegmentIndexFile(meta *base.IndexMeta) base.IVirtaulFile {
	return nil
}

func (msf *MockSegmentFile) MakeVirtualPartFile(id *common.ID) base.IVirtaulFile {
	psf := &ColPartFile{
		SegmentFile: msf,
		ID:          id,
	}
	return psf
}
