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

func NewMockSegmentFile(dirname string, ft FileType, id common.ID) ISegmentFile {
	sf := new(MockSegmentFile)
	sf.FileType = ft
	sf.ID = id
	if ft == SortedSegFile {
		sf.TypeName = "MockSortedSegmentFile"
	} else if ft == UnsortedSegFile {
		sf.TypeName = "MockUnsortedSegmentFile"
	} else {
		panic("unspported")
	}
	sf.FileName = e.MakeFilename(dirname, e.FTSegment, id.ToSegmentFileName(), false)
	log.Infof("%s:%s | Created", sf.TypeName, sf.FileName)
	return sf
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

func (msf *MockSegmentFile) RefIndex() {
	log.Infof("%s:%s | RefIndex", msf.TypeName, msf.FileName)
	atomic.AddInt32(&msf.Refs, int32(1))
}

func (msf *MockSegmentFile) UnrefIndex() {
	log.Infof("%s:%s | UnrefIndex", msf.TypeName, msf.FileName)
	v := atomic.AddInt32(&msf.Refs, int32(-1))
	if v < int32(0) {
		panic("logic error")
	}
	if v == int32(0) {
		msf.Close()
		msf.Destory()
	}
}

func (msf *MockSegmentFile) MakeColPartFile(id *common.ID) IColPartFile {
	psf := &ColPartFile{
		SegmentFile: msf,
		ID:          id,
	}
	return psf
}
