package dataio

import (
	"fmt"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"os"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

func NewSortedSegmentFile(dirname string, id common.ID) base.ISegmentFile {
	sf := &SortedSegmentFile{
		Parts:      make(map[base.Key]*base.Pointer),
		ID:         id,
		BlocksMeta: make(map[common.ID]*FileMeta),
	}

	name := e.MakeFilename(dirname, e.FTSegment, id.ToSegmentFileName(), false)
	log.Infof("SegmentFile name %s", name)
	if _, err := os.Stat(name); os.IsNotExist(err) {
		panic(fmt.Sprintf("Specified file %s not existed", name))
	}
	r, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("Cannot open specified file %s: %s", name, err))
	}

	sf.File = *r
	sf.initPointers()
	return sf
}

type SortedSegmentFile struct {
	sync.RWMutex
	ID common.ID
	os.File
	Refs       int32
	Parts      map[base.Key]*base.Pointer
	Meta       *FileMeta
	BlocksMeta map[common.ID]*FileMeta
}

func (sf *SortedSegmentFile) MakeVirtualSegmentIndexFile(meta *base.IndexMeta) base.IVirtaulFile {
	vf := &EmbbedIndexFile{
		SegmentFile: sf,
		Meta:        meta,
	}
	return vf
}

func (sf *SortedSegmentFile) MakeVirtualPartFile(id *common.ID) base.IVirtaulFile {
	cpf := &ColPartFile{
		ID:          id,
		SegmentFile: sf,
	}
	return cpf
}

func (sf *SortedSegmentFile) Ref() {
	atomic.AddInt32(&sf.Refs, int32(1))
}

func (sf *SortedSegmentFile) Unref() {
	v := atomic.AddInt32(&sf.Refs, int32(-1))
	if v < int32(0) {
		panic("logic error")
	}
	if v == int32(0) {
		sf.Close()
		sf.Destory()
	}
}

func (sf *SortedSegmentFile) RefBlock(id common.ID) {
	atomic.AddInt32(&sf.Refs, int32(1))
}

func (sf *SortedSegmentFile) UnrefBlock(id common.ID) {
	v := atomic.AddInt32(&sf.Refs, int32(-1))
	if v == int32(0) {
		sf.Destory()
	}
	if v < int32(0) {
		panic("logic error")
	}
}

func (sf *SortedSegmentFile) initPointers() {
	// TODO
}

func (sf *SortedSegmentFile) GetIndexesMeta() *base.IndexesMeta {
	return sf.Meta.Indexes
}

func (sf *SortedSegmentFile) GetBlockIndexesMeta(id common.ID) *base.IndexesMeta {
	blkMeta := sf.BlocksMeta[id]
	if blkMeta == nil {
		return nil
	}
	return blkMeta.Indexes
}

func (sf *SortedSegmentFile) Destory() {
	name := sf.Name()
	log.Infof("Destory sorted segment file: %s", name)
	err := sf.Close()
	if err != nil {
		panic(err)
	}
	err = os.Remove(name)
	if err != nil {
		panic(err)
	}
}

func (sf *SortedSegmentFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	sf.Lock()
	defer sf.Unlock()
	n, err := sf.ReadAt(buf, ptr.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n != int(ptr.Len) {
		panic("logic error")
	}
}

func (sf *SortedSegmentFile) ReadBlockPoint(id common.ID, ptr *base.Pointer, buf []byte) {
	sf.ReadPoint(ptr, buf)
}

func (sf *SortedSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	key := base.Key{
		Col: colIdx,
		ID:  id,
	}
	pointer, ok := sf.Parts[key]
	if !ok {
		panic("logic error")
	}
	if len(buf) != int(pointer.Len) {
		panic("logic error")
	}

	sf.ReadPoint(pointer, buf)
}
