package dataio

import (
	"fmt"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"os"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

func NewSortedSegmentFile(dirname string, id common.ID) ISegmentFile {
	sf := &SortedSegmentFile{
		Parts: make(map[Key]Pointer),
		ID:    id,
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
	Refs  int32
	Parts map[Key]Pointer
}

func (sf *SortedSegmentFile) MakeColPartFile(colIdx int, id *common.ID) IColPartFile {
	cpf := &ColPartFile{
		ColIdx:      uint64(colIdx),
		ID:          id,
		SegmentFile: sf,
	}
	return cpf
}

func (sf *SortedSegmentFile) MakeColSegmentFile(colIdx int) IColSegmentFile {
	csf := &ColSegmentFile{
		SegmentFile: sf,
		ColIdx:      uint64(colIdx),
	}
	return csf
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

func (sf *SortedSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	key := Key{
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
	sf.Lock()
	defer sf.Unlock()
	n, err := sf.ReadAt(buf, pointer.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n != int(pointer.Len) {
		panic("logic error")
	}
}
