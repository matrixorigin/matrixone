package dataio

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"os"
	"sync"
)

type ISegmentFile interface {
	ReadPart(colIdx uint64, id common.ID, buf []byte)
}

type IColSegmentFile interface {
	ReadPart(id common.ID, buf []byte)
}

type ColSegmentFile struct {
	SegmentFile ISegmentFile
	ColIdx      uint64
}

func (csf *ColSegmentFile) ReadPart(id common.ID, buf []byte) {
	csf.SegmentFile.ReadPart(csf.ColIdx, id, buf)
}

type MockColSegmentFile struct {
}

func (msf *MockColSegmentFile) ReadPart(id common.ID, buf []byte) {
	log.Infof("MockColSegmentFile ReadPart %s size: %d cap: %d", id.SegmentString(), len(buf), cap(buf))
}

type MockSegmentFile struct {
}

func (msf *MockSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	log.Infof("MockSegmentFile ReadPart %d %s size: %d cap: %d", colIdx, id.SegmentString(), len(buf), cap(buf))
}

type UnsortedSegmentFile struct {
	sync.RWMutex
	ID     common.ID
	Blocks map[common.ID]*BlockFile
}

func NewUnsortedSegmentFile(dirname string, id common.ID) ISegmentFile {
	usf := &UnsortedSegmentFile{
		ID:     id,
		Blocks: make(map[common.ID]*BlockFile),
	}
	return usf
}

func (sf *UnsortedSegmentFile) AddBlock(id common.ID, bf *BlockFile) {
	_, ok := sf.Blocks[id]
	if ok {
		panic("logic error")
	}
	sf.Blocks[id] = bf
}

func (sf *UnsortedSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	blk.ReadPart(colIdx, id, buf)
}

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
	Parts map[Key]Pointer
}

func (sf *SortedSegmentFile) initPointers() {
	// TODO
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
	n, err := sf.ReadAt(buf, pointer.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n != int(pointer.Len) {
		panic("logic error")
	}
}
