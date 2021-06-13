package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
	"sync/atomic"
)

type UnsortedSegmentFile struct {
	sync.RWMutex
	ID     common.ID
	Blocks map[common.ID]*BlockFile
	Dir    string
	Refs   int32
}

func NewUnsortedSegmentFile(dirname string, id common.ID) ISegmentFile {
	usf := &UnsortedSegmentFile{
		ID:     id,
		Dir:    dirname,
		Blocks: make(map[common.ID]*BlockFile),
	}
	return usf
}

func (sf *UnsortedSegmentFile) RefBlock(id common.ID) {
	sf.Lock()
	defer sf.Unlock()
	_, ok := sf.Blocks[id]
	if !ok {
		bf := NewBlockFile(sf.Dir, id)
		sf.AddBlock(id, bf)
	}
	atomic.AddInt32(&sf.Refs, int32(1))
}

func (sf *UnsortedSegmentFile) UnrefBlock(id common.ID) {
	v := atomic.AddInt32(&sf.Refs, int32(-1))
	if v == int32(0) {
		sf.Destory()
	}
	if v < int32(0) {
		panic("logic error")
	}
}

func (sf *UnsortedSegmentFile) MakeColSegmentFile(colIdx int) IColSegmentFile {
	csf := &ColSegmentFile{
		SegmentFile: sf,
		ColIdx:      uint64(colIdx),
	}
	return csf
}

func (sf *UnsortedSegmentFile) MakeColPartFile(colIdx int, id *common.ID) IColPartFile {
	cpf := &ColPartFile{
		ColIdx:      uint64(colIdx),
		ID:          id,
		SegmentFile: sf,
	}
	return cpf
}

func (sf *UnsortedSegmentFile) Close() error {
	for _, blkFile := range sf.Blocks {
		err := blkFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (sf *UnsortedSegmentFile) Destory() {
	for _, blkFile := range sf.Blocks {
		blkFile.Destory()
	}
	sf.Blocks = nil
}

func (sf *UnsortedSegmentFile) GetBlock(id common.ID) *BlockFile {
	sf.RLock()
	defer sf.RUnlock()
	blk := sf.Blocks[id]
	return blk
}

func (sf *UnsortedSegmentFile) AddBlock(id common.ID, bf *BlockFile) {
	_, ok := sf.Blocks[id]
	if ok {
		panic("logic error")
	}
	sf.Blocks[id] = bf
}

func (sf *UnsortedSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	sf.RLock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	sf.RUnlock()
	blk.ReadPart(colIdx, id, buf)
}
