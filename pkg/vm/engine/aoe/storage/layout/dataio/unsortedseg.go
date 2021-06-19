package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
	"sync/atomic"
)

type UnsortedSegmentFile struct {
	sync.RWMutex
	ID     common.ID
	Blocks map[common.ID]base.IBlockFile
	Dir    string
	Refs   int32
}

func NewUnsortedSegmentFile(dirname string, id common.ID) base.ISegmentFile {
	usf := &UnsortedSegmentFile{
		ID:     id,
		Dir:    dirname,
		Blocks: make(map[common.ID]base.IBlockFile),
	}
	return usf
}

func (sf *UnsortedSegmentFile) Ref() {
	atomic.AddInt32(&sf.Refs, int32(1))
}

func (sf *UnsortedSegmentFile) Unref() {
	v := atomic.AddInt32(&sf.Refs, int32(-1))
	if v < int32(0) {
		panic("logic error")
	}
	if v == int32(0) {
		sf.Close()
		sf.Destory()
	}
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

func (sf *UnsortedSegmentFile) GetIndexMeta() *base.IndexesMeta {
	return nil
}

func (sf *UnsortedSegmentFile) GetBlockIndexMeta(id common.ID) *base.IndexesMeta {
	blk := sf.GetBlock(id)
	if blk == nil {
		return nil
	}
	return blk.GetIndexMeta()
}

func (sf *UnsortedSegmentFile) MakeVirtualSegmentIndexFile(meta *base.IndexMeta) base.IVirtaulFile {
	return nil
}

func (sf *UnsortedSegmentFile) MakeVirtualPartFile(id *common.ID) base.IVirtaulFile {
	cpf := &ColPartFile{
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

func (sf *UnsortedSegmentFile) GetBlock(id common.ID) base.IBlockFile {
	sf.RLock()
	defer sf.RUnlock()
	blk := sf.Blocks[id]
	return blk
}

func (sf *UnsortedSegmentFile) AddBlock(id common.ID, bf base.IBlockFile) {
	_, ok := sf.Blocks[id]
	if ok {
		panic("logic error")
	}
	sf.Blocks[id] = bf
}

func (sf *UnsortedSegmentFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	panic("not supported")
}

func (sf *UnsortedSegmentFile) ReadBlockPoint(id common.ID, ptr *base.Pointer, buf []byte) {
	sf.RLock()
	blk, ok := sf.Blocks[id.AsBlockID()]
	if !ok {
		panic("logic error")
	}
	sf.RUnlock()
	blk.ReadPoint(ptr, buf)
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
