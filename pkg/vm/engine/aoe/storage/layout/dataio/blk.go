package dataio

import (
	"encoding/binary"
	"fmt"
	"io"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"os"
	"path/filepath"
	"sync"

	log "github.com/sirupsen/logrus"
)

type BlockFile struct {
	sync.RWMutex
	os.File
	ID          common.ID
	Parts       map[base.Key]*base.Pointer
	Meta        *FileMeta
	SegmentFile base.ISegmentFile
	Info        base.FileInfo
}

func NewBlockFile(segFile base.ISegmentFile, id common.ID) base.IBlockFile {
	bf := &BlockFile{
		Parts:       make(map[base.Key]*base.Pointer),
		ID:          id,
		Meta:        NewFileMeta(),
		SegmentFile: segFile,
	}

	dirname := segFile.GetDir()
	name := e.MakeFilename(dirname, e.FTBlock, id.ToBlockFileName(), false)
	// log.Infof("BlockFile name %s", name)
	var info os.FileInfo
	var err error
	if info, err = os.Stat(name); os.IsNotExist(err) {
		panic(fmt.Sprintf("Specified file %s not existed", name))
	}
	bf.Info = &fileStat{
		size: info.Size(),
		name: id.ToBlockFilePath(),
	}
	r, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("Cannot open specified file %s: %s", name, err))
	}

	bf.File = *r
	bf.initPointers(id)
	return bf
}

func (bf *BlockFile) GetDir() string {
	return filepath.Dir(bf.Name())
}

func (bf *BlockFile) Destory() {
	name := bf.Name()
	log.Infof("Destory blockfile: %s", name)
	err := bf.Close()
	if err != nil {
		panic(err)
	}
	err = os.Remove(name)
	if err != nil {
		panic(err)
	}
}

func (bf *BlockFile) GetIndexesMeta() *base.IndexesMeta {
	return bf.Meta.Indexes
}

func (bf *BlockFile) MakeVirtualIndexFile(meta *base.IndexMeta) base.IVirtaulFile {
	return newEmbbedBlockIndexFile(&bf.ID, bf.SegmentFile, meta)
}

func (bf *BlockFile) initPointers(id common.ID) {
	indexMeta, err := index.DefaultRWHelper.ReadIndexesMeta(bf.File)
	if err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	bf.Meta.Indexes = indexMeta
	offset, _ := bf.File.Seek(0, io.SeekCurrent)
	twoBytes := make([]byte, 2)
	_, err = bf.File.Read(twoBytes)
	if err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}

	cols := binary.BigEndian.Uint16(twoBytes)
	headSize := 2 + 2*8*int(cols)
	currOffset := headSize + int(offset)
	eightBytes := make([]byte, 8)
	for i := uint16(0); i < cols; i++ {
		_, err = bf.File.Read(eightBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		blkID := binary.BigEndian.Uint64(eightBytes)
		if blkID != id.BlockID {
			panic("logic error")
		}
		key := base.Key{
			Col: uint64(i),
			ID:  id.AsBlockID(),
		}
		_, err = bf.File.Read(eightBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		bf.Parts[key] = &base.Pointer{
			Offset: int64(currOffset),
			Len:    binary.BigEndian.Uint64(eightBytes),
		}
		currOffset += int(bf.Parts[key].Len)
	}
}

func (bf *BlockFile) Stat() base.FileInfo {
	return bf.Info
}

func (bf *BlockFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	bf.Lock()
	defer bf.Unlock()
	n, err := bf.ReadAt(buf, ptr.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n > int(ptr.Len) {
		panic("logic error")
	}
}

func (bf *BlockFile) PartSize(colIdx uint64, id common.ID) int64 {
	key := base.Key{
		Col: colIdx,
		ID:  id.AsBlockID(),
	}
	pointer, ok := bf.Parts[key]
	if !ok {
		panic("logic error")
	}

	return int64(pointer.Len)
}

func (bf *BlockFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	key := base.Key{
		Col: colIdx,
		ID:  id.AsBlockID(),
	}
	pointer, ok := bf.Parts[key]
	if !ok {
		panic("logic error")
	}

	if len(buf) > int(pointer.Len) {
		panic(fmt.Sprintf("buf len is %d, but pointer len is %d", len(buf), pointer.Len))
	}
	bf.ReadPoint(pointer, buf)
}
