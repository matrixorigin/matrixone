package dataio

import (
	"encoding/binary"
	"fmt"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

type BlockFile struct {
	sync.RWMutex
	os.File
	ID    common.ID
	Parts map[Key]Pointer
}

func NewBlockFile(dirname string, id common.ID) *BlockFile {
	bf := &BlockFile{
		Parts: make(map[Key]Pointer),
		ID:    id,
	}

	name := e.MakeFilename(dirname, e.FTBlock, id.ToBlockFileName(), false)
	// log.Infof("BlockFile name %s", name)
	if _, err := os.Stat(name); os.IsNotExist(err) {
		panic(fmt.Sprintf("Specified file %s not existed", name))
	}
	r, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("Cannot open specified file %s: %s", name, err))
	}

	bf.File = *r
	bf.initPointers(id)
	return bf
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

func (bf *BlockFile) initPointers(id common.ID) {
	twoBytes := make([]byte, 2)
	_, err := bf.File.Read(twoBytes)
	if err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}

	cols := binary.BigEndian.Uint16(twoBytes)
	headSize := 2 + 2*8*int(cols)
	currOffset := headSize
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
		key := Key{
			Col: uint64(i),
			ID:  id.AsBlockID(),
		}
		_, err = bf.File.Read(eightBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		bf.Parts[key] = Pointer{
			Offset: int64(currOffset),
			Len:    binary.BigEndian.Uint64(eightBytes),
		}
		currOffset += int(bf.Parts[key].Len)
	}
}

func (bf *BlockFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	key := Key{
		Col: colIdx,
		ID:  id.AsBlockID(),
	}
	pointer, ok := bf.Parts[key]
	if !ok {
		panic("logic error")
	}
	if len(buf) != int(pointer.Len) {
		panic("logic error")
	}
	bf.Lock()
	defer bf.Unlock()
	n, err := bf.ReadAt(buf, pointer.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n != int(pointer.Len) {
		panic("logic error")
	}
}
