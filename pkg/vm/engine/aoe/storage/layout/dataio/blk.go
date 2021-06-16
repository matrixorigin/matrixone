package dataio

import (
	"encoding/binary"
	"fmt"
	"matrixone/pkg/encoding"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

type BlockFile struct {
	sync.RWMutex
	os.File
	ID    common.ID
	Parts map[Key]base.Pointer
	Meta  *FileMeta
}

func NewBlockFile(dirname string, id common.ID) *BlockFile {
	bf := &BlockFile{
		Parts: make(map[Key]base.Pointer),
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
	fourBytes := make([]byte, 4)
	_, err := bf.File.Read(twoBytes)
	if err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	indexes := []index.Index{}
	indexCnt := encoding.DecodeInt16(twoBytes)
	if indexCnt > 0 {
		for i := 0; i < int(indexCnt); i++ {
			_, err := bf.File.Read(fourBytes)
			if err != nil {
				panic(fmt.Sprintf("unexpect error: %s", err))
			}
			length := encoding.DecodeInt32(fourBytes)
			buf := make([]byte, int(length))
			_, err = bf.File.Read(buf)
			if err != nil {
				panic(fmt.Sprintf("unexpect error: %s", err))
			}
			zm := new(index.ZoneMapIndex)
			zm.Unmarshall(buf)
			indexes = append(indexes, zm)
		}
	}

	_, err = bf.File.Read(twoBytes)
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
		bf.Parts[key] = base.Pointer{
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
