package dataio

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"os"
	"sync"
)

type BlockFile struct {
	sync.RWMutex
	os.File
	ID    layout.ID
	Parts map[Key]Pointer
}

func NewBlockFile(dirname string, id layout.ID) *BlockFile {
	bf := &BlockFile{
		Parts: make(map[Key]Pointer),
		ID:    id,
	}

	name := e.MakeFilename(dirname, e.FTBlock, id.ToBlockFileName(), false)
	log.Infof("BlockFile name %s", name)
	if _, err := os.Stat(name); os.IsNotExist(err) {
		panic(fmt.Sprintf("Specified file %s not existed", name))
	}
	r, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("Cannot open specified file %s: %s", name, err))
	}

	bf.File = *r
	bf.initPointers()
	return bf
}

func (bf *BlockFile) initPointers() {
	// TODO
}

func (bf *BlockFile) ReadPart(colIdx uint64, id layout.ID, buf []byte) {
	key := Key{
		Col: colIdx,
		ID:  id,
	}
	pointer, ok := bf.Parts[key]
	if !ok {
		panic("logic error")
	}
	if len(buf) != int(pointer.Len) {
		panic("logic error")
	}
	n, err := bf.ReadAt(buf, pointer.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n != int(pointer.Len) {
		panic("logic error")
	}
}
