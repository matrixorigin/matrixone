package dataio

import (
	"encoding/binary"
	"fmt"
	"io"
	logutil2 "matrixone/pkg/logutil"
	"matrixone/pkg/prefetch"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
)

type BlockFile struct {
	common.RefHelper
	os.File
	ID          common.ID
	Parts       map[base.Key]*base.Pointer
	Meta        *FileMeta
	SegmentFile base.ISegmentFile
	Info        common.FileInfo
	DataAlgo    int
}

func NewBlockFile(segFile base.ISegmentFile, id common.ID) base.IBlockFile {
	bf := &BlockFile{
		Parts:       make(map[base.Key]*base.Pointer),
		ID:          id,
		Meta:        NewFileMeta(),
		SegmentFile: segFile,
	}

	dirname := segFile.GetDir()
	name := e.MakeBlockFileName(dirname, id.ToBlockFileName(), id.TableID, false)
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
	bf.Ref()
	bf.OnZeroCB = bf.close
	return bf
}

func (bf *BlockFile) GetDir() string {
	return filepath.Dir(bf.Name())
}

func (bf *BlockFile) close() {
	bf.Close()
	bf.Destory()
}

func (bf *BlockFile) Destory() {
	name := bf.Name()
	logutil2.Debugf(" %s | BlockFile | Destorying", name)
	err := os.Remove(name)
	if err != nil {
		panic(err)
	}
}

func (bf *BlockFile) GetIndicesMeta() *base.IndicesMeta {
	return bf.Meta.Indices
}

func (bf *BlockFile) MakeVirtualIndexFile(meta *base.IndexMeta) common.IVFile {
	return newEmbedBlockIndexFile(&bf.ID, bf.SegmentFile, meta)
}

func (bf *BlockFile) initPointers(id common.ID) {
	//indexMeta, err := index.DefaultRWHelper.ReadIndicesMeta(bf.File)
	//if err != nil {
	//	panic(fmt.Sprintf("unexpect error: %s", err))
	//}
	//bf.Meta.Indices = indexMeta

	var (
		cols uint16
		algo uint8
		err error
	)
	offset, _ := bf.File.Seek(0, io.SeekCurrent)
	if err = binary.Read(&bf.File, binary.BigEndian, &algo); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	if err = binary.Read(&bf.File, binary.BigEndian, &cols); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	var count uint64
	if err = binary.Read(&bf.File, binary.BigEndian, &count); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	var sz int32
	if err = binary.Read(&bf.File, binary.BigEndian, &sz); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	buf := make([]byte, sz)
	if err = binary.Read(&bf.File, binary.BigEndian, &buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	prevIdx := metadata.LogIndex{}
	if err = prevIdx.UnMarshall(buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	var sz_ int32
	if err = binary.Read(&bf.File, binary.BigEndian, &sz_); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	buf = make([]byte, sz_)
	if err = binary.Read(&bf.File, binary.BigEndian, &buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	idx := metadata.LogIndex{}
	if err = idx.UnMarshall(buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	headSize := 8 + int(sz + sz_) + 3 + 8 + 2*8*int(cols)
	currOffset := headSize + int(offset)
	for i := uint16(0); i < cols; i++ {
		key := base.Key{
			Col: uint64(i),
			ID:  id.AsBlockID(),
		}
		bf.Parts[key] = &base.Pointer{}
		err = binary.Read(&bf.File, binary.BigEndian, &bf.Parts[key].Len)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		err = binary.Read(&bf.File, binary.BigEndian, &bf.Parts[key].OriginLen)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		// log.Infof("(Len, OriginLen, Algo)=(%d, %d, %d)", bf.Parts[key].Len, bf.Parts[key].OriginLen, algo)
		bf.Parts[key].Offset = int64(currOffset)
		currOffset += int(bf.Parts[key].Len)
	}
	bf.DataAlgo = int(algo)
}

func (bf *BlockFile) Stat() common.FileInfo {
	return bf.Info
}

func (bf *BlockFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	n, err := bf.ReadAt(buf, ptr.Offset)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	if n > int(ptr.Len) {
		panic("logic error")
	}
}

func (bf *BlockFile) DataCompressAlgo(id common.ID) int {
	return bf.DataAlgo
}

func (bf *BlockFile) PartSize(colIdx uint64, id common.ID, isOrigin bool) int64 {
	key := base.Key{
		Col: colIdx,
		ID:  id.AsBlockID(),
	}
	pointer, ok := bf.Parts[key]
	if !ok {
		panic("logic error")
	}
	if isOrigin {
		return int64(pointer.OriginLen)
	}
	return int64(pointer.Len)
}

func (bf *BlockFile) GetFileType() common.FileType {
	return common.DiskFile
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

func (bf *BlockFile) PrefetchPart(colIdx uint64, id common.ID) error {
	key := base.Key{
		Col: colIdx,
		ID:  id.AsBlockID(),
	}
	pointer, ok := bf.Parts[key]
	if !ok {
		panic("logic error")
	}
	offset := pointer.Offset
	sz := pointer.Len
	// integrate vfs later
	return prefetch.Prefetch(bf.Fd(), uintptr(offset), uintptr(sz))
}
