package dataio

import (
	"encoding/binary"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"os"
	"path/filepath"
	"sync/atomic"
)

func NewSortedSegmentFile(dirname string, id common.ID) base.ISegmentFile {
	sf := &SortedSegmentFile{
		Parts:      make(map[base.Key]*base.Pointer),
		ID:         id,
		Meta:       NewFileMeta(),
		BlocksMeta: make(map[common.ID]*FileMeta),
		Info: &fileStat{
			name: id.ToSegmentFilePath(),
		},
	}

	name := e.MakeSegmentFileName(dirname, id.ToSegmentFileName(), id.TableID, false)
	// log.Infof("SegmentFile name %s", name)
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
	ID common.ID
	os.File
	Refs       int32
	Parts      map[base.Key]*base.Pointer
	Meta       *FileMeta
	BlocksMeta map[common.ID]*FileMeta
	Info       *fileStat
	DataAlgo   int
}

func (sf *SortedSegmentFile) MakeVirtualIndexFile(meta *base.IndexMeta) common.IVFile {
	return newEmbedIndexFile(sf, meta)
}

func (sf *SortedSegmentFile) MakeVirtualBlkIndexFile(id *common.ID, meta *base.IndexMeta) common.IVFile {
	return newEmbedIndexFile(sf, meta)
}

func (sf *SortedSegmentFile) MakeVirtualPartFile(id *common.ID) common.IVFile {
	return newPartFile(id, sf, false)
}

func (sf *SortedSegmentFile) Stat() common.FileInfo {
	return sf.Info
}

func (sf *SortedSegmentFile) GetDir() string {
	return filepath.Dir(sf.Name())
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
	meta, err := index.DefaultRWHelper.ReadIndicesMeta(sf.File)
	if err != nil {
		panic(err)
	}
	sf.Meta.Indices = meta
	//log.Info(meta.String())

	blkCnt := uint32(0)
	err = binary.Read(&sf.File, binary.BigEndian, &blkCnt)
	if err != nil {
		panic(err)
	}
	// log.Infof("blkCnt=%d", blkCnt)
	blkIds := make([]uint64, blkCnt)
	blksPos := make([]uint32, blkCnt)
	for i := 0; i < int(blkCnt); i++ {
		if err = binary.Read(&sf.File, binary.BigEndian, &blkIds[i]); err != nil {
			panic(err)
		}
		// log.Infof("blkId=%d", blkIds[i])
	}
	for i := 0; i < int(blkCnt); i++ {
		if err = binary.Read(&sf.File, binary.BigEndian, &blksPos[i]); err != nil {
			panic(err)
		}
		// log.Infof("blkPos=%d", blksPos[i])
	}
	var endPos uint32
	if err = binary.Read(&sf.File, binary.BigEndian, &endPos); err != nil {
		panic(err)
	}
	// log.Infof("endPos=%d", endPos)
	for i := 0; i < int(blkCnt); i++ {
		sf.initBlkPointers(blkIds[i], blksPos[i])
	}
	// log.Infof("parts cnt %d", len(sf.Parts))
	// for k, v := range sf.Parts {
	// 	log.Infof("blk=%s, col=%d, value=%v", k.ID.BlockString(), k.Col, v)
	// }
}

func (sf *SortedSegmentFile) initBlkPointers(blkId uint64, pos uint32) {
	id := sf.ID.AsBlockID()
	id.BlockID = blkId
	_, err := sf.File.Seek(int64(pos), io.SeekStart)
	if err != nil {
		panic(err)
	}

	//_, err = index.DefaultRWHelper.ReadIndicesMeta(sf.File)
	//if err != nil {
	//	panic(fmt.Sprintf("unexpect error: %s", err))
	//}

	var (
		cols uint16
		algo uint8
	)
	offset, _ := sf.File.Seek(0, io.SeekCurrent)
	if err = binary.Read(&sf.File, binary.BigEndian, &algo); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	if err = binary.Read(&sf.File, binary.BigEndian, &cols); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	headSize := 3 + 2*8*int(cols)
	currOffset := headSize + int(offset)
	for i := uint16(0); i < cols; i++ {
		key := base.Key{
			Col: uint64(i),
			ID:  id.AsBlockID(),
		}
		key.ID.Idx = i
		sf.Parts[key] = &base.Pointer{}
		err = binary.Read(&sf.File, binary.BigEndian, &sf.Parts[key].Len)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		err = binary.Read(&sf.File, binary.BigEndian, &sf.Parts[key].OriginLen)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		// log.Infof("(Len, OriginLen, Algo)=(%d, %d, %d)", sf.Parts[key].Len, sf.Parts[key].OriginLen, algo)
		sf.Parts[key].Offset = int64(currOffset)
		currOffset += int(sf.Parts[key].Len)
	}
	sf.DataAlgo = int(algo)
}

func (sf *SortedSegmentFile) GetFileType() common.FileType {
	return common.DiskFile
}

func (sf *SortedSegmentFile) GetIndicesMeta() *base.IndicesMeta {
	return sf.Meta.Indices
}

func (sf *SortedSegmentFile) GetBlockIndicesMeta(id common.ID) *base.IndicesMeta {
	blkMeta := sf.BlocksMeta[id]
	if blkMeta == nil {
		return nil
	}
	return blkMeta.Indices
}

func (sf *SortedSegmentFile) Destory() {
	name := sf.Name()
	log.Infof(" %s | SegmentFile | Destorying", name)
	err := os.Remove(name)
	if err != nil {
		panic(err)
	}
}

func (sf *SortedSegmentFile) ReadPoint(ptr *base.Pointer, buf []byte) {
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

func (sf *SortedSegmentFile) DataCompressAlgo(id common.ID) int {
	return sf.DataAlgo
}

func (sf *SortedSegmentFile) PartSize(colIdx uint64, id common.ID, isOrigin bool) int64 {
	key := base.Key{
		Col: colIdx,
		ID:  id,
	}
	pointer, ok := sf.Parts[key]
	if !ok {
		panic("logic error")
	}
	if isOrigin {
		return int64(pointer.OriginLen)
	}
	return int64(pointer.Len)
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
	if len(buf) > int(pointer.Len) {
		panic(fmt.Sprintf("buf len is %d, but pointer len is %d", len(buf), pointer.Len))
	}

	sf.ReadPoint(pointer, buf)
}
