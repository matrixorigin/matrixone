package dataio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"matrixone/pkg/encoding"
	logutil2 "matrixone/pkg/logutil"
	"matrixone/pkg/prefetch"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
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
	sf.OnZeroCB = sf.close
	return sf
}

type SortedSegmentFile struct {
	common.RefHelper
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

func (sf *SortedSegmentFile) close() {
	sf.Close()
	sf.Destory()
}

func (sf *SortedSegmentFile) RefBlock(id common.ID) {
	sf.Ref()
}

func (sf *SortedSegmentFile) UnrefBlock(id common.ID) {
	sf.Unref()
}

func (sf *SortedSegmentFile) initPointers() {
	// read metadata-1
	sz := headerSize + reservedSize + algoSize + blkCntSize + colCntSize
	buf := make([]byte, sz)
	metaBuf := bytes.NewBuffer(buf)
	if err := binary.Read(&sf.File, binary.BigEndian, metaBuf.Bytes()); err != nil {
		panic(err)
	}

	blkCnt := uint32(0)
	colCnt := uint32(0)
	algo := uint8(0)
	header := make([]byte, 32)
	reserved := make([]byte, 64)
	var err error
	if err = binary.Read(metaBuf, binary.BigEndian, &header); err != nil {
		panic(err)
	}
	if version := encoding.DecodeUint64(header); version != Version {
		panic("version mismatched")
	}
	if err = binary.Read(metaBuf, binary.BigEndian, &reserved); err != nil {
		panic(err)
	}
	if err = binary.Read(metaBuf, binary.BigEndian, &algo); err != nil {
		panic(err)
	}
	if err = binary.Read(metaBuf, binary.BigEndian, &blkCnt); err != nil {
		panic(err)
	}
	if err = binary.Read(metaBuf, binary.BigEndian, &colCnt); err != nil {
		panic(err)
	}

	// read metadata-2
	sz = startPosSize +
		endPosSize +
		int(blkCnt)*(blkCountSize+blkIdSize+2*blkIdxSize) +
		int(blkCnt*colCnt)*(colSizeSize*2) +
		int(colCnt)*colPosSize

	buf = make([]byte, sz)
	metaBuf = bytes.NewBuffer(buf)
	if err = binary.Read(&sf.File, binary.BigEndian, metaBuf.Bytes()); err != nil {
		panic(err)
	}

	blkIds := make([]uint64, blkCnt)
	blkCounts := make([]uint64, blkCnt)
	idxBuf := make([]byte, 32)
	preIndices := make([]*metadata.LogIndex, blkCnt)
	indices := make([]*metadata.LogIndex, blkCnt)

	for i := uint32(0); i < blkCnt; i++ {
		if err = binary.Read(metaBuf, binary.BigEndian, &blkIds[i]); err != nil {
			panic(err)
		}
		if err = binary.Read(metaBuf, binary.BigEndian, &blkCounts[i]); err != nil {
			panic(err)
		}
		if err = binary.Read(metaBuf, binary.BigEndian, &idxBuf); err != nil {
			panic(err)
		}
		if !bytes.Equal(idxBuf, []byte{}) {
			preIndices[i] = &metadata.LogIndex{
				ID:       0,
				Start:    0,
				Count:    0,
				Capacity: 0,
			}
			if err = preIndices[i].UnMarshall(idxBuf); err != nil {
				panic(err)
			}
		}
		if err = binary.Read(metaBuf, binary.BigEndian, &idxBuf); err != nil {
			panic(err)
		}
		if !bytes.Equal(idxBuf, []byte{}) {
			indices[i] = &metadata.LogIndex{
				ID:       0,
				Start:    0,
				Count:    0,
				Capacity: 0,
			}
			if err = indices[i].UnMarshall(idxBuf); err != nil {
				panic(err)
			}
		}
	}

	for i := uint32(0); i < colCnt; i++ {
		for j := uint32(0); j < blkCnt; j++ {
			blkId := blkIds[j]
			id := sf.ID.AsBlockID()
			id.BlockID = blkId
			key := base.Key{
				Col: uint64(i),
				ID:  id,
			}
			key.ID.Idx = uint16(i)
			sf.Parts[key] = &base.Pointer{}
			if err = binary.Read(metaBuf, binary.BigEndian, &sf.Parts[key].Len); err != nil {
				panic(err)
			}
			if err = binary.Read(metaBuf, binary.BigEndian, &sf.Parts[key].OriginLen); err != nil {
				panic(err)
			}
		}
	}

	startPos := int64(0)
	endPos := int64(0)
	colPos := make([]int64, colCnt)

	if err = binary.Read(metaBuf, binary.BigEndian, &startPos); err != nil {
		panic(err)
	}
	if err = binary.Read(metaBuf, binary.BigEndian, &endPos); err != nil {
		panic(err)
	}
	for i := 0; i < int(colCnt); i++ {
		if err = binary.Read(metaBuf, binary.BigEndian, &colPos[i]); err != nil {
			panic(err)
		}
	}

	curOffset := startPos
	for i := 0; i < int(colCnt); i++ {
		for j := 0; j < int(blkCnt); j++ {
			blkId := blkIds[j]
			id := sf.ID.AsBlockID()
			id.BlockID = blkId
			key := base.Key{
				Col: uint64(i),
				ID:  id,
			}
			key.ID.Idx = uint16(i)
			sf.Parts[key].Offset = curOffset
			curOffset += int64(sf.Parts[key].Len)
		}
	}

	// skip data
	if _, err = sf.Seek(curOffset, io.SeekStart); err != nil {
		panic(err)
	}

	// read index
	meta, err := index.DefaultRWHelper.ReadIndicesMeta(sf.File)
	if err != nil {
		panic(err)
	}
	sf.Meta.Indices = meta

	// read footer
	footer := make([]byte, 64)
	if err = binary.Read(&sf.File, binary.BigEndian, &footer); err != nil {
		panic(err)
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
	logutil2.Debugf(" %s | SegmentFile | Destorying", name)
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

func (sf *SortedSegmentFile) PrefetchPart(colIdx uint64, id common.ID) error {
	key := base.Key{
		Col: colIdx,
		ID:  id,
	}
	pointer, ok := sf.Parts[key]
	if !ok {
		panic("logic error")
	}
	offset := pointer.Offset
	sz := pointer.Len
	// integrate vfs later
	return prefetch.Prefetch(sf.Fd(), uintptr(offset), uintptr(sz))
}
