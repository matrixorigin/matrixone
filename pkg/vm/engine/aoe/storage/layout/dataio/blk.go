// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/prefetch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type FileNameFactory = func(string, common.ID) string

// BlockFile file structure:
// algo | colCntlen | metaCnt | preIdxLen | preIdx | IdxLen | Idx
// col01 : coldata len | coldata originlen |
// col02 : coldata len | coldata originlen |
// ...
// col01 data | col02 data |  ...
type BlockFile struct {
	common.RefHelper
	os.File
	ID          common.ID
	Parts       map[base.Key]*base.Pointer
	Meta        *FileMeta
	SegmentFile base.ISegmentFile
	Info        common.FileInfo
	DataAlgo    int
	Idx         *metadata.LogIndex
	PrevIdx     *metadata.LogIndex
	Range       *metadata.LogRange
	Count       uint64
}

func blockFileNameFactory(dir string, id common.ID) string {
	return common.MakeBlockFileName(dir, id.ToBlockFileName(), id.TableID, false)
}

func NewBlockFile(segFile base.ISegmentFile, id common.ID, nameFactory FileNameFactory) *BlockFile {
	bf := &BlockFile{
		Parts:       make(map[base.Key]*base.Pointer),
		ID:          id,
		Meta:        NewFileMeta(),
		SegmentFile: segFile,
	}

	dirname := segFile.GetDir()
	if nameFactory == nil {
		nameFactory = blockFileNameFactory
	}
	name := nameFactory(dirname, id)
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
	logutil.Infof(" %s | BlockFile | Destorying", name)
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
	var (
		cols uint16
		algo uint8
		err  error
	)
	offset, _ := bf.File.Seek(0, io.SeekCurrent)
	if err = binary.Read(&bf.File, binary.BigEndian, &algo); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	if err = binary.Read(&bf.File, binary.BigEndian, &cols); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	if err = binary.Read(&bf.File, binary.BigEndian, &bf.Count); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}

	buf := make([]byte, 24)
	if err = binary.Read(&bf.File, binary.BigEndian, &buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	bf.Range = new(metadata.LogRange)
	if err = bf.Range.Unmarshal(buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}

	var sz int32
	if err = binary.Read(&bf.File, binary.BigEndian, &sz); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	buf = make([]byte, sz)
	if err = binary.Read(&bf.File, binary.BigEndian, &buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	bf.PrevIdx = new(metadata.LogIndex)
	if err = bf.PrevIdx.UnMarshal(buf); err != nil {
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
	bf.Idx = new(metadata.LogIndex)
	if err = bf.Idx.UnMarshal(buf); err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	headSize := 8 + int(sz+sz_) + 24 + 3 + 8 + 2*8*int(cols)
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
		bf.Parts[key].Offset = int64(currOffset)
		// log.Infof("(Offset, Len, OriginLen, Algo)=(%d %d, %d, %d)", currOffset, bf.Parts[key].Len, bf.Parts[key].OriginLen, algo)
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
	// logutil.Infof("%s %d-%d-%d", bf.Name(), pointer.Offset, pointer.Len, pointer.OriginLen)
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
		return errors.New(fmt.Sprintf("column block <blk:%d-col:%d> not found", id.BlockID, colIdx))
	}
	offset := pointer.Offset
	sz := pointer.Len
	return prefetch.Prefetch(bf.Fd(), uintptr(offset), uintptr(sz))
}
