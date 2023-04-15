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

package objectio

import (
	"bytes"
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/pierrec/lz4/v4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type ObjectWriter struct {
	sync.RWMutex
	object   *Object
	blocks   []blockData
	totalRow uint32
	colmeta  []ColumnMeta
	buffer   *ObjectBuffer
	fileName string
	lastId   uint32
	name     ObjectName
}

type blockData struct {
	meta        BlockObject
	data        [][]byte
	bloomFilter []byte
}

type WriterType int8

const (
	WriterNormal = iota
	WriterCheckpoint
	WriterQueryResult
	WriterGC
	WriterETL
)

func NewObjectWriterSpecial(wt WriterType, fileName string, fs fileservice.FileService) (*ObjectWriter, error) {
	var name ObjectName
	object := NewObject(fileName, fs)
	switch wt {
	case WriterNormal:
		name = BuildNormalName()
		break
	case WriterCheckpoint:
		name = BuildCheckpointName()
		break
	case WriterQueryResult:
		name = BuildQueryResultName()
		break
	case WriterGC:
		name = BuildDiskCleanerName()
		break
	case WriterETL:
		name = BuildETLName()
		break
	}
	writer := &ObjectWriter{
		fileName: fileName,
		name:     name,
		object:   object,
		buffer:   NewObjectBuffer(fileName),
		blocks:   make([]blockData, 0),
		lastId:   0,
	}
	return writer, nil
}

func NewObjectWriter(name ObjectName, fs fileservice.FileService) (*ObjectWriter, error) {
	fileName := name.String()
	object := NewObject(fileName, fs)
	writer := &ObjectWriter{
		fileName: fileName,
		name:     name,
		object:   object,
		buffer:   NewObjectBuffer(fileName),
		blocks:   make([]blockData, 0),
		lastId:   0,
	}
	return writer, nil
}

func (w *ObjectWriter) Write(batch *batch.Batch) (BlockObject, error) {
	block := NewBlock(uint16(len(batch.Vecs)))
	w.AddBlock(block, batch)
	return block, nil
}

func (w *ObjectWriter) UpdateBlockZM(blkIdx, colIdx int, zm ZoneMap) {
	w.blocks[blkIdx].meta.ColumnMeta(uint16(colIdx)).SetZoneMap(zm)
}

func (w *ObjectWriter) WriteBF(blkIdx, colIdx int, buf []byte) (err error) {
	w.blocks[blkIdx].bloomFilter = buf
	return
}

func (w *ObjectWriter) WriteObjectMeta(ctx context.Context, totalrow uint32, metas []ColumnMeta) {
	w.totalRow = totalrow
	w.colmeta = metas
}

func (w *ObjectWriter) prepareObjectMeta(columnCount uint16) (ObjectMeta, BlockIndex, uint32) {
	offset := uint32(0)
	blockCount := uint32(len(w.blocks))
	objectMeta := BuildObjectMeta(columnCount)
	objectMeta.BlockHeader().SetSequence(uint16(blockCount))
	sid := w.name.SegmentId()
	blockId := NewBlockid(&sid, w.name.Num(), uint16(blockCount))
	objectMeta.BlockHeader().SetBlockID(&blockId)
	objectMeta.BlockHeader().SetRows(w.totalRow)
	objectMeta.BlockHeader().SetColumnCount(columnCount)
	// write column meta
	for i, colMeta := range w.colmeta {
		objectMeta.AddColumnMeta(uint16(i), colMeta)
	}
	offset += objectMeta.Length()
	blockIndex := BuildBlockIndex(blockCount)
	blockIndex.SetBlockCount(blockCount)
	offset += blockIndex.Length()
	blockIndex.SetBlockCount(blockCount)
	for i, block := range w.blocks {
		n := uint32(len(block.meta))
		blockIndex.SetBlockMetaPos(uint32(i), offset, n)
		offset += n
	}
	extent := NewExtent(compress.None, HeaderSize, offset, offset)
	objectMeta.BlockHeader().SetMetaLocation(extent)
	return objectMeta, blockIndex, offset
}

func (w *ObjectWriter) prepareBlockMeta(offset uint32) uint32 {
	for i, block := range w.blocks {
		for idx := range block.data {
			location := w.blocks[i].meta.ColumnMeta(uint16(idx)).Location()
			location.SetOffset(offset)
			w.blocks[i].meta.ColumnMeta(uint16(idx)).setLocation(location)
			offset += location.Length()
		}
	}
	return offset
}

func (w *ObjectWriter) prepareBloomFilter(blockCount uint32, offset uint32) ([]byte, Extent, error) {
	bloomFilter := new(bytes.Buffer)
	bloomFilterStart := uint32(0)
	bloomFilterIndex := BuildBlockIndex(blockCount)
	bloomFilterIndex.SetBlockCount(blockCount)
	bloomFilterStart += bloomFilterIndex.Length()
	for i, block := range w.blocks {
		n := uint32(len(block.bloomFilter))
		bloomFilterIndex.SetBlockMetaPos(uint32(i), bloomFilterStart, n)
		bloomFilterStart += n
	}
	bloomFilter.Write(bloomFilterIndex)
	for _, block := range w.blocks {
		bloomFilter.Write(block.bloomFilter)
	}
	return w.WriteWithCompress(offset, bloomFilter.Bytes())
}

func (w *ObjectWriter) prepareZoneMapArea(blockCount uint32, offset uint32) ([]byte, Extent, error) {
	zoneMapArea := new(bytes.Buffer)
	zoneMapAreaStart := uint32(0)
	zoneMapAreaIndex := BuildBlockIndex(blockCount)
	zoneMapAreaIndex.SetBlockCount(blockCount)
	zoneMapAreaStart += zoneMapAreaIndex.Length()
	for i, block := range w.blocks {
		n := uint32(block.meta.GetColumnCount() * ZoneMapSize)
		zoneMapAreaIndex.SetBlockMetaPos(uint32(i), zoneMapAreaStart, n)
		zoneMapAreaStart += n
	}
	zoneMapArea.Write(zoneMapAreaIndex)
	for _, block := range w.blocks {
		for i := range block.data {
			zoneMapArea.Write(block.meta.ColumnMeta(uint16(i)).ZoneMap())
		}
	}
	return w.WriteWithCompress(offset, zoneMapArea.Bytes())
}

func (w *ObjectWriter) WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error) {
	var err error
	w.RLock()
	defer w.RUnlock()
	var columnCount uint16
	columnCount = 0
	if len(w.blocks) == 0 {
		logutil.Warn("object io: no block needs to be written")
	} else {
		columnCount = w.blocks[0].meta.GetColumnCount()
	}

	objectHeader := BuildHeader()

	blockCount := uint32(len(w.blocks))
	objectMeta := BuildObjectMeta(columnCount)

	// prepare object meta and block index
	objectMeta, blockIndex, offset := w.prepareObjectMeta(columnCount)
	objectMetaLocation := objectMeta.BlockHeader().MetaLocation()
	objectHeader.SetLocation(objectMetaLocation)
	offset += HeaderSize
	offset = w.prepareBlockMeta(offset)

	// prepare bloom filter
	bloomFilterData, bloomFilterExtent, err := w.prepareBloomFilter(blockCount, offset)
	if err != nil {
		return nil, err
	}
	objectMeta.BlockHeader().SetBloomFilter(bloomFilterExtent)
	offset += uint32(len(bloomFilterData))

	// prepare zone map area
	zoneMapAreaData, zoneMapAreaExtent, err := w.prepareZoneMapArea(blockCount, offset)
	if err != nil {
		return nil, err
	}
	objectMeta.BlockHeader().SetZoneMapArea(zoneMapAreaExtent)
	// begin write

	// writer object header
	w.buffer.Write(objectHeader)
	// writer object metadata
	w.buffer.Write(objectMeta)
	// writer block index
	w.buffer.Write(blockIndex)

	// writer block metadata
	for _, block := range w.blocks {
		w.buffer.Write(block.meta)
	}

	// writer data& bloom filter
	for _, block := range w.blocks {
		for _, data := range block.data {
			w.buffer.Write(data)
		}
	}

	// writer bloom filter
	w.buffer.Write(bloomFilterData)

	w.buffer.Write(zoneMapAreaData)

	// write footer
	footer := Footer{
		metaExtent: objectMetaLocation,
		version:    Version,
		magic:      Magic,
	}

	w.buffer.Write(footer.Marshal())
	if err != nil {
		return nil, err
	}
	blockObjects := make([]BlockObject, 0)
	for i := range w.blocks {
		header := w.blocks[i].meta.BlockHeader()
		header.SetMetaLocation(objectMeta.BlockHeader().MetaLocation())
		blockObjects = append(blockObjects, w.blocks[i].meta)
	}
	err = w.Sync(ctx, items...)
	if err != nil {
		return nil, err
	}

	// The buffer needs to be released at the end of WriteEnd
	// Because the outside may hold this writer
	// After WriteEnd is called, no more data can be written
	w.buffer = nil
	return blockObjects, err
}

// Sync is for testing
func (w *ObjectWriter) Sync(ctx context.Context, items ...WriteOptions) error {
	w.buffer.SetDataOptions(items...)
	// if a compact task is rollbacked, it may leave a written file in fs
	// here we just delete it and write again
	err := w.object.fs.Write(ctx, w.buffer.GetData())
	if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		if err = w.object.fs.Delete(ctx, w.fileName); err != nil {
			return err
		}
		return w.object.fs.Write(ctx, w.buffer.GetData())
	}
	return err
}

func (w *ObjectWriter) WriteWithCompress(offset uint32, buf []byte) (data []byte, extent Extent, err error) {
	dataLen := len(buf)
	data = make([]byte, lz4.CompressBlockBound(dataLen))
	if data, err = compress.Compress(buf, data, compress.Lz4); err != nil {
		return
	}
	extent = NewExtent(compress.Lz4, offset, uint32(len(data)), uint32(dataLen))
	return
}

func (w *ObjectWriter) AddBlock(blockMeta BlockObject, bat *batch.Batch) error {
	w.Lock()
	defer w.Unlock()
	// CHANGE ME
	// block.BlockHeader().SetBlockID(w.lastId)
	blockMeta.BlockHeader().SetSequence(uint16(w.lastId))

	block := blockData{meta: blockMeta}
	var data []byte
	for i, vec := range bat.Vecs {
		buf, err := vec.MarshalBinary()
		if err != nil {
			return err
		}
		var ext Extent
		if data, ext, err = w.WriteWithCompress(0, buf); err != nil {
			return err
		}
		block.data = append(block.data, data)
		blockMeta.ColumnMeta(uint16(i)).setLocation(ext)
		blockMeta.ColumnMeta(uint16(i)).setDataType(uint8(vec.GetType().Oid))
	}
	w.blocks = append(w.blocks, block)
	w.lastId++
	return nil
}

func (w *ObjectWriter) GetBlock(id uint32) BlockObject {
	w.Lock()
	defer w.Unlock()
	return w.blocks[id].meta
}
