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

	// CHANGE ME
	objectMeta.BlockHeader().SetSequence(uint16(blockCount))
	sid := w.name.SegmentId()
	blockid := NewBlockid(&sid, w.name.Num(), uint16(blockCount))
	objectMeta.BlockHeader().SetBlockID(&blockid)

	objectMeta.BlockHeader().SetRows(w.totalRow)
	objectMeta.BlockHeader().SetColumnCount(columnCount)
	blockIndex := BuildBlockIndex(blockCount)
	blockIndex.SetBlockCount(blockCount)
	start := objectMeta.Length()
	start += blockIndex.Length()
	length := 0

	// write block meta
	for i, block := range w.blocks {
		n := uint32(len(block.meta))
		blockIndex.SetBlockMetaPos(uint32(i), start, n)
		start += n
	}

	// write column meta
	for i, colmeta := range w.colmeta {
		objectMeta.AddColumnMeta(uint16(i), colmeta)
	}
	extent := NewExtent(0, HeaderSize, start, start)
	objectMeta.BlockHeader().SetMetaLocation(extent)
	objectHeader.SetLocation(extent)

	for y, block := range w.blocks {
		for i := range block.data {
			location := w.blocks[y].meta.ColumnMeta(uint16(i)).Location()
			logutil.Infof("location : %v", location.String())
			location.SetOffset(start + HeaderSize)
			w.blocks[y].meta.ColumnMeta(uint16(i)).setLocation(location)
			location = w.blocks[y].meta.ColumnMeta(uint16(i)).Location()
			logutil.Infof("location2 : %v", location.String())
			start += location.Length()
		}
	}
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
	dataLen := len(bloomFilter.Bytes())
	bloomFilterData := make([]byte, lz4.CompressBlockBound(dataLen))
	if bloomFilterData, err = compress.Compress(bloomFilter.Bytes(), bloomFilterData, compress.Lz4); err != nil {
		return nil, err
	}
	bloomFilterExtent := NewExtent(compress.Lz4, start+HeaderSize, uint32(len(bloomFilterData)), uint32(dataLen))
	objectMeta.BlockHeader().SetBloomFilter(bloomFilterExtent)
	logutil.Infof("blockid %v, eeeeeee %v", bloomFilterExtent.String(), blockid.String())
	logutil.Infof("22222222 %v", objectMeta.BlockHeader().BloomFilter().String())

	if w.name.Num() == 13 {
		logutil.Infof("blockid1111 %v, eeeeeee %v", bloomFilterExtent.String(), blockid.String())
	}
	// begin write

	// writer object header
	_, n, err := w.buffer.Write(objectHeader)
	if err != nil {
		return nil, err
	}
	// writer object metadata
	_, n, err = w.buffer.Write(objectMeta)
	if err != nil {
		return nil, err
	}
	length += n

	// writer block index
	_, n, err = w.buffer.Write(blockIndex)
	if err != nil {
		return nil, err
	}
	length += n

	// writer block metadata
	for _, block := range w.blocks {
		_, n, err = w.buffer.Write(block.meta)
		if err != nil {
			return nil, err
		}

	}

	// writer data& bloom filter
	for _, block := range w.blocks {
		for _, data := range block.data {
			_, n, err = w.buffer.Write(data)
			if err != nil {
				return nil, err
			}
		}

	}

	// writer bloom filter
	_, n, err = w.buffer.Write(bloomFilterData)
	if err != nil {
		return nil, err
	}
	length += n

	// write footer
	footer := Footer{
		metaExtent: extent,
		version:    Version,
		magic:      Magic,
	}

	if _, _, err = w.buffer.Write(footer.Marshal()); err != nil {
		return nil, err
	}
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

func (b *blockData) WriteWithCompress(buf []byte) (extent Extent, err error) {
	dataLen := len(buf)
	data := make([]byte, lz4.CompressBlockBound(dataLen))
	if data, err = compress.Compress(buf, data, compress.Lz4); err != nil {
		return
	}
	b.data = append(b.data, data)
	extent = NewExtent(compress.Lz4, 0, uint32(len(data)), uint32(dataLen))
	return
}

func (w *ObjectWriter) AddBlock(block BlockObject, bat *batch.Batch) error {
	w.Lock()
	defer w.Unlock()
	// CHANGE ME
	// block.BlockHeader().SetBlockID(w.lastId)
	block.BlockHeader().SetSequence(uint16(w.lastId))

	data := blockData{meta: block}
	for i, vec := range bat.Vecs {
		buf, err := vec.MarshalBinary()
		if err != nil {
			return err
		}
		var ext Extent
		if ext, err = data.WriteWithCompress(buf); err != nil {
			return err
		}
		block.ColumnMeta(uint16(i)).setLocation(ext)
		block.ColumnMeta(uint16(i)).setDataType(uint8(vec.GetType().Oid))
	}
	w.blocks = append(w.blocks, data)
	w.lastId++
	return nil
}

func (w *ObjectWriter) GetBlock(id uint32) BlockObject {
	w.Lock()
	defer w.Unlock()
	return w.blocks[id].meta
}
