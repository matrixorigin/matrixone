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

type objectWriterV1 struct {
	sync.RWMutex
	object      *Object
	blocks      []blockData
	totalRow    uint32
	colmeta     []ColumnMeta
	buffer      *ObjectBuffer
	fileName    string
	lastId      uint32
	name        ObjectName
	compressBuf []byte
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

func newObjectWriterSpecialV1(wt WriterType, fileName string, fs fileservice.FileService) (*objectWriterV1, error) {
	var name ObjectName
	object := NewObject(fileName, fs)
	switch wt {
	case WriterNormal:
		name = BuildNormalName()
	case WriterCheckpoint:
		name = BuildCheckpointName()
	case WriterQueryResult:
		name = BuildQueryResultName()
	case WriterGC:
		name = BuildDiskCleanerName()
	case WriterETL:
		name = BuildETLName()
	}
	writer := &objectWriterV1{
		fileName: fileName,
		name:     name,
		object:   object,
		buffer:   NewObjectBuffer(fileName),
		blocks:   make([]blockData, 0),
		lastId:   0,
	}
	return writer, nil
}

func newObjectWriterV1(name ObjectName, fs fileservice.FileService) (*objectWriterV1, error) {
	fileName := name.String()
	object := NewObject(fileName, fs)
	writer := &objectWriterV1{
		fileName: fileName,
		name:     name,
		object:   object,
		buffer:   NewObjectBuffer(fileName),
		blocks:   make([]blockData, 0),
		lastId:   0,
	}
	return writer, nil
}

func (w *objectWriterV1) Write(batch *batch.Batch) (BlockObject, error) {
	block := NewBlock(uint16(len(batch.Vecs)))
	w.AddBlock(block, batch)
	return block, nil
}

func (w *objectWriterV1) UpdateBlockZM(blkIdx, colIdx int, zm ZoneMap) {
	w.blocks[blkIdx].meta.ColumnMeta(uint16(colIdx)).SetZoneMap(zm)
}

func (w *objectWriterV1) WriteBF(blkIdx, colIdx int, buf []byte) (err error) {
	w.blocks[blkIdx].bloomFilter = buf
	return
}

func (w *objectWriterV1) WriteObjectMeta(ctx context.Context, totalrow uint32, metas []ColumnMeta) {
	w.totalRow = totalrow
	w.colmeta = metas
}

func (w *objectWriterV1) prepareObjectMeta(objectMeta ObjectMeta, offset uint32) ([]byte, Extent, error) {
	length := uint32(0)
	blockCount := uint32(len(w.blocks))
	objectMeta.BlockHeader().SetSequence(uint16(blockCount))
	sid := w.name.SegmentId()
	blockId := NewBlockid(&sid, w.name.Num(), uint16(blockCount))
	objectMeta.BlockHeader().SetBlockID(&blockId)
	objectMeta.BlockHeader().SetRows(w.totalRow)
	// write column meta
	for i, colMeta := range w.colmeta {
		objectMeta.AddColumnMeta(uint16(i), colMeta)
	}
	length += objectMeta.Length()
	blockIndex := BuildBlockIndex(blockCount)
	blockIndex.SetBlockCount(blockCount)
	length += blockIndex.Length()
	blockIndex.SetBlockCount(blockCount)
	for i, block := range w.blocks {
		n := uint32(len(block.meta))
		blockIndex.SetBlockMetaPos(uint32(i), length, n)
		length += n
	}
	extent := NewExtent(compress.None, offset, 0, length)
	objectMeta.BlockHeader().SetMetaLocation(extent)

	var buf bytes.Buffer
	h := IOEntryHeader{IOET_ObjMeta, IOET_ObjectMeta_CurrVer}
	buf.Write(EncodeIOEntryHeader(&h))
	buf.Write(objectMeta)
	buf.Write(blockIndex)
	// writer block metadata
	for _, block := range w.blocks {
		buf.Write(block.meta)
	}
	return w.WriteWithCompress(offset, buf.Bytes())
}

func (w *objectWriterV1) prepareBlockMeta(offset uint32) uint32 {
	maxIndex := w.getMaxIndex()
	for idx := uint16(0); idx < maxIndex; idx++ {
		for i, block := range w.blocks {
			if block.meta.BlockHeader().ColumnCount() <= idx {
				continue
			}
			location := w.blocks[i].meta.ColumnMeta(uint16(idx)).Location()
			location.SetOffset(offset)
			w.blocks[i].meta.ColumnMeta(uint16(idx)).setLocation(location)
			offset += location.Length()
		}
	}
	return offset
}

func (w *objectWriterV1) prepareBloomFilter(blockCount uint32, offset uint32) ([]byte, Extent, error) {
	buf := new(bytes.Buffer)
	h := IOEntryHeader{IOET_BF, IOET_BloomFilter_CurrVer}
	buf.Write(EncodeIOEntryHeader(&h))
	bloomFilterStart := uint32(0)
	bloomFilterIndex := BuildBlockIndex(blockCount)
	bloomFilterIndex.SetBlockCount(blockCount)
	bloomFilterStart += bloomFilterIndex.Length()
	for i, block := range w.blocks {
		n := uint32(len(block.bloomFilter))
		bloomFilterIndex.SetBlockMetaPos(uint32(i), bloomFilterStart, n)
		bloomFilterStart += n
	}
	buf.Write(bloomFilterIndex)
	for _, block := range w.blocks {
		buf.Write(block.bloomFilter)
	}
	return w.WriteWithCompress(offset, buf.Bytes())
}

func (w *objectWriterV1) prepareZoneMapArea(blockCount uint32, offset uint32) ([]byte, Extent, error) {
	buf := new(bytes.Buffer)
	h := IOEntryHeader{IOET_ZM, IOET_ZoneMap_CurrVer}
	buf.Write(EncodeIOEntryHeader(&h))
	zoneMapAreaStart := uint32(0)
	zoneMapAreaIndex := BuildBlockIndex(blockCount)
	zoneMapAreaIndex.SetBlockCount(blockCount)
	zoneMapAreaStart += zoneMapAreaIndex.Length()
	for i, block := range w.blocks {
		n := uint32(block.meta.GetColumnCount() * ZoneMapSize)
		zoneMapAreaIndex.SetBlockMetaPos(uint32(i), zoneMapAreaStart, n)
		zoneMapAreaStart += n
	}
	buf.Write(zoneMapAreaIndex)
	for _, block := range w.blocks {
		for i := range block.data {
			buf.Write(block.meta.ColumnMeta(uint16(i)).ZoneMap())
		}
	}
	return w.WriteWithCompress(offset, buf.Bytes())
}

func (w *objectWriterV1) getMaxIndex() uint16 {
	if len(w.blocks) == 0 {
		return 0
	}
	maxIndex := len(w.blocks[0].data)
	for _, block := range w.blocks {
		idxes := len(block.data)
		if idxes > maxIndex {
			maxIndex = idxes
		}
	}
	return uint16(maxIndex)
}

func (w *objectWriterV1) writerBlocks() {
	maxIndex := w.getMaxIndex()
	for idx := uint16(0); idx < maxIndex; idx++ {
		for _, block := range w.blocks {
			if block.meta.BlockHeader().ColumnCount() <= idx {
				continue
			}
			w.buffer.Write(block.data[idx])
		}
	}
}

func (w *objectWriterV1) WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error) {
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
	objectMeta.BlockHeader().SetColumnCount(columnCount)

	offset := w.prepareBlockMeta(HeaderSize)

	// prepare bloom filter
	bloomFilterData, bloomFilterExtent, err := w.prepareBloomFilter(blockCount, offset)
	if err != nil {
		return nil, err
	}
	objectMeta.BlockHeader().SetBFExtent(bloomFilterExtent)
	offset += bloomFilterExtent.Length()

	// prepare zone map area
	zoneMapAreaData, zoneMapAreaExtent, err := w.prepareZoneMapArea(blockCount, offset)
	if err != nil {
		return nil, err
	}
	objectMeta.BlockHeader().SetZoneMapArea(zoneMapAreaExtent)
	offset += zoneMapAreaExtent.Length()

	// prepare object meta and block index
	meta, metaExtent, err := w.prepareObjectMeta(objectMeta, offset)
	objectHeader.SetExtent(metaExtent)
	// begin write

	// writer object header
	w.buffer.Write(objectHeader)

	// writer data
	w.writerBlocks()

	// writer bloom filter
	w.buffer.Write(bloomFilterData)

	w.buffer.Write(zoneMapAreaData)

	// writer object metadata
	w.buffer.Write(meta)

	// write footer
	footer := Footer{
		metaExtent: metaExtent,
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
		header.SetMetaLocation(objectHeader.Extent())
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
func (w *objectWriterV1) Sync(ctx context.Context, items ...WriteOptions) error {
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

func (w *objectWriterV1) WriteWithCompress(offset uint32, buf []byte) (data []byte, extent Extent, err error) {
	var tmpData []byte
	dataLen := len(buf)
	compressBlockBound := lz4.CompressBlockBound(dataLen)
	if len(w.compressBuf) < compressBlockBound {
		w.compressBuf = make([]byte, compressBlockBound)
	}
	if tmpData, err = compress.Compress(buf, w.compressBuf[:compressBlockBound], compress.Lz4); err != nil {
		return
	}
	length := uint32(len(tmpData))
	data = make([]byte, length)
	copy(data, tmpData[:length])
	extent = NewExtent(compress.Lz4, offset, length, uint32(dataLen))
	return
}

func (w *objectWriterV1) AddBlock(blockMeta BlockObject, bat *batch.Batch) error {
	w.Lock()
	defer w.Unlock()
	// CHANGE ME
	// block.BlockHeader().SetBlockID(w.lastId)
	blockMeta.BlockHeader().SetSequence(uint16(w.lastId))

	block := blockData{meta: blockMeta}
	var data []byte
	var buf bytes.Buffer
	for i, vec := range bat.Vecs {
		buf.Reset()
		h := IOEntryHeader{IOET_ColData, IOET_ColumnData_CurrVer}
		buf.Write(EncodeIOEntryHeader(&h))
		err := vec.MarshalBinaryWithBuffer(&buf)
		if err != nil {
			return err
		}
		var ext Extent
		if data, ext, err = w.WriteWithCompress(0, buf.Bytes()); err != nil {
			return err
		}
		block.data = append(block.data, data)
		blockMeta.ColumnMeta(uint16(i)).setLocation(ext)
		blockMeta.ColumnMeta(uint16(i)).setDataType(uint8(vec.GetType().Oid))
		blockMeta.ColumnMeta(uint16(i)).SetNullCnt(uint32(vec.GetNulls().GetCardinality()))
	}
	w.blocks = append(w.blocks, block)
	w.lastId++
	return nil
}

func (w *objectWriterV1) GetBlock(id uint32) BlockObject {
	w.Lock()
	defer w.Unlock()
	return w.blocks[id].meta
}
