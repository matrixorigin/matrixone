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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/pierrec/lz4/v4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type objectWriterV1 struct {
	sync.RWMutex
	schemaVer         uint32
	seqnums           *Seqnums
	object            *Object
	blocks            []blockData
	tombstones        []blockData
	tombstonesColmeta []ColumnMeta
	totalRow          uint32
	colmeta           []ColumnMeta
	buffer            *ObjectBuffer
	fileName          string
	lastId            uint32
	name              ObjectName
	compressBuf       []byte
	bloomFilter       []byte
}

type blockData struct {
	meta        BlockObject
	seqnums     *Seqnums
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
		seqnums:    NewSeqnums(nil),
		fileName:   fileName,
		name:       name,
		object:     object,
		buffer:     NewObjectBuffer(fileName),
		blocks:     make([]blockData, 0),
		tombstones: make([]blockData, 0),
		lastId:     0,
	}
	return writer, nil
}

func newObjectWriterV1(name ObjectName, fs fileservice.FileService, schemaVersion uint32, seqnums []uint16) (*objectWriterV1, error) {
	fileName := name.String()
	object := NewObject(fileName, fs)
	writer := &objectWriterV1{
		schemaVer:  schemaVersion,
		seqnums:    NewSeqnums(seqnums),
		fileName:   fileName,
		name:       name,
		object:     object,
		buffer:     NewObjectBuffer(fileName),
		blocks:     make([]blockData, 0),
		tombstones: make([]blockData, 0),
		lastId:     0,
	}
	return writer, nil
}

func (w *objectWriterV1) GetSeqnums() []uint16 {
	return w.seqnums.Seqs
}

func (w *objectWriterV1) GetMaxSeqnum() uint16 {
	return w.seqnums.MaxSeq
}

func (w *objectWriterV1) Write(batch *batch.Batch) (BlockObject, error) {
	if col := len(w.seqnums.Seqs); col == 0 {
		w.seqnums.InitWithColCnt(len(batch.Vecs))
	} else if col != len(batch.Vecs) {
		panic(fmt.Sprintf("Unmatched Write Batch, expect %d, get %d, %v", col, len(batch.Vecs), batch.Attrs))
	}
	block := NewBlock(w.seqnums)
	w.AddBlock(block, batch, w.seqnums)
	return block, nil
}

func (w *objectWriterV1) WriteTombstone(batch *batch.Batch) (BlockObject, error) {
	denseSeqnums := NewSeqnums(nil)
	denseSeqnums.InitWithColCnt(len(batch.Vecs))
	block := NewBlock(denseSeqnums)
	w.AddTombstone(block, batch, denseSeqnums)
	return block, nil
}

func (w *objectWriterV1) WriteWithoutSeqnum(batch *batch.Batch) (BlockObject, error) {
	denseSeqnums := NewSeqnums(nil)
	denseSeqnums.InitWithColCnt(len(batch.Vecs))
	block := NewBlock(denseSeqnums)
	w.AddBlock(block, batch, denseSeqnums)
	return block, nil
}

func (w *objectWriterV1) UpdateBlockZM(blkIdx int, seqnum uint16, zm ZoneMap) {
	w.blocks[blkIdx].meta.ColumnMeta(seqnum).SetZoneMap(zm)
}

func (w *objectWriterV1) WriteBF(blkIdx int, seqnum uint16, buf []byte) (err error) {
	w.blocks[blkIdx].bloomFilter = buf
	return
}

func (w *objectWriterV1) WriteObjectMetaBF(buf []byte) (err error) {
	w.bloomFilter = buf
	return
}

func (w *objectWriterV1) WriteObjectMeta(ctx context.Context, totalrow uint32, metas []ColumnMeta) {
	w.totalRow = totalrow
	w.colmeta = metas
}

func (w *objectWriterV1) prepareDataMeta(objectMeta objectMetaV1, blocks []blockData, offset uint32, offsetId uint16) ([]byte, Extent, error) {
	var columnCount uint16
	columnCount = 0
	metaColCnt := uint16(0)
	maxSeqnum := uint16(0)
	var seqnums *Seqnums
	if len(blocks) == 0 {
		logutil.Warn("object io: no block needs to be written")
	} else {
		columnCount = blocks[0].meta.GetColumnCount()
		metaColCnt = blocks[0].meta.GetMetaColumnCount()
		maxSeqnum = blocks[0].meta.GetMaxSeqnum()
		seqnums = blocks[0].seqnums
	}
	objectMeta.BlockHeader().SetColumnCount(columnCount)
	objectMeta.BlockHeader().SetMetaColumnCount(metaColCnt)
	objectMeta.BlockHeader().SetMaxSeqnum(maxSeqnum)
	logutil.Infof("tObjectMeta is %d", objectMeta.BlockHeader().Sequence())

	// prepare object meta and block index
	meta, extent, err := w.prepareObjectMeta(blocks, objectMeta, offset, seqnums, offsetId)
	logutil.Infof("tObjectMeta11 is %d, offsetId is %d", objectMeta.BlockHeader().Sequence(), offsetId)
	if err != nil {
		return nil, nil, err
	}
	return meta, extent, err
}

func (w *objectWriterV1) prepareObjectMeta(blocks []blockData, objectMeta objectMetaV1, offset uint32, seqnums *Seqnums, offsetId uint16) ([]byte, Extent, error) {
	length := uint32(0)
	blockCount := uint32(len(blocks))
	sid := w.name.SegmentId()
	objectMeta.BlockHeader().SetSequence(uint16(blockCount))
	blockId := NewBlockid(&sid, w.name.Num(), uint16(blockCount))
	objectMeta.BlockHeader().SetBlockID(blockId)
	objectMeta.BlockHeader().SetStartID(offsetId)
	objectMeta.BlockHeader().SetRows(w.totalRow)
	// write column meta
	if seqnums != nil && len(seqnums.Seqs) > 0 {
		for i, colMeta := range w.colmeta {
			if i >= len(seqnums.Seqs) {
				break
			}
			objectMeta.AddColumnMeta(seqnums.Seqs[i], colMeta)
		}
	}
	length += objectMeta.Length()
	blockIndex := BuildBlockIndex(blockCount)
	blockIndex.SetBlockCount(blockCount)
	length += blockIndex.Length()
	for i, block := range blocks {
		n := uint32(len(block.meta))
		blockIndex.SetBlockMetaPos(uint32(i), length, n)
		length += n
	}
	extent := NewExtent(compress.None, offset, 0, length)
	objectMeta.BlockHeader().SetMetaLocation(extent)

	var buf bytes.Buffer
	buf.Write(objectMeta)
	buf.Write(blockIndex)
	// writer block metadata
	for _, block := range blocks {
		buf.Write(block.meta)
	}
	return buf.Bytes(), extent, nil
}

func (w *objectWriterV1) prepareBlockMeta(offset uint32, blocks []blockData, colmeta []ColumnMeta) uint32 {
	logutil.Infof("object io: prepare block meta, offset %d, blocks %d, colmeta %d", offset, len(blocks), len(colmeta))
	maxIndex := w.getMaxIndex(blocks)
	var off, size, oSize uint32
	for idx := uint16(0); idx < maxIndex; idx++ {
		off = offset
		size = 0
		oSize = 0
		alg := uint8(0)
		for i, block := range blocks {
			if block.meta.BlockHeader().ColumnCount() <= idx {
				continue
			}
			blk := blocks[i]
			location := blk.meta.ColumnMeta(blk.seqnums.Seqs[idx]).Location()
			location.SetOffset(offset)
			blk.meta.ColumnMeta(blk.seqnums.Seqs[idx]).setLocation(location)
			offset += location.Length()
			size += location.Length()
			oSize += location.OriginSize()
			alg = location.Alg()
		}
		if uint16(len(colmeta)) <= idx {
			continue
		}
		colmeta[idx].Location().SetAlg(alg)
		colmeta[idx].Location().SetOffset(off)
		colmeta[idx].Location().SetLength(size)
		colmeta[idx].Location().SetOriginSize(oSize)
	}
	return offset
}

func (w *objectWriterV1) prepareBloomFilter(blocks []blockData, blockCount uint32, offset uint32) ([]byte, Extent, error) {
	buf := new(bytes.Buffer)
	h := IOEntryHeader{IOET_BF, IOET_BloomFilter_CurrVer}
	buf.Write(EncodeIOEntryHeader(&h))
	bloomFilterStart := uint32(0)
	bloomFilterIndex := BuildBlockIndex(blockCount + 1)
	bloomFilterIndex.SetBlockCount(blockCount + 1)
	bloomFilterStart += bloomFilterIndex.Length()
	for i, block := range blocks {
		n := uint32(len(block.bloomFilter))
		bloomFilterIndex.SetBlockMetaPos(uint32(i), bloomFilterStart, n)
		bloomFilterStart += n
	}
	bloomFilterIndex.SetBlockMetaPos(blockCount, bloomFilterStart, uint32(len(w.bloomFilter)))
	buf.Write(bloomFilterIndex)
	for _, block := range blocks {
		buf.Write(block.bloomFilter)
	}
	buf.Write(w.bloomFilter)
	length := uint32(len(buf.Bytes()))
	extent := NewExtent(compress.None, offset, length, length)
	return buf.Bytes(), extent, nil
}

func (w *objectWriterV1) prepareZoneMapArea(blocks []blockData, blockCount uint32, offset uint32) ([]byte, Extent, error) {
	buf := new(bytes.Buffer)
	h := IOEntryHeader{IOET_ZM, IOET_ZoneMap_CurrVer}
	buf.Write(EncodeIOEntryHeader(&h))
	zoneMapAreaStart := uint32(0)
	zoneMapAreaIndex := BuildBlockIndex(blockCount)
	zoneMapAreaIndex.SetBlockCount(blockCount)
	zoneMapAreaStart += zoneMapAreaIndex.Length()
	for i, block := range blocks {
		n := uint32(block.meta.GetMetaColumnCount() * ZoneMapSize)
		zoneMapAreaIndex.SetBlockMetaPos(uint32(i), zoneMapAreaStart, n)
		zoneMapAreaStart += n
	}
	buf.Write(zoneMapAreaIndex)
	for _, block := range blocks {
		for seqnum := uint16(0); seqnum < block.meta.GetMetaColumnCount(); seqnum++ {
			buf.Write(block.meta.ColumnMeta(seqnum).ZoneMap())
		}
	}
	return w.WriteWithCompress(offset, buf.Bytes())
}

func (w *objectWriterV1) getMaxIndex(blocks []blockData) uint16 {
	if len(blocks) == 0 {
		return 0
	}
	maxIndex := len(blocks[0].data)
	for _, block := range blocks {
		idxes := len(block.data)
		if idxes > maxIndex {
			maxIndex = idxes
		}
	}
	return uint16(maxIndex)
}

func (w *objectWriterV1) writerBlocks(blocks []blockData) {
	maxIndex := w.getMaxIndex(blocks)
	for idx := uint16(0); idx < maxIndex; idx++ {
		for _, block := range blocks {
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

	objectHeader := BuildHeader()
	objectHeader.SetSchemaVersion(w.schemaVer)

	offset := w.prepareBlockMeta(HeaderSize, w.blocks, w.colmeta)
	offset = w.prepareBlockMeta(offset, w.tombstones, w.tombstonesColmeta)
	metaHeader := buildMetaHeaderV1()
	colMetaCount := uint16(0)
	if len(w.blocks) > 0 {
		colMetaCount = w.blocks[0].meta.GetMetaColumnCount()
	}
	objectMeta := BuildObjectMeta(colMetaCount)
	if len(w.tombstones) > 0 {
		colMetaCount = w.tombstones[0].meta.GetMetaColumnCount()
	}
	tObjectMeta := BuildObjectMeta(colMetaCount)
	// prepare bloom filter
	bloomFilterData, bloomFilterExtent, err := w.prepareBloomFilter(w.blocks, uint32(len(w.blocks)), offset)
	if err != nil {
		return nil, err
	}
	objectMeta.BlockHeader().SetBFExtent(bloomFilterExtent)
	offset += bloomFilterExtent.Length()

	// prepare zone map area
	zoneMapAreaData, zoneMapAreaExtent, err := w.prepareZoneMapArea(w.blocks, uint32(len(w.blocks)), offset)
	if err != nil {
		return nil, err
	}
	objectMeta.BlockHeader().SetZoneMapArea(zoneMapAreaExtent)
	offset += zoneMapAreaExtent.Length()
	tbloomFilterData, tbloomFilterExtent, err := w.prepareBloomFilter(w.tombstones, uint32(len(w.tombstones)), offset)
	if err != nil {
		return nil, err
	}
	tObjectMeta.BlockHeader().SetBFExtent(tbloomFilterExtent)
	offset += tbloomFilterExtent.Length()

	// prepare zone map area
	tzoneMapAreaData, tzoneMapAreaExtent, err := w.prepareZoneMapArea(w.tombstones, uint32(len(w.tombstones)), offset)
	if err != nil {
		return nil, err
	}
	tObjectMeta.BlockHeader().SetZoneMapArea(tzoneMapAreaExtent)
	offset += tzoneMapAreaExtent.Length()
	// prepare object meta and block index
	meta, metaExtent, err := w.prepareDataMeta(objectMeta, w.blocks, offset, 0)
	start := metaExtent.Offset()

	metaHeader.SetDataMetaCount(uint16(len(w.blocks)))
	metaHeader.SetDataMetaOffset(metaHeader.Length())
	tmeta, _, err := w.prepareDataMeta(tObjectMeta, w.tombstones, metaExtent.End(), metaHeader.DataMetaCount())
	logutil.Infof("tObjectMeta.DataMetaCount()) is %d", tObjectMeta.BlockHeader().Sequence())
	metaHeader.SetTombstoneMetaCount(uint16(len(w.tombstones)))
	metaHeader.SetTombstoneMetaOffset(metaHeader.Length() + metaExtent.OriginSize())
	var buf bytes.Buffer
	h := IOEntryHeader{IOET_ObjMeta, IOET_ObjectMeta_CurrVer}
	buf.Write(EncodeIOEntryHeader(&h))
	buf.Write(metaHeader)
	buf.Write(meta)
	buf.Write(tmeta)
	oMeta, extent, err := w.WriteWithCompress(start, buf.Bytes())
	objectHeader.SetExtent(extent)

	// begin write

	// writer object header
	w.buffer.Write(objectHeader)

	// writer data
	w.writerBlocks(w.blocks)
	// writer data
	w.writerBlocks(w.tombstones)
	// writer bloom filter
	w.buffer.Write(bloomFilterData)

	w.buffer.Write(zoneMapAreaData)
	w.buffer.Write(tbloomFilterData)

	w.buffer.Write(tzoneMapAreaData)
	// writer object metadata
	w.buffer.Write(oMeta)

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
	for i := range w.tombstones {
		header := w.tombstones[i].meta.BlockHeader()
		header.SetMetaLocation(objectHeader.Extent())
		blockObjects = append(blockObjects, w.tombstones[i].meta)
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

func (w *objectWriterV1) AddBlock(blockMeta BlockObject, bat *batch.Batch, seqnums *Seqnums) error {
	w.Lock()
	defer w.Unlock()
	// CHANGE ME
	// block.BlockHeader()return w.WriteWithCompress(offset, buf.Bytes()).SetBlockID(w.lastId)
	blockMeta.BlockHeader().SetSequence(uint16(w.lastId))

	block := blockData{meta: blockMeta, seqnums: seqnums}
	var data []byte
	var buf bytes.Buffer
	var rows int
	warned := 0
	for i, vec := range bat.Vecs {
		if i == 0 {
			rows = vec.Length()
		} else if rows != vec.Length() {
			warned++
			logutil.Warnf("%s unmatched length, expect %d, get %d", bat.Attrs[i], rows, vec.Length())
		}
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
		blockMeta.ColumnMeta(seqnums.Seqs[i]).setLocation(ext)
		blockMeta.ColumnMeta(seqnums.Seqs[i]).setDataType(uint8(vec.GetType().Oid))
		if vec.GetType().Oid == types.T_any {
			panic("any type batch")
		}
		blockMeta.ColumnMeta(seqnums.Seqs[i]).SetNullCnt(uint32(vec.GetNulls().GetCardinality()))
	}
	if warned > 0 {
		logutil.Warnf("warned %d times", warned)
	}
	blockMeta.BlockHeader().SetRows(uint32(rows))
	w.blocks = append(w.blocks, block)
	w.lastId++
	return nil
}

func (w *objectWriterV1) AddTombstone(blockMeta BlockObject, bat *batch.Batch, seqnums *Seqnums) error {
	w.Lock()
	defer w.Unlock()
	if w.tombstonesColmeta == nil {
		w.tombstonesColmeta = make([]ColumnMeta, len(bat.Vecs))
	}
	for i := range w.tombstonesColmeta {
		w.tombstonesColmeta[i] = BuildObjectColumnMeta()
	}
	blockMeta.BlockHeader().SetSequence(uint16(w.lastId))

	block := blockData{meta: blockMeta, seqnums: seqnums}
	var data []byte
	var buf bytes.Buffer
	var rows int
	warned := 0
	for i, vec := range bat.Vecs {
		if i == 0 {
			rows = vec.Length()
		} else if rows != vec.Length() {
			warned++
			logutil.Warnf("%s unmatched length, expect %d, get %d", bat.Attrs[i], rows, vec.Length())
		}
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
		blockMeta.ColumnMeta(seqnums.Seqs[i]).setLocation(ext)
		logutil.Infof("uint8(vec.GetType().Oid) is %d", uint8(vec.GetType().Oid))
		blockMeta.ColumnMeta(seqnums.Seqs[i]).setDataType(uint8(vec.GetType().Oid))
		if vec.GetType().Oid == types.T_any {
			panic("any type batch")
		}
	}
	if warned > 0 {
		logutil.Warnf("warned %d times", warned)
	}
	blockMeta.BlockHeader().SetRows(uint32(rows))
	w.tombstones = append(w.tombstones, block)
	w.lastId++
	return nil
}

func (w *objectWriterV1) GetBlock(id uint32) BlockObject {
	w.Lock()
	defer w.Unlock()
	return w.blocks[id].meta
}
