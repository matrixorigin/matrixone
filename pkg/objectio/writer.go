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
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap"
)

// arenaMaxSize caps WriteArena backing-array growth to bound memory use
// for unusually large block write cycles.
const arenaMaxSize = 128 * 1024 * 1024

// arenaMPool is a dedicated off-heap allocator for WriteArena buffers.
// Using mpool with offHeap=true routes through C.calloc/C.free, keeping
// the backing arrays out of Go's heap and invisible to pprof inuse_space.
var arenaMPool = mpool.MustNew("write-arena")

type WriteArena struct {
	data           []byte
	usedOffset     int
	compressBuf    []byte
	serialBuf      bytes.Buffer // reused for column serialization across write cycles
	totalRequested int          // sum of all Alloc sizes in the current cycle
	sizeLimit      int          // max data growth; 0 means arenaMaxSize
}

func NewArena(size int) *WriteArena {
	data, err := arenaMPool.Alloc(size, true)
	if err != nil {
		panic(err)
	}
	return &WriteArena{
		data: data,
	}
}

// Alloc returns a slice of exactly size bytes.  When the arena has
// insufficient space it falls back to a plain make so callers are
// never blocked.  totalRequested is always updated so that Reset can
// grow the backing array for the next cycle, eliminating the fallback
// for future rounds of similar demand.
func (a *WriteArena) Alloc(size int) []byte {
	a.totalRequested += size
	if a.usedOffset+size > len(a.data) {
		return make([]byte, size)
	}
	offset := a.usedOffset
	a.usedOffset += size
	return a.data[offset:a.usedOffset]
}

// Reset prepares the arena for a new write cycle.  If the previous
// cycle overflowed (totalRequested > len(data)) and the required
// capacity is within sizeLimit, the backing array is grown to a
// power-of-two capacity large enough to hold an equivalent cycle
// without any fallback allocations.
func (a *WriteArena) Reset() {
	limit := a.sizeLimit
	if limit <= 0 {
		limit = arenaMaxSize
	}
	if needed := a.totalRequested; needed > len(a.data) && needed <= limit {
		newCap := arenaNextPow2(needed)
		if newCap > limit {
			newCap = limit
		}
		arenaMPool.Free(a.data)
		a.data = nil // nil immediately so a panic in Alloc doesn't leave a dangling pointer
		var err error
		a.data, err = arenaMPool.Alloc(newCap, true)
		if err != nil {
			panic(err)
		}
	}
	a.usedOffset = 0
	a.totalRequested = 0
}

func arenaNextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// CompressBuf returns a scratch buffer for LZ4 compression, growing as needed.
// The buffer is retained across arena Reset() calls.  We round up to the next
// power of 2 so that minor size variations don't trigger repeated allocations.
func (a *WriteArena) CompressBuf(minSize int) []byte {
	if len(a.compressBuf) < minSize {
		if a.compressBuf != nil {
			arenaMPool.Free(a.compressBuf)
			a.compressBuf = nil // nil immediately so a panic in Alloc doesn't leave a dangling pointer
		}
		var err error
		a.compressBuf, err = arenaMPool.Alloc(arenaNextPow2(minSize), true)
		if err != nil {
			panic(err)
		}
	}
	return a.compressBuf[:minSize]
}

// FreeBuffers releases the off-heap data and compressBuf allocations.
// Call this before dropping an arena that won't be returned to the pool.
func (a *WriteArena) FreeBuffers() {
	if a.data != nil {
		arenaMPool.Free(a.data)
		a.data = nil
	}
	if a.compressBuf != nil {
		arenaMPool.Free(a.compressBuf)
		a.compressBuf = nil
	}
}

type objectWriterV1 struct {
	sync.RWMutex
	arena             *WriteArena
	lz4c              lz4.Compressor
	schemaVer         uint32
	seqnums           *Seqnums
	object            *Object
	blocks            [][]blockData
	tombstonesColmeta []ColumnMeta
	totalRow          uint32
	colmeta           []ColumnMeta
	buffer            *ObjectBuffer
	fileName          string
	lastId            uint32
	name              ObjectName
	compressBuf       []byte
	buf               bytes.Buffer
	bloomFilter       []byte
	objStats          ObjectStats
	sortKeySeqnum     uint16
	appendable        bool
	originSize        uint32
	size              uint32
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
	WriterTmp
	WriterDumpTable
)

// make it mutable in ut
var ObjectSizeLimit = 3 * mpool.GB

// SetObjectSizeLimit set ObjectSizeLimit to limit Bytes
func SetObjectSizeLimit(limit int) {
	ObjectSizeLimit = limit
}

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
	case WriterTmp:
		name = BuildTmpName()
	case WriterDumpTable:
		name = BuildDumpTableName()
	}
	writer := &objectWriterV1{
		seqnums:       NewSeqnums(nil),
		fileName:      fileName,
		name:          name,
		object:        object,
		buffer:        NewObjectBuffer(fileName),
		blocks:        make([][]blockData, 2),
		lastId:        0,
		sortKeySeqnum: math.MaxUint16,
	}
	writer.blocks[SchemaData] = make([]blockData, 0)
	writer.blocks[SchemaTombstone] = make([]blockData, 0)
	return writer, nil
}

func newObjectWriterV1(name ObjectName, fs fileservice.FileService, schemaVersion uint32, seqnums []uint16, arena *WriteArena) (*objectWriterV1, error) {
	fileName := name.String()
	object := NewObject(fileName, fs)
	writer := &objectWriterV1{
		arena:         arena,
		schemaVer:     schemaVersion,
		seqnums:       NewSeqnums(seqnums),
		fileName:      fileName,
		name:          name,
		object:        object,
		buffer:        NewObjectBuffer(fileName),
		blocks:        make([][]blockData, 2),
		lastId:        0,
		sortKeySeqnum: math.MaxUint16,
	}
	writer.blocks[SchemaData] = make([]blockData, 0)
	writer.blocks[SchemaTombstone] = make([]blockData, 0)
	return writer, nil
}

func (w *objectWriterV1) GetObjectStats(opts ...ObjectStatsOptions) (copied ObjectStats) {
	copied = w.objStats

	for _, opt := range opts {
		opt(&copied)
	}

	return copied
}

func describeObjectHelper(w *objectWriterV1, colmeta []ColumnMeta, idx DataMetaType) ObjectStats {
	ss := NewObjectStats()
	SetObjectStatsObjectName(ss, w.name)
	SetObjectStatsExtent(ss, Header(w.buffer.vector.Entries[0].Data).Extent())
	SetObjectStatsRowCnt(ss, w.totalRow)
	SetObjectStatsBlkCnt(ss, uint32(len(w.blocks[idx])))

	if len(colmeta) > int(w.sortKeySeqnum) {
		SetObjectStatsSortKeyZoneMap(ss, colmeta[w.sortKeySeqnum].ZoneMap())
	}
	SetObjectStatsSize(ss, w.size)
	SetObjectStatsOriginSize(ss, w.originSize)

	return *ss
}

// DescribeObject generates two object stats:
//
// 0: data object stats
//
// 1: tombstone object stats
//
// if an object only has inserts, only the data object stats valid.
//
// if an object only has deletes, only the tombstone object stats valid.
//
// if an object has both inserts and deletes, both stats are valid.
func (w *objectWriterV1) DescribeObject() (ObjectStats, error) {
	var stats ObjectStats

	if len(w.blocks[SchemaData]) != 0 {
		stats = describeObjectHelper(w, w.colmeta, SchemaData)
	}

	if len(w.blocks[SchemaTombstone]) != 0 {
		if !stats.IsZero() {
			panic("schema data and schema tombstone should not all be valid at the same time")
		}
		stats = describeObjectHelper(w, w.tombstonesColmeta, SchemaTombstone)
	}

	return stats, nil
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

func (w *objectWriterV1) GetOrignalSize() uint32 {
	return w.originSize
}

func (w *objectWriterV1) WriteSubBlock(batch *batch.Batch, dataType DataMetaType) (BlockObject, int, error) {
	denseSeqnums := NewSeqnums(nil)
	denseSeqnums.InitWithColCnt(len(batch.Vecs))
	block := NewBlock(denseSeqnums)
	size, err := w.AddSubBlock(block, batch, denseSeqnums, dataType)
	return block, size, err
}

func (w *objectWriterV1) WriteWithoutSeqnum(batch *batch.Batch) (BlockObject, error) {
	denseSeqnums := NewSeqnums(nil)
	denseSeqnums.InitWithColCnt(len(batch.Vecs))
	block := NewBlock(denseSeqnums)
	w.AddBlock(block, batch, denseSeqnums)
	return block, nil
}

func (w *objectWriterV1) UpdateBlockZM(tye DataMetaType, blkIdx int, seqnum uint16, zm ZoneMap) {
	w.blocks[tye][blkIdx].meta.ColumnMeta(seqnum).SetZoneMap(zm)
}

func (w *objectWriterV1) WriteBF(blkIdx int, seqnum uint16, buf []byte, typ uint8) (err error) {
	w.blocks[SchemaData][blkIdx].bloomFilter = buf
	w.blocks[SchemaData][blkIdx].meta.BlockHeader().SetBloomFilterType(typ)
	return
}

// AllocFromArena allocates size bytes from the writer's arena.
// Falls back to make when there is no arena.
func (w *objectWriterV1) AllocFromArena(size int) []byte {
	if w.arena != nil {
		return w.arena.Alloc(size)
	}
	return make([]byte, size)
}

func (w *objectWriterV1) SetAppendable() {
	w.appendable = true
}

func (w *objectWriterV1) SetSortKeySeqnum(seqnum uint16) {
	w.sortKeySeqnum = seqnum
}

func (w *objectWriterV1) WriteObjectMetaBF(buf []byte) (err error) {
	w.bloomFilter = buf
	return
}

func (w *objectWriterV1) WriteObjectMeta(ctx context.Context, totalrow uint32, metas []ColumnMeta) {
	w.totalRow = totalrow
	w.colmeta = metas
}

func (w *objectWriterV1) prepareDataMeta(objectMeta objectDataMetaV1, blocks []blockData, offset uint32, offsetId uint16) ([]byte, Extent, error) {
	var columnCount uint16
	columnCount = 0
	metaColCnt := uint16(0)
	maxSeqnum := uint16(0)
	var seqnums *Seqnums
	if len(blocks) != 0 {
		columnCount = blocks[0].meta.GetColumnCount()
		metaColCnt = blocks[0].meta.GetMetaColumnCount()
		maxSeqnum = blocks[0].meta.GetMaxSeqnum()
		seqnums = blocks[0].seqnums
	}
	objectMeta.BlockHeader().SetColumnCount(columnCount)
	objectMeta.BlockHeader().SetMetaColumnCount(metaColCnt)
	objectMeta.BlockHeader().SetMaxSeqnum(maxSeqnum)

	// prepare object meta and block index
	meta, extent, err := w.prepareObjectMeta(blocks, objectMeta, offset, seqnums, offsetId)
	if err != nil {
		return nil, nil, err
	}
	return meta, extent, err
}

func (w *objectWriterV1) prepareObjectMeta(blocks []blockData, objectMeta objectDataMetaV1, offset uint32, seqnums *Seqnums, offsetId uint16) ([]byte, Extent, error) {
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

	// Pre-size to avoid repeated bytes.Buffer growth.  length already holds the exact
	// total byte count of what we will write.
	buf := bytes.NewBuffer(make([]byte, 0, int(length)))
	buf.Write(objectMeta)
	buf.Write(blockIndex)
	// writer block metadata
	for _, block := range blocks {
		buf.Write(block.meta)
	}
	return buf.Bytes(), extent, nil
}

func (w *objectWriterV1) prepareBlockMeta(offset uint32, blocks []blockData, colmeta []ColumnMeta) uint32 {
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
	h := IOEntryHeader{IOET_BF, IOET_BloomFilter_CurrVer}
	bloomFilterIndex := BuildBlockIndex(blockCount + 1)
	bloomFilterIndex.SetBlockCount(blockCount + 1)
	// bloomFilterStart tracks byte offset within the BF content area (i.e., after the header).
	bloomFilterStart := uint32(bloomFilterIndex.Length())
	for i, block := range blocks {
		n := uint32(len(block.bloomFilter))
		bloomFilterIndex.SetBlockMetaPos(uint32(i), bloomFilterStart, n)
		bloomFilterStart += n
	}
	bloomFilterIndex.SetBlockMetaPos(blockCount, bloomFilterStart, uint32(len(w.bloomFilter)))
	total := IOEntryHeaderSize + int(bloomFilterStart) + len(w.bloomFilter)
	data := w.AllocFromArena(total)
	off := 0
	copy(data[off:], EncodeIOEntryHeader(&h))
	off += IOEntryHeaderSize
	copy(data[off:], bloomFilterIndex)
	off += len(bloomFilterIndex)
	for _, block := range blocks {
		copy(data[off:], block.bloomFilter)
		off += len(block.bloomFilter)
	}
	copy(data[off:], w.bloomFilter)
	length := uint32(total)
	extent := NewExtent(compress.None, offset, length, length)
	return data, extent, nil
}

func (w *objectWriterV1) prepareZoneMapArea(blocks []blockData, blockCount uint32, offset uint32) ([]byte, Extent, error) {
	h := IOEntryHeader{IOET_ZM, IOET_ZoneMap_CurrVer}
	zoneMapAreaIndex := BuildBlockIndex(blockCount)
	zoneMapAreaIndex.SetBlockCount(blockCount)
	zoneMapAreaStart := uint32(zoneMapAreaIndex.Length())
	for i, block := range blocks {
		n := uint32(block.meta.GetMetaColumnCount() * ZoneMapSize)
		zoneMapAreaIndex.SetBlockMetaPos(uint32(i), zoneMapAreaStart, n)
		zoneMapAreaStart += n
	}
	// Pre-size to avoid repeated bytes.Buffer growth during Write calls.
	total := IOEntryHeaderSize + int(zoneMapAreaStart)
	buf := bytes.NewBuffer(make([]byte, 0, total))
	buf.Write(EncodeIOEntryHeader(&h))
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

// writerBlocks writes blocks to object file
// If the compressed data exceeds 3G,
// an error of limited writing is returned
func (w *objectWriterV1) writerBlocks(blocks []blockData) error {
	maxIndex := w.getMaxIndex(blocks)
	size := uint64(0)
	for idx := uint16(0); idx < maxIndex; idx++ {
		for _, block := range blocks {
			if block.meta.BlockHeader().ColumnCount() <= idx {
				continue
			}
			w.buffer.Write(block.data[idx])
			size += uint64(len(block.data[idx]))
		}
	}
	if size > uint64(ObjectSizeLimit) {
		return moerr.NewErrTooLargeObjectSizeNoCtx(size)
	}
	return nil
}

func (w *objectWriterV1) WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error) {
	var err error
	w.RLock()
	defer w.RUnlock()

	objectHeader := BuildHeader()
	objectHeader.SetSchemaVersion(w.schemaVer)
	offset := uint32(HeaderSize)
	w.originSize += HeaderSize

	for i := range w.blocks {
		if i == int(SchemaData) {
			offset = w.prepareBlockMeta(offset, w.blocks[SchemaData], w.colmeta)
			continue
		}
		offset = w.prepareBlockMeta(offset, w.blocks[i], w.tombstonesColmeta)
	}

	metaHeader := buildObjectMetaV3()
	objectMetas := make([]objectDataMetaV1, len(w.blocks))
	bloomFilterDatas := make([][]byte, len(w.blocks))
	bloomFilterExtents := make([]Extent, len(w.blocks))
	zoneMapAreaDatas := make([][]byte, len(w.blocks))
	zoneMapAreaExtents := make([]Extent, len(w.blocks))
	metas := make([][]byte, len(w.blocks))
	metaExtents := make([]Extent, len(w.blocks))
	for i := range w.blocks {
		colMetaCount := uint16(0)
		if len(w.blocks[i]) > 0 {
			colMetaCount = w.blocks[i][0].meta.GetMetaColumnCount()
		}
		objectMetas[i] = BuildObjectMeta(colMetaCount)
		// prepare bloom filter
		bloomFilterDatas[i], bloomFilterExtents[i], err = w.prepareBloomFilter(w.blocks[i], uint32(len(w.blocks[i])), offset)
		if err != nil {
			return nil, err
		}
		objectMetas[i].BlockHeader().SetBFExtent(bloomFilterExtents[i])
		objectMetas[i].BlockHeader().SetAppendable(w.appendable)
		objectMetas[i].BlockHeader().SetSortKey(w.sortKeySeqnum)
		offset += bloomFilterExtents[i].Length()
		w.originSize += bloomFilterExtents[i].OriginSize()

		// prepare zone map area
		zoneMapAreaDatas[i], zoneMapAreaExtents[i], err = w.prepareZoneMapArea(w.blocks[i], uint32(len(w.blocks[i])), offset)
		if err != nil {
			return nil, err
		}
		objectMetas[i].BlockHeader().SetZoneMapArea(zoneMapAreaExtents[i])
		offset += zoneMapAreaExtents[i].Length()
		w.originSize += zoneMapAreaExtents[i].OriginSize()
	}
	subMetaCount := uint16(len(w.blocks) - 2)
	subMetachIndex := BuildSubMetaIndex(subMetaCount)
	subMetachIndex.SetSubMetaCount(subMetaCount)
	startID := uint16(0)
	start := uint32(0)
	idxStart := metaHeader.HeaderLength() + subMetachIndex.Length()
	for i := range w.blocks {
		// prepare object meta and block index
		metas[i], metaExtents[i], err = w.prepareDataMeta(objectMetas[i], w.blocks[i], offset, startID)
		if err != nil {
			return nil, err
		}
		if i == int(SchemaData) {
			start = metaExtents[SchemaData].Offset()
			metaHeader.SetDataMetaOffset(idxStart)
			metaHeader.SetDataMetaCount(uint16(len(w.blocks[i])))
		} else if i == int(SchemaTombstone) {
			if len(w.blocks[i]) != 0 {
				panic("invalid data meta type")
			}
		} else {
			subMetachIndex.SetSchemaMeta(uint16(i-2), uint16(i-2), uint16(len(w.blocks[i])), idxStart)
		}
		idxStart += metaExtents[i].OriginSize()
		startID += uint16(len(w.blocks[i]))
	}
	var buf bytes.Buffer
	h := IOEntryHeader{IOET_ObjMeta, IOET_ObjectMeta_CurrVer}
	buf.Write(EncodeIOEntryHeader(&h))
	buf.Write(metaHeader)
	buf.Write(subMetachIndex)

	for i := range metas {
		buf.Write(metas[i])
	}
	objMeta, extent, err := w.WriteWithCompress(start, buf.Bytes())
	objectHeader.SetExtent(extent)

	// begin write

	// writer object header
	w.buffer.Write(objectHeader)

	// writer data
	for i := range w.blocks {
		err = w.writerBlocks(w.blocks[i])
		if err != nil {
			return nil, err
		}
	}
	// writer bloom filter
	for i := range bloomFilterDatas {
		w.buffer.Write(bloomFilterDatas[i])
		w.buffer.Write(zoneMapAreaDatas[i])
	}
	// writer object metadata
	w.buffer.Write(objMeta)

	// write footer
	footer := Footer{
		metaExtent: extent,
		version:    Version,
		magic:      Magic,
	}
	footerBuf := footer.Marshal()
	w.buffer.Write(footerBuf)
	if err != nil {
		return nil, err
	}
	w.originSize += objectHeader.Extent().OriginSize()
	w.originSize += uint32(len(footerBuf))
	w.size = objectHeader.Extent().End() + uint32(len(footerBuf))
	blockObjects := make([]BlockObject, 0)
	for i := range w.blocks {
		for j := range w.blocks[i] {
			header := w.blocks[i][j].meta.BlockHeader()
			header.SetMetaLocation(objectHeader.Extent())
			blockObjects = append(blockObjects, w.blocks[i][j].meta)
		}
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

func (w *objectWriterV1) Sync(ctx context.Context, items ...WriteOptions) error {
	var err error
	w.buffer.SetDataOptions(items...)
	defer func() {
		if err != nil {
			w.buffer = nil
			logutil.Error("[DoneWithErr] Write Sync error",
				zap.Error(err),
				zap.String("file name", w.fileName))
		}
	}()
	// if a compact task is rollbacked, it may leave a written file in fs
	// here we just delete it and write again
	err = w.object.fs.Write(ctx, w.buffer.GetData())
	if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		if err = w.object.fs.Delete(ctx, w.fileName); err != nil {
			return err
		}
		err = w.object.fs.Write(ctx, w.buffer.GetData())
	}

	if err != nil {
		return err
	}

	w.objStats, err = w.DescribeObject()
	return err
}

func (w *objectWriterV1) GetDataStats() ObjectStats {
	return w.objStats
}

func (w *objectWriterV1) WriteWithCompress(offset uint32, buf []byte) (data []byte, extent Extent, err error) {
	dataLen := len(buf)
	compressBlockBound := lz4.CompressBlockBound(dataLen)
	var compressBuf []byte
	if w.arena != nil {
		compressBuf = w.arena.CompressBuf(compressBlockBound)
	} else {
		if len(w.compressBuf) < compressBlockBound {
			w.compressBuf = make([]byte, compressBlockBound)
		}
		compressBuf = w.compressBuf[:compressBlockBound]
	}
	n, err := w.lz4c.CompressBlock(buf, compressBuf)
	if err != nil {
		return
	}
	length := uint32(n)
	if w.arena != nil {
		data = w.arena.Alloc(int(length))
	} else {
		data = make([]byte, length)
	}
	copy(data, compressBuf[:length])
	extent = NewExtent(compress.Lz4, offset, length, uint32(dataLen))
	return
}

func (w *objectWriterV1) addBlock(blocks *[]blockData, blockMeta BlockObject, bat *batch.Batch, seqnums *Seqnums) (int, error) {
	// CHANGE ME
	// block.BlockHeader()return w.WriteWithCompress(offset, buf.Bytes()).SetBlockID(w.lastId)
	blockMeta.BlockHeader().SetSequence(uint16(w.lastId))

	block := blockData{meta: blockMeta, seqnums: seqnums}
	var data []byte
	var rows int
	var size int
	for i, vec := range bat.Vecs {
		if i == 0 {
			rows = vec.Length()
		} else if rows != vec.Length() {
			attr := "unknown"
			if len(bat.Attrs) > i {
				attr = bat.Attrs[i]
			}
			logutil.Debugf("%s unmatched length, expect %d, get %d", attr, rows, vec.Length())
		}
		// Use the arena's serialization buffer when available so it is
		// reused across write cycles (avoids a fresh bytes.Buffer growth
		// for every new objectWriterV1 instance).
		var sbuf *bytes.Buffer
		if w.arena != nil {
			sbuf = &w.arena.serialBuf
		} else {
			sbuf = &w.buf
		}
		sbuf.Reset()
		// Pre-size buffer to avoid repeated growSlice during MarshalBinaryWithBuffer.
		// vec.Size() ≈ data + area; add overhead for header, lengths, nsp, sorted flag.
		if needed := vec.Size() + 64; needed > sbuf.Cap() {
			sbuf.Grow(needed)
		}
		h := IOEntryHeader{IOET_ColData, IOET_ColumnData_CurrVer}
		sbuf.Write(EncodeIOEntryHeader(&h))
		if err := vec.MarshalBinaryWithBuffer(sbuf); err != nil {
			return 0, err
		}
		var ext Extent
		var err error
		if data, ext, err = w.WriteWithCompress(0, sbuf.Bytes()); err != nil {
			return 0, err
		}
		size += len(data)
		block.data = append(block.data, data)
		blockMeta.ColumnMeta(seqnums.Seqs[i]).setLocation(ext)
		blockMeta.ColumnMeta(seqnums.Seqs[i]).setDataType(uint8(vec.GetType().Oid))
		if vec.GetType().Oid == types.T_any {
			panic("any type batch")
		}
		blockMeta.ColumnMeta(seqnums.Seqs[i]).SetNullCnt(uint32(vec.GetNulls().GetCardinality()))
		w.originSize += ext.OriginSize()
	}
	blockMeta.BlockHeader().SetRows(uint32(rows))
	*blocks = append(*blocks, block)
	w.lastId++
	return size, nil
}

func (w *objectWriterV1) AddBlock(blockMeta BlockObject, bat *batch.Batch, seqnums *Seqnums) (int, error) {
	w.Lock()
	defer w.Unlock()

	return w.addBlock(&w.blocks[SchemaData], blockMeta, bat, seqnums)
}

func (w *objectWriterV1) AddSubBlock(blockMeta BlockObject, bat *batch.Batch, seqnums *Seqnums, dataType DataMetaType) (int, error) {
	w.Lock()
	defer w.Unlock()
	if dataType < CkpMetaStart {
		panic("invalid data type")
	}
	for i := int(CkpMetaStart); i <= int(CkpMetaEnd); i++ {
		if len(w.blocks) <= i {
			blocks := make([]blockData, 0)
			w.blocks = append(w.blocks, blocks)
		}
	}
	return w.addBlock(&w.blocks[dataType], blockMeta, bat, seqnums)
}

func (w *objectWriterV1) GetBlock(id uint32) BlockObject {
	w.Lock()
	defer w.Unlock()
	return w.blocks[SchemaData][id].meta
}
