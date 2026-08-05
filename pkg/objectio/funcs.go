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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

func ReleaseIOVector(vector *fileservice.IOVector) {
	vector.Release()
}

func ReadExtent(
	ctx context.Context,
	name string,
	extent *Extent,
	policy fileservice.Policy,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (buf []byte, err error) {
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 1),
		Policy:   policy,
	}

	ioVec.Entries[0] = fileservice.IOEntry{
		Offset:      int64(extent.Offset()),
		Size:        int64(extent.Length()),
		ToCacheData: factory(int64(extent.OriginSize()), extent.Alg()),
	}
	if err = fs.Read(ctx, ioVec); err != nil {
		ioVec.ReleaseReadResultOnError()
		return
	}
	if ioVec.Entries[0].CachedData == nil {
		logutil.Errorf("ReadExtent: ioVec.Entries[0].CachedData is nil, name: %s, extent: %v",
			name, extent.String())
		util.EnableCoreDump()
		util.CoreDump()
	}
	//TODO when to call ioVec.Release?
	v := ioVec.Entries[0].CachedData.Bytes()
	buf = make([]byte, len(v))
	copy(buf, v)
	ReleaseIOVector(ioVec)
	return
}

func ReadBloomFilter(
	ctx context.Context,
	name string,
	extent *Extent,
	policy fileservice.Policy,
	fs fileservice.FileService,
) (filters BloomFilter, err error) {
	var v []byte
	if v, err = ReadExtent(
		ctx,
		name,
		extent,
		policy,
		fs,
		constructorFactory); err != nil {
		return
	}

	var obj any
	obj, err = Decode(v)
	if err != nil {
		return
	}

	filters = obj.([]byte)
	return
}

func ReadObjectMeta(
	ctx context.Context,
	name string,
	extent *Extent,
	policy fileservice.Policy,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	var v []byte
	if v, err = ReadExtent(ctx, name, extent, policy, fs, constructorFactory); err != nil {
		return
	}
	meta = MustObjectMeta(v)
	return
}

func ReadOneBlock(
	ctx context.Context,
	meta *ObjectDataMeta,
	name string,
	blk uint16,
	seqnums []uint16,
	typs []types.Type,
	m *mpool.MPool,
	fs fileservice.FileService,
	policy fileservice.Policy,
) (ioVec fileservice.IOVector, err error) {
	return ReadOneBlockWithMeta(ctx, meta, name, blk, seqnums, typs, m, fs, columnCacheConstructorFactory, policy)
}

func ReadOneBlockWithMeta(
	ctx context.Context,
	meta *ObjectDataMeta,
	name string,
	blk uint16,
	seqnums []uint16,
	typs []types.Type,
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
	policy fileservice.Policy,
) (ioVec fileservice.IOVector, err error) {
	ioVec = fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0, len(seqnums)),
		Policy:   policy,
	}
	var generatedIOVec fileservice.IOVector
	defer func() {
		if err != nil {
			ioVec.ReleaseReadResultOnError()
			generatedIOVec.ReleaseReadResultOnError()
			ioVec = fileservice.IOVector{}
		}
	}()

	var filledEntries []fileservice.IOEntry
	putFillHolder := func(i int, seqnum uint16) {
		if filledEntries == nil {
			filledEntries = make([]fileservice.IOEntry, len(seqnums))
		}
		filledEntries[i] = fileservice.IOEntry{
			Size: int64(seqnum) + 1, // a marker, it must not be zero
		}
	}

	blkmeta := meta.GetBlockMeta(uint32(blk))
	maxSeqnum := blkmeta.GetMaxSeqnum()
	for i, seqnum := range seqnums {
		// special columns
		if seqnum >= SEQNUM_UPPER {
			metaColCnt := blkmeta.GetMetaColumnCount()
			switch seqnum {
			case SEQNUM_COMMITTS:
				if metaColCnt == 0 {
					putFillHolder(i, 0)
					continue
				}
				seqnum = metaColCnt - 1
			case SEQNUM_ABORT:
				panic("not support")
			default:
				panic(fmt.Sprintf("bad path to read special column %d", seqnum))
			}
			// Type alone is insufficient: the last user column may itself be
			// T_TS. A hidden commit-TS column must sit beyond MaxSeqnum.
			// If the last column is not commits, do not read it:
			//  1. created by cn
			//  2. old version tn nonappendable block
			col := blkmeta.ColumnMeta(seqnum)
			hasHiddenColumn := metaColCnt > maxSeqnum+1
			if !hasHiddenColumn || col.DataType() != uint8(types.T_TS) {
				putFillHolder(i, seqnum)
			} else {
				ext := col.Location()
				ioVec.Entries = append(ioVec.Entries, newColumnIOEntry(ext, factory))
			}
			continue
		}

		// need fill vector
		if seqnum > maxSeqnum || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
			putFillHolder(i, seqnum)
			continue
		}

		// read written normal column
		col := blkmeta.ColumnMeta(seqnum)
		ext := col.Location()
		ioVec.Entries = append(ioVec.Entries, newColumnIOEntry(ext, factory))
	}
	if len(ioVec.Entries) > 0 {
		err = fs.Read(ctx, &ioVec)
		if err != nil {
			return
		}

		// Record actual bytes read from storage layer (excluding rowid, which is generated, not loaded)
		var totalReadSize int64
		for _, entry := range ioVec.Entries {
			totalReadSize += entry.Size
		}

		// Record actual bytes read from storage layer using CounterSet
		// Note: S3 and Disk read sizes are recorded in filesystem layer (S3FS/LocalFS)
		// where we can accurately determine the data source
		if totalReadSize > 0 {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.ReadSize.Add(totalReadSize)
			})
		}
		//TODO when to call ioVec.Release?
	}

	// need to generate vector
	if filledEntries != nil {
		if len(typs) == 0 {
			panic(fmt.Sprintf("block %s generate need typs", meta.BlockHeader().BlockID().String()))
		}
		length := int(blkmeta.GetRows())
		readed := ioVec.Entries
		for i := range filledEntries {
			if filledEntries[i].Size == 0 { // we can tell it is the placeholder for the readed column
				filledEntries[i] = readed[0]
				readed = readed[1:]
			} else {
				buf := &bytes.Buffer{}
				buf.Write(EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_CurrVer}))
				if err = vector.NewConstNull(typs[i], length, m).MarshalBinaryWithBuffer(buf); err != nil {
					return
				}
				cacheData := fileservice.DefaultCacheDataAllocator().CopyToCacheData(ctx, buf.Bytes())
				filledEntries[i].CachedData = cacheData
				generatedIOVec.Entries = append(generatedIOVec.Entries, fileservice.IOEntry{
					CachedData: cacheData,
				})
			}
		}
		ioVec.Entries = filledEntries
	}

	return
}

func ReadAllBlocksWithMeta(
	ctx context.Context,
	meta *ObjectDataMeta,
	name string,
	cols []uint16,
	policy fileservice.Policy,
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec fileservice.IOVector, err error) {
	ioVec = fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0, len(cols)*int(meta.BlockCount())),
		Policy:   policy,
	}
	for blk := uint32(0); blk < meta.BlockCount(); blk++ {
		for _, seqnum := range cols {
			blkmeta := meta.GetBlockMeta(blk)
			if seqnum > blkmeta.GetMaxSeqnum() || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
				// prefetch, do not generate
				panic("ReadAllBlocksWithMeta expect no schema changes")
			}
			col := blkmeta.ColumnMeta(seqnum)
			ext := col.Location()
			ioVec.Entries = append(ioVec.Entries, newColumnIOEntry(ext, factory))

		}
	}

	err = fs.Read(ctx, &ioVec)
	if err != nil {
		ioVec.ReleaseReadResultOnError()
		ioVec = fileservice.IOVector{}
		return
	}
	//TODO when to call ioVec.Release?
	return
}

func ReadOneBlockAllColumns(
	ctx context.Context,
	meta *ObjectDataMeta,
	name string,
	id uint32,
	cols []uint16,
	cachePolicy fileservice.Policy,
	fs fileservice.FileService,
) (bat *batch.Batch, err error) {
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
		Policy:   cachePolicy,
	}
	for _, seqnum := range cols {
		blkmeta := meta.GetBlockMeta(id)
		col := blkmeta.ColumnMeta(seqnum)
		ext := col.Location()
		ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
			Offset:         int64(ext.Offset()),
			Size:           int64(ext.Length()),
			CachedDataSize: int64(ext.OriginSize()),
			ToCacheData:    constructorFactory(int64(ext.OriginSize()), ext.Alg()),
		})
	}

	err = fs.Read(ctx, ioVec)
	if err != nil {
		ioVec.ReleaseReadResultOnError()
		return nil, err
	}
	defer ioVec.Release()

	bat = batch.NewWithSize(len(cols))
	var obj any
	for i := range cols {
		// always copy to avoid memory leak
		bs := slices.Clone(ioVec.Entries[i].CachedData.Bytes())
		obj, err = Decode(bs)
		if err != nil {
			return nil, err
		}
		bat.Vecs[i] = obj.(*vector.Vector)
		bat.SetRowCount(bat.Vecs[i].Length())
	}
	return
}

// ReadOneBlockAllColumnsWindow materializes only the requested rows and reads
// columns sequentially. This keeps the retained source working set to one
// decoded column plus the bounded output window.
func ReadOneBlockAllColumnsWindow(
	ctx context.Context,
	meta *ObjectDataMeta,
	name string,
	id uint32,
	cols []uint16,
	offset, length int,
	cachePolicy fileservice.Policy,
	fs fileservice.FileService,
	mp *mpool.MPool,
	maxSourceBytes int,
	spillFactory ColumnWindowSpillFactory,
) (bat *batch.Batch, err error) {
	if length <= 0 {
		return nil, moerr.NewInvalidInputNoCtx("object block window must contain rows")
	}
	bat = batch.NewWithSize(len(cols))
	defer func() {
		if err != nil {
			bat.Clean(mp)
			bat = nil
		}
	}()
	for i, seqnum := range cols {
		col := meta.GetBlockMeta(id).ColumnMeta(seqnum)
		ext := col.Location()
		if ext.Alg() == compress.Lz4Chunked {
			bat.Vecs[i], err = readChunkedColumnWindow(
				ctx, name, ext, offset, length, cachePolicy, fs, mp,
			)
			if err != nil {
				return nil, err
			}
			continue
		}
		if maxSourceBytes > 0 && int64(ext.OriginSize()) > int64(maxSourceBytes) {
			bat.Vecs[i], err = readLegacyColumnWindow(
				ctx, name, ext, offset, length, fs, mp, spillFactory,
			)
			if err != nil {
				return nil, err
			}
			continue
		}
		ioVec := &fileservice.IOVector{
			FilePath: name,
			Entries: []fileservice.IOEntry{{
				Offset: int64(ext.Offset()), Size: int64(ext.Length()),
				CachedDataSize: int64(ext.OriginSize()),
				ToCacheData:    constructorFactory(int64(ext.OriginSize()), ext.Alg()),
			}},
			Policy: cachePolicy,
		}
		if err = fs.Read(ctx, ioVec); err != nil {
			ioVec.ReleaseReadResultOnError()
			return nil, err
		}
		vec, materializeErr := MaterializeCachedVectorWindow(
			ioVec.Entries[0].CachedData, offset, length, mp,
		)
		ioVec.Release()
		if materializeErr != nil {
			return nil, materializeErr
		}
		bat.Vecs[i] = vec
	}
	bat.SetRowCount(length)
	return bat, nil
}

func readChunkedColumnWindow(
	ctx context.Context,
	name string,
	ext Extent,
	offset, length int,
	policy fileservice.Policy,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (*vector.Vector, error) {
	readRange := func(relativeOffset, size uint32, originSize uint32, algorithm uint8) (fscache.Data, error) {
		ioVec := &fileservice.IOVector{
			FilePath: name,
			Entries: []fileservice.IOEntry{{
				Offset: int64(ext.Offset() + relativeOffset), Size: int64(size),
				CachedDataSize: int64(originSize),
				ToCacheData:    constructorFactory(int64(originSize), algorithm),
			}},
			Policy: policy,
		}
		if err := fs.Read(ctx, ioVec); err != nil {
			ioVec.ReleaseReadResultOnError()
			return nil, err
		}
		data := ioVec.Entries[0].CachedData
		data.Retain()
		ioVec.Release()
		return data, nil
	}
	prefix, err := readRange(0, columnChunkHeaderSize, columnChunkHeaderSize, compress.None)
	if err != nil {
		return nil, err
	}
	headerSize, err := chunkedColumnHeaderReadSize(prefix.Bytes())
	prefix.Release()
	if err != nil {
		return nil, err
	}
	header, err := readRange(0, uint32(headerSize), uint32(headerSize), compress.None)
	if err != nil {
		return nil, err
	}
	totalRows, metas, err := parseColumnChunkHeader(header.Bytes(), ext.Length())
	header.Release()
	if err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 || offset > int(totalRows)-length {
		return nil, moerr.NewInvalidInputNoCtx("chunked object column window is out of range")
	}
	windowEnd := offset + length
	var dst *vector.Vector
	for _, meta := range metas {
		chunkStart, chunkEnd := int(meta.rowStart), int(meta.rowStart+meta.rowCount)
		if chunkEnd <= offset || chunkStart >= windowEnd {
			continue
		}
		chunkData, readErr := readRange(meta.offset, meta.length, meta.originSize, meta.algorithm)
		if readErr != nil {
			if dst != nil {
				dst.Free(mp)
			}
			return nil, readErr
		}
		localStart := max(offset, chunkStart) - chunkStart
		localEnd := min(windowEnd, chunkEnd) - chunkStart
		part, materializeErr := MaterializeCachedVectorWindow(
			chunkData, localStart, localEnd-localStart, mp,
		)
		chunkData.Release()
		if materializeErr != nil {
			if dst != nil {
				dst.Free(mp)
			}
			return nil, materializeErr
		}
		if dst == nil {
			dst = part
		} else {
			if err = dst.UnionBatch(part, 0, part.Length(), nil, mp); err != nil {
				part.Free(mp)
				dst.Free(mp)
				return nil, err
			}
			part.Free(mp)
		}
	}
	if dst == nil || dst.Length() != length {
		if dst != nil {
			dst.Free(mp)
		}
		return nil, moerr.NewInvalidInputNoCtx("chunked object column window row count mismatch")
	}
	return dst, nil
}
