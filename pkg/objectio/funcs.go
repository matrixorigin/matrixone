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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	if ctx == nil || meta == nil || fs == nil || factory == nil {
		return fileservice.IOVector{}, moerr.NewInvalidInputNoCtx(
			"object block read requires context, metadata, file service, and cache factory",
		)
	}
	if len(typs) != 0 && len(typs) != len(seqnums) {
		return fileservice.IOVector{}, moerr.NewInvalidInputNoCtxf(
			"object block read has %d columns but %d fallback types",
			len(seqnums), len(typs),
		)
	}
	ioVec = fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0, len(seqnums)),
		Policy:   policy,
	}
	var generatedIOVec fileservice.IOVector
	defer func() {
		if recovered := recover(); recovered != nil {
			err = moerr.NewInvalidInputNoCtxf(
				"object block %d metadata or column data is malformed: %v", blk, recovered,
			)
		}
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

	startID := uint32(meta.BlockHeader().StartID())
	blockID := uint32(blk)
	if blockID < startID || blockID-startID >= meta.BlockCount() {
		return ioVec, moerr.NewInvalidInputNoCtxf(
			"object block %d is outside metadata range [%d,%d)",
			blk, startID, uint64(startID)+uint64(meta.BlockCount()),
		)
	}
	blkmeta := meta.GetBlockMeta(blockID)
	maxSeqnum := blkmeta.GetMaxSeqnum()
	specialLayout := ResolveSpecialColumnLayout(blkmeta)
	for i, seqnum := range seqnums {
		// special columns
		if seqnum >= SEQNUM_UPPER {
			var ok bool
			if seqnum != SEQNUM_COMMITTS && seqnum != SEQNUM_ABORT {
				return ioVec, moerr.NewInvalidInputNoCtxf(
					"unsupported object special column %d", seqnum,
				)
			}
			seqnum, ok = specialLayout.Resolve(seqnum)
			if !ok {
				putFillHolder(i, seqnum)
			} else {
				col := blkmeta.ColumnMeta(seqnum)
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
	if filledEntries != nil && len(typs) == 0 {
		return ioVec, moerr.NewInvalidInputNoCtx(
			"object block read requires fallback types for missing columns",
		)
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
		length := int(blkmeta.GetRows())
		readed := ioVec.Entries
		for i := range filledEntries {
			if filledEntries[i].Size == 0 { // we can tell it is the placeholder for the readed column
				if len(readed) == 0 {
					return ioVec, moerr.NewInternalErrorNoCtx(
						"object block read returned fewer columns than metadata requested",
					)
				}
				filledEntries[i] = readed[0]
				readed = readed[1:]
			} else {
				buf := &bytes.Buffer{}
				buf.Write(EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_CurrVer}))
				func() {
					generated := vector.NewConstNull(typs[i], length, m)
					defer generated.Free(m)
					err = generated.MarshalBinaryWithBuffer(buf)
				}()
				if err != nil {
					return
				}
				cacheData := fileservice.DefaultCacheDataAllocator().CopyToCacheData(ctx, buf.Bytes())
				if cacheData == nil {
					return ioVec, moerr.NewInternalErrorNoCtx(
						"object block fallback column cache allocation failed",
					)
				}
				filledEntries[i].CachedData = cacheData
				generatedIOVec.Entries = append(generatedIOVec.Entries, fileservice.IOEntry{
					CachedData: cacheData,
				})
			}
		}
		if len(readed) != 0 {
			return ioVec, moerr.NewInternalErrorNoCtxf(
				"object block read returned %d unassigned columns", len(readed),
			)
		}
		ioVec.Entries = filledEntries
		// Ownership of generated cache entries has moved into ioVec.
		generatedIOVec = fileservice.IOVector{}
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
	defer func() {
		if recovered := recover(); recovered != nil {
			if bat != nil {
				bat.Clean(nil)
				bat = nil
			}
			err = moerr.NewInvalidInputNoCtxf(
				"object block %d metadata is malformed: %v", id, recovered,
			)
		}
	}()
	if ctx == nil || meta == nil || fs == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"object block read requires context, metadata, and file service",
		)
	}
	if len(cols) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("object block read has no columns")
	}
	dataMeta := *meta
	startID := uint32(dataMeta.BlockHeader().StartID())
	if id < startID || id-startID >= dataMeta.BlockCount() {
		return nil, moerr.NewInvalidInputNoCtxf(
			"object block %d is outside metadata range [%d,%d)",
			id, startID, uint64(startID)+uint64(dataMeta.BlockCount()),
		)
	}
	blockMeta := dataMeta.GetBlockMeta(id)
	metaColumnCount := blockMeta.GetMetaColumnCount()
	columnTypes := make([]types.T, len(cols))
	for position, seqnum := range cols {
		if seqnum >= metaColumnCount {
			return nil, moerr.NewInvalidInputNoCtxf(
				"object block %d has no metadata for column %d", id, seqnum,
			)
		}
		columnType := types.T(blockMeta.ColumnMeta(seqnum).DataType())
		if columnType == types.T_any {
			return nil, moerr.NewInvalidInputNoCtxf(
				"object block %d column %d is absent", id, seqnum,
			)
		}
		columnTypes[position] = columnType
	}
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
		Policy:   cachePolicy,
	}
	for _, seqnum := range cols {
		col := blockMeta.ColumnMeta(seqnum)
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
	success := false
	defer func() {
		if !success && bat != nil {
			// Decode binds vectors to the cloned Go backing below; they do not own
			// mpool allocations, so nil is the matching cleanup allocator.
			bat.Clean(nil)
			bat = nil
		}
	}()
	var obj any
	expectedRows := int(blockMeta.GetRows())
	for i := range cols {
		if ioVec.Entries[i].CachedData == nil {
			return nil, moerr.NewInvalidInputNoCtxf(
				"object column %d returned no cached data", cols[i],
			)
		}
		// always copy to avoid memory leak
		bs := slices.Clone(ioVec.Entries[i].CachedData.Bytes())
		obj, err = Decode(bs)
		if err != nil {
			return nil, err
		}
		decoded, ok := obj.(*vector.Vector)
		if !ok || decoded == nil {
			return nil, moerr.NewInvalidInputNoCtxf(
				"object column %d decoded as %T, expected vector", cols[i], obj,
			)
		}
		if decoded.Length() != expectedRows {
			decoded.Free(nil)
			return nil, moerr.NewInvalidInputNoCtxf(
				"object column %d has %d rows, expected %d",
				cols[i], decoded.Length(), expectedRows,
			)
		}
		if decoded.GetType().Oid != columnTypes[i] {
			decoded.Free(nil)
			return nil, moerr.NewInvalidInputNoCtxf(
				"object column %d has type %s, expected %s",
				cols[i], decoded.GetType().String(), columnTypes[i].String(),
			)
		}
		bat.Vecs[i] = decoded
	}
	bat.SetRowCount(expectedRows)
	success = true
	return
}
