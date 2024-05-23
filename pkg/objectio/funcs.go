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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
		return
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

	var obj any
	obj, err = Decode(v)
	if err != nil {
		return
	}

	meta = obj.(ObjectMeta)
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
) (ioVec *fileservice.IOVector, err error) {
	return ReadOneBlockWithMeta(ctx, meta, name, blk, seqnums, typs, m, fs, constructorFactory, policy)
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
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
		Policy:   policy,
	}

	var filledEntries []fileservice.IOEntry
	blkmeta := meta.GetBlockMeta(uint32(blk))
	maxSeqnum := blkmeta.GetMaxSeqnum()
	for i, seqnum := range seqnums {
		// special columns
		if seqnum >= SEQNUM_UPPER {
			metaColCnt := blkmeta.GetMetaColumnCount()
			// read appendable block file, the last columns is commits and abort
			if seqnum == SEQNUM_COMMITTS {
				seqnum = metaColCnt - 2
			} else if seqnum == SEQNUM_ABORT {
				seqnum = metaColCnt - 1
			} else {
				panic(fmt.Sprintf("bad path to read special column %d", seqnum))
			}
			col := blkmeta.ColumnMeta(seqnum)
			ext := col.Location()
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset:      int64(ext.Offset()),
				Size:        int64(ext.Length()),
				ToCacheData: factory(int64(ext.OriginSize()), ext.Alg()),
			})
			continue
		}

		// need fill vector
		if seqnum > maxSeqnum || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
			if filledEntries == nil {
				filledEntries = make([]fileservice.IOEntry, len(seqnums))
			}
			filledEntries[i] = fileservice.IOEntry{
				Size: int64(seqnum), // a marker, it can not be zero
			}
			continue
		}

		// read written normal column
		col := blkmeta.ColumnMeta(seqnum)
		ext := col.Location()
		ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
			Offset:      int64(ext.Offset()),
			Size:        int64(ext.Length()),
			ToCacheData: factory(int64(ext.OriginSize()), ext.Alg()),
		})
	}
	if len(ioVec.Entries) > 0 {
		err = fs.Read(ctx, ioVec)
		if err != nil {
			return
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
				logutil.Infof("block %s generate seqnum %d %v",
					meta.BlockHeader().BlockID().String(), filledEntries[i].Size, typs[i])
				buf := &bytes.Buffer{}
				buf.Write(EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_CurrVer}))
				if err = vector.NewConstNull(typs[i], length, m).MarshalBinaryWithBuffer(buf); err != nil {
					return
				}
				cacheData := fileservice.GetDefaultCacheDataAllocator().Alloc(buf.Len())
				copy(cacheData.Bytes(), buf.Bytes())
				filledEntries[i].CachedData = cacheData
			}
		}
		ioVec.Entries = filledEntries
	}

	return
}

func ReadMultiBlocksWithMeta(
	ctx context.Context,
	name string,
	meta ObjectMeta,
	options map[uint16]*ReadBlockOptions,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	var dataMeta ObjectDataMeta
	for _, opt := range options {
		for seqnum := range opt.Idxes {
			if DataMetaType(opt.DataType) == SchemaData {
				dataMeta = meta.MustDataMeta()
			} else if DataMetaType(opt.DataType) == SchemaTombstone {
				dataMeta = meta.MustTombstoneMeta()
			} else {
				dataMeta, _ = meta.SubMeta(ConvertToCkpIdx(opt.DataType))
			}
			blkmeta := dataMeta.GetBlockMeta(uint32(opt.Id))
			if seqnum > blkmeta.GetMaxSeqnum() || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
				// prefetch, do not generate
				continue
			}
			col := blkmeta.ColumnMeta(seqnum)
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(col.Location().Offset()),
				Size:   int64(col.Location().Length()),

				ToCacheData: factory(int64(col.Location().OriginSize()), col.Location().Alg()),
			})
		}
	}

	err = fs.Read(ctx, ioVec)
	//TODO when to call ioVec.Release?
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
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
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
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(ext.Offset()),
				Size:   int64(ext.Length()),

				ToCacheData: factory(int64(ext.OriginSize()), ext.Alg()),
			})
		}
	}

	err = fs.Read(ctx, ioVec)
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
			Offset: int64(ext.Offset()),
			Size:   int64(ext.Length()),

			ToCacheData: constructorFactory(int64(ext.OriginSize()), ext.Alg()),
		})
	}

	err = fs.Read(ctx, ioVec)
	//TODO when to call ioVec.Release?
	bat = batch.NewWithSize(len(cols))
	var obj any
	for i := range cols {
		obj, err = Decode(ioVec.Entries[i].CachedData.Bytes())
		if err != nil {
			return nil, err
		}
		bat.Vecs[i] = obj.(*vector.Vector)
		bat.SetRowCount(bat.Vecs[i].Length())
	}
	return
}
