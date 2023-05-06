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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func ReadExtent(
	ctx context.Context,
	name string,
	extent *Extent,
	noLRUCache bool,
	noHeaderHint bool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (v any, err error) {
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 1),
		NoCache:  noLRUCache,
	}

	ioVec.Entries[0] = fileservice.IOEntry{
		Offset:   int64(extent.Offset()),
		Size:     int64(extent.Length()),
		ToObject: factory(int64(extent.OriginSize()), extent.Alg(), noHeaderHint),
	}
	if err = fs.Read(ctx, ioVec); err != nil {
		return
	}
	v = ioVec.Entries[0].Object
	return
}

func ReadBloomFilter(
	ctx context.Context,
	name string,
	extent *Extent,
	noLRUCache bool,
	fs fileservice.FileService,
) (filters BloomFilter, err error) {
	var v any
	if v, err = ReadExtent(
		ctx,
		name,
		extent,
		noLRUCache,
		false,
		fs,
		constructorFactory); err != nil {
		return
	}
	filters = v.([]byte)
	return
}

func ReadObjectMetaWithLocation(
	ctx context.Context,
	location *Location,
	noLRUCache bool,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	name := location.Name().String()
	extent := location.Extent()
	return ReadObjectMeta(ctx, name, &extent, noLRUCache, fs)
}

func ReadObjectMeta(
	ctx context.Context,
	name string,
	extent *Extent,
	noLRUCache bool,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	var v any
	if v, err = ReadExtent(ctx, name, extent, noLRUCache, false, fs, constructorFactory); err != nil {
		return
	}
	meta = ObjectMeta(v.([]byte))
	return
}

func ReadOneBlockWithMeta(
	ctx context.Context,
	meta *ObjectMeta,
	name string,
	blk uint16,
	idxs []uint16,
	typs []types.Type,
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	var filledEntries []fileservice.IOEntry
	blkmeta := meta.GetBlockMeta(uint32(blk))
	maxSeqnum := blkmeta.GetMaxSeqnum()
	for i, seqnum := range idxs {
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
				Offset:   int64(ext.Offset()),
				Size:     int64(ext.Length()),
				ToObject: factory(int64(ext.OriginSize()), ext.Alg(), false),
			})
			continue
		}

		// need fill vector
		if seqnum > maxSeqnum || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
			filledEntries = make([]fileservice.IOEntry, len(idxs))
			filledEntries[i] = fileservice.IOEntry{
				Size: int64(seqnum), // a marker
			}
			continue
		}

		// read written normal column
		col := blkmeta.ColumnMeta(seqnum)
		ext := col.Location()
		ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
			Offset:   int64(ext.Offset()),
			Size:     int64(ext.Length()),
			ToObject: factory(int64(ext.OriginSize()), ext.Alg(), false),
		})
	}
	if len(ioVec.Entries) > 0 {
		err = fs.Read(ctx, ioVec)
		if err != nil {
			return
		}
	}

	if filledEntries != nil {
		if len(typs) == 0 {
			panic(fmt.Sprintf("block %s generate need typs", meta.BlockHeader().BlockID().String()))
		}
		length := int(blkmeta.GetRows())
		readed := ioVec.Entries
		// need to generate vector
		for i := range filledEntries {
			if filledEntries[i].Size == 0 {
				filledEntries[i] = readed[0]
				readed = readed[1:]
			} else {
				logutil.Infof("block %s generate seqnum %d %v",
					meta.BlockHeader().BlockID().String(), filledEntries[i].Size, typs[i])
				filledEntries[i].Object = containers.FillCNConstVector(length, typs[i], nil, m)
			}
		}
		ioVec.Entries = filledEntries
	}

	return
}

func ReadMultiBlocksWithMeta(
	ctx context.Context,
	name string,
	meta *ObjectMeta,
	options map[uint16]*ReadBlockOptions,
	noLRUCache bool,
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, opt := range options {
		for seqnum := range opt.Idxes {
			blkmeta := meta.GetBlockMeta(uint32(opt.Id))
			if seqnum > blkmeta.GetMaxSeqnum() || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
				// prefetch, do not generate
				continue
			}
			col := blkmeta.ColumnMeta(seqnum)
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(col.Location().Offset()),
				Size:   int64(col.Location().Length()),

				ToObject: factory(int64(col.Location().OriginSize()), col.Location().Alg(), false),
			})
		}
	}

	err = fs.Read(ctx, ioVec)
	return
}

func ReadAllBlocksWithMeta(
	ctx context.Context,
	meta *ObjectMeta,
	name string,
	cols []uint16,
	noLRUCache bool,
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0, len(cols)*int(meta.BlockCount())),
		NoCache:  noLRUCache,
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

				ToObject: factory(int64(ext.OriginSize()), ext.Alg(), false),
			})
		}
	}

	err = fs.Read(ctx, ioVec)
	return
}
