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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func ReadExtent(
	ctx context.Context,
	name string,
	extent *Extent,
	noCache bool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (v any, err error) {
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 1),
		NoCache:  noCache,
	}

	ioVec.Entries[0] = fileservice.IOEntry{
		Offset:   int64(extent.Offset()),
		Size:     int64(extent.Length()),
		ToObject: factory(int64(extent.OriginSize())),
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
	noCache bool,
	fs fileservice.FileService,
) (filters []StaticFilter, err error) {
	var v any
	if v, err = ReadExtent(
		ctx,
		name,
		extent,
		noCache,
		fs,
		BloomFilterConstructorFactory); err != nil {
		return
	}
	filters = v.([]StaticFilter)
	return
}

func ReadObjectMetaWithLocation(
	ctx context.Context,
	location *Location,
	noCache bool,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	name := location.Name().String()
	extent := location.Extent()
	return ReadObjectMeta(ctx, name, &extent, noCache, fs)
}

func ReadObjectMeta(
	ctx context.Context,
	name string,
	extent *Extent,
	noCache bool,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	var v any
	if v, err = ReadExtent(ctx, name, extent, noCache, fs, objectMetaConstructorFactory); err != nil {
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
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, col := range idxs {
		col := meta.GetColumnMeta(uint32(blk), col)
		ext := col.Location()
		ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
			Offset:   int64(ext.Offset()),
			Size:     int64(ext.Length()),
			ToObject: factory(int64(ext.OriginSize())),
		})
	}
	err = fs.Read(ctx, ioVec)
	return
}

func ReadMultiBlocksWithMeta(
	ctx context.Context,
	name string,
	meta *ObjectMeta,
	options map[uint16]*ReadBlockOptions,
	noCache bool,
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, opt := range options {
		for col := range opt.Idxes {
			col := meta.GetColumnMeta(uint32(opt.Id), col)
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(col.Location().Offset()),
				Size:   int64(col.Location().Length()),

				ToObject: factory(int64(col.Location().OriginSize())),
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
	noCache bool,
	m *mpool.MPool,
	fs fileservice.FileService,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0, len(cols)*int(meta.BlockCount())),
		NoCache:  noCache,
	}
	for blk := uint32(0); blk < meta.BlockCount(); blk++ {
		for _, colIdx := range cols {
			col := meta.GetColumnMeta(blk, colIdx)
			ext := col.Location()
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(ext.Offset()),
				Size:   int64(ext.Length()),

				ToObject: factory(int64(ext.OriginSize())),
			})
		}
	}

	err = fs.Read(ctx, ioVec)
	return
}
