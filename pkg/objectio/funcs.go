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
) (buf []byte, err error) {
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 1),
		NoCache:  noCache,
	}

	ioVec.Entries[0] = fileservice.IOEntry{
		Offset:   int64(extent.Offset()),
		Size:     int64(extent.Length()),
		ToObject: newDecompressToObject(int64(extent.OriginSize())),
	}
	if err = fs.Read(ctx, ioVec); err != nil {
		return
	}
	buf = ioVec.Entries[0].Object.([]byte)
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
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 1),
		NoCache:  noCache,
	}

	ioVec.Entries[0] = fileservice.IOEntry{
		Offset: int64(extent.Offset()),
		Size:   int64(extent.Length()),

		ToObject: newObjectMetaToObject(int64(extent.OriginSize())),
	}

	if err = fs.Read(ctx, ioVec); err != nil {
		return
	}

	meta = ObjectMeta(ioVec.Entries[0].Object.([]byte))
	return
}

func ReadColumnsWithMeta(
	ctx context.Context,
	name string,
	meta *ObjectMeta,
	options map[uint16]*ReadBlockOptions,
	noCache bool,
	m *mpool.MPool,
	fs fileservice.FileService,
	constructor ReadObjectFunc,
) (ioVec *fileservice.IOVector, err error) {
	ioVec = &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, opt := range options {
		for idx := range opt.Idxes {
			col := meta.GetColumnMeta(idx, uint32(opt.Id))
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(col.Location().Offset()),
				Size:   int64(col.Location().Length()),

				ToObject: constructor(int64(col.Location().OriginSize())),
			})
		}
	}

	err = fs.Read(ctx, ioVec)
	return
}
