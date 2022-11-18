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

package blockio

import (
	"context"
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Reader struct {
	reader  objectio.Reader
	fs      *objectio.ObjectFS
	key     string
	meta    *Meta
	name    string
	locs    []objectio.Extent
	readCxt context.Context
}

func NewReader(cxt context.Context, fs *objectio.ObjectFS, key string) (*Reader, error) {
	meta, err := DecodeMetaLocToMeta(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(meta.GetKey(), fs.Service)
	if err != nil {
		return nil, err
	}
	return &Reader{
		fs:      fs,
		reader:  reader,
		key:     key,
		meta:    meta,
		readCxt: cxt,
	}, nil
}

func NewCheckpointReader(fs fileservice.FileService, key string) (*Reader, error) {
	name, locs, err := DecodeMetaLocToMetas(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, fs)
	if err != nil {
		return nil, err
	}
	return &Reader{
		fs:     objectio.NewObjectFS(fs, ""),
		reader: reader,
		key:    key,
		name:   name,
		locs:   locs,
	}, nil
}

func (r *Reader) LoadBlkColumnsByMeta(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject) (*containers.Batch, error) {
	bat := containers.NewBatch()
	if block.GetExtent().End() == 0 {
		return bat, nil
	}
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	ioResult, err := r.reader.Read(r.readCxt, block.GetExtent(), idxs, nil)
	if err != nil {
		return nil, err
	}

	for i := range colNames {
		pkgVec := vector.New(colTypes[i])
		data := make([]byte, len(ioResult.Entries[i].Object.([]byte)))
		copy(data, ioResult.Entries[i].Object.([]byte))
		if err = pkgVec.Read(data); err != nil && !errors.Is(err, io.EOF) {
			return bat, err
		}
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(colTypes[i], nullables[i])
		} else {
			vec = containers.NewVectorWithSharedMemory(pkgVec, nullables[i])
		}
		bat.AddVector(colNames[i], vec)
		bat.Vecs[i] = vec

	}
	return bat, nil
}

func (r *Reader) LoadBlkColumnsByMetaAndIdx(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject,
	idx int) (*containers.Batch, error) {
	bat := containers.NewBatch()

	if block.GetExtent().End() == 0 {
		return nil, nil
	}
	col, err := block.GetColumn(uint16(idx))
	if err != nil {
		return bat, err
	}
	data, err := col.GetData(r.readCxt, nil)
	if err != nil {
		return bat, err
	}
	pkgVec := vector.New(colTypes[0])
	v := make([]byte, len(data.Entries[0].Object.([]byte)))
	copy(v, data.Entries[0].Object.([]byte))
	if err = pkgVec.Read(v); err != nil && !errors.Is(err, io.EOF) {
		return bat, err
	}
	var vec containers.Vector
	if pkgVec.Length() == 0 {
		vec = containers.MakeVector(colTypes[0], nullables[0])
	} else {
		vec = containers.NewVectorWithSharedMemory(pkgVec, nullables[0])
	}
	bat.AddVector(colNames[0], vec)
	return bat, nil
}

func (r *Reader) ReadMeta(m *mpool.MPool) (objectio.BlockObject, error) {
	extents := make([]objectio.Extent, 1)
	extents[0] = r.meta.GetLoc()
	block, err := r.reader.ReadMeta(r.readCxt, extents, m)
	if err != nil {
		return nil, err
	}
	return block[0], err
}

func (r *Reader) ReadMetas(m *mpool.MPool) ([]objectio.BlockObject, error) {
	block, err := r.reader.ReadMeta(r.readCxt, r.locs, m)
	return block, err
}

func (r *Reader) GetDataObject(idx uint16, m *mpool.MPool) objectio.ColumnObject {
	block, err := r.ReadMeta(m)
	if err != nil {
		panic(any(err))
	}
	if block == nil {
		return nil
	}
	object, err := block.GetColumn(idx)
	if err != nil {
		panic(any(err))
	}
	return object
}
