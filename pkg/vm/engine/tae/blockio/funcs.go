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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

func LoadColumnsData(
	ctx context.Context,
	metaType objectio.DataMetaType,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	m *mpool.MPool,
	policy fileservice.Policy,
) (bat *batch.Batch, release func(), err error) {
	name := location.Name()
	var meta objectio.ObjectMeta
	var ioVectors *fileservice.IOVector
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	dataMeta := meta.MustGetMeta(metaType)
	if ioVectors, err = objectio.ReadOneBlock(ctx, &dataMeta, name.String(), location.ID(), cols, typs, m, fs, policy); err != nil {
		return
	}
	release = func() {
		objectio.ReleaseIOVector(ioVectors)
	}
	bat = batch.NewWithSize(len(cols))
	var obj any
	for i := range cols {
		obj, err = objectio.Decode(ioVectors.Entries[i].CachedData.Bytes())
		if err != nil {
			return
		}
		bat.Vecs[i] = obj.(*vector.Vector)
		bat.SetRowCount(bat.Vecs[i].Length())
	}
	//TODO call CachedData.Release
	return
}

func LoadColumnsData2(
	ctx context.Context,
	metaType objectio.DataMetaType,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	policy fileservice.Policy,
	needCopy bool,
	vPool *containers.VectorPool,
) (vectors []containers.Vector, release func(), err error) {
	name := location.Name()
	var meta objectio.ObjectMeta
	var ioVectors *fileservice.IOVector
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	dataMeta := meta.MustGetMeta(metaType)
	if ioVectors, err = objectio.ReadOneBlock(ctx, &dataMeta, name.String(), location.ID(), cols, typs, nil, fs, policy); err != nil {
		return
	}
	defer func() {
		if needCopy {
			objectio.ReleaseIOVector(ioVectors)
			return
		}
		release = func() {
			objectio.ReleaseIOVector(ioVectors)
		}
	}()
	var obj any
	vectors = make([]containers.Vector, len(cols))
	for i := range cols {
		obj, err = objectio.Decode(ioVectors.Entries[i].CachedData.Bytes())
		if err != nil {
			return
		}

		var vec containers.Vector
		if needCopy {
			if vec, err = containers.CloneVector(
				obj.(*vector.Vector),
				vPool.GetAllocator(),
				vPool,
			); err != nil {
				return
			}
		} else {
			vec = containers.ToTNVector(obj.(*vector.Vector), nil)
		}
		vectors[i] = vec
	}
	if err != nil {
		for _, col := range vectors {
			if col != nil {
				col.Close()
			}
		}
		return nil, release, err
	}
	return
}

func LoadColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	m *mpool.MPool,
	policy fileservice.Policy,
) (bat *batch.Batch, release func(), err error) {
	return LoadColumnsData(ctx, objectio.SchemaData, cols, typs, fs, location, m, policy)
}

func LoadTombstoneColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	m *mpool.MPool,
) (bat *batch.Batch, release func(), err error) {
	return LoadColumnsData(ctx, objectio.SchemaTombstone, cols, typs, fs, location, m, fileservice.Policy(0))
}

// LoadColumns2 load columns data from file service for TN
// need to copy data from vPool to avoid releasing cache
func LoadColumns2(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	policy fileservice.Policy,
	needCopy bool,
	vPool *containers.VectorPool,
) (vectors []containers.Vector, release func(), err error) {
	return LoadColumnsData2(ctx, objectio.SchemaData, cols, typs, fs, location, policy, needCopy, vPool)
}

// LoadTombstoneColumns2 load tombstone data from file service for TN
// need to copy data from vPool to avoid releasing cache
func LoadTombstoneColumns2(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	needCopy bool,
	vPool *containers.VectorPool,
) (vectors []containers.Vector, release func(), err error) {
	return LoadColumnsData2(ctx, objectio.SchemaTombstone, cols, typs, fs, location, fileservice.Policy(0), needCopy, vPool)
}

func LoadOneBlock(
	ctx context.Context,
	fs fileservice.FileService,
	key objectio.Location,
	metaType objectio.DataMetaType,
) (*batch.Batch, error) {
	meta, err := objectio.FastLoadObjectMeta(ctx, &key, false, fs)
	if err != nil {
		return nil, err
	}
	data := meta.MustGetMeta(metaType)

	idxes := make([]uint16, data.BlockHeader().ColumnCount())
	for i := range idxes {
		idxes[i] = uint16(i)
	}
	bat, err := objectio.ReadOneBlockAllColumns(ctx, &data, key.Name().String(),
		uint32(key.ID()), idxes, fileservice.SkipAllCache, fs)
	return bat, err
}
