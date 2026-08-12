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

package ioutil

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func LoadColumnsData(
	ctx context.Context,
	columns []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	cacheVectors containers.Vectors, // cacheVectors.Allocated() must be 0
	m *mpool.MPool,
	policy fileservice.Policy,
) (dataMeta objectio.ObjectDataMeta, release func(), fromCache bool, err error) {
	name := location.Name().UnsafeString()
	var meta objectio.ObjectMeta
	var vectors fileservice.IOVector
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	dataMeta = meta.MustGetMeta(objectio.SchemaData)
	if vectors, err = objectio.ReadOneBlock(
		ctx,
		&dataMeta,
		name,
		location.ID(),
		columns,
		typs,
		m,
		fs,
		policy,
	); err != nil {
		return
	}
	// fromCache is true only when every entry was served from cache.
	fromCache = len(vectors.Entries) > 0
	for _, entry := range vectors.Entries {
		if !entry.WasFromCache() {
			fromCache = false
			break
		}
	}
	release = func() {
		objectio.ReleaseIOVector(&vectors)
		cacheVectors.Free(m)
	}
	for i := range columns {
		if err = objectio.MustVectorToCachedWithMpool(&cacheVectors[i], vectors.Entries[i].CachedData, m); err != nil {
			logutil.Errorf("LoadColumnsData %s error: %v", location.String(), err.Error())
			release()
			release = nil
			return
		}
	}
	return
}

func readColumnsData(
	ctx context.Context,
	columns []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	extraTSColumn *uint16,
	m *mpool.MPool,
	policy fileservice.Policy,
) (ioVectors fileservice.IOVector, fromCache bool, err error) {
	if len(columns) != len(typs) {
		return ioVectors, false, moerr.NewInvalidInputNoCtxf(
			"object column count %d does not match type count %d",
			len(columns), len(typs),
		)
	}
	if len(columns) == 0 && extraTSColumn == nil {
		return
	}

	readColumns := columns
	readTypes := typs
	if extraTSColumn != nil {
		readColumns = make([]uint16, len(columns), len(columns)+1)
		copy(readColumns, columns)
		readColumns = append(readColumns, *extraTSColumn)
		readTypes = make([]types.Type, len(typs), len(typs)+1)
		copy(readTypes, typs)
		readTypes = append(readTypes, objectio.TSType)
	}

	name := location.Name().UnsafeString()
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return ioVectors, false, err
	}
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	ioVectors, err = objectio.ReadOneBlock(
		ctx,
		&dataMeta,
		name,
		location.ID(),
		readColumns,
		readTypes,
		m,
		fs,
		policy,
	)
	if err != nil {
		return ioVectors, false, err
	}
	fromCache = len(ioVectors.Entries) > 0
	for i := range ioVectors.Entries {
		if !ioVectors.Entries[i].WasFromCache() {
			fromCache = false
			break
		}
	}
	return
}

// LoadColumnsDataInto reads a block while its FileService cache entries are
// pinned and materializes directly into caller-owned destination Vectors. A
// nil sels copies every row; a non-nil sels copies only the selected rows.
//
// visibilityTS is non-nil for appendable blocks. In that case the internal
// commit-ts column is read in the same IOVector and rows newer than the
// snapshot are returned in deleteMask. The commit-ts Vector never escapes this
// scoped read. On an allocation or decode error, destinations may contain a
// successfully materialized prefix; ownership remains with the caller.
func LoadColumnsDataInto(
	ctx context.Context,
	columns []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	destinations []*vector.Vector,
	sels []int64,
	visibilityTS *types.TS,
	m *mpool.MPool,
	policy fileservice.Policy,
) (deleteMask objectio.Bitmap, fromCache bool, err error) {
	if len(columns) != len(destinations) {
		err = moerr.NewInvalidInputNoCtxf(
			"object column count %d does not match destination count %d",
			len(columns), len(destinations),
		)
		return
	}
	if len(columns) != len(typs) {
		err = moerr.NewInvalidInputNoCtxf(
			"object column count %d does not match type count %d",
			len(columns), len(typs),
		)
		return
	}
	for i := range destinations {
		if destinations[i] == nil {
			err = moerr.NewInvalidInputNoCtxf("nil destination for object column %d", columns[i])
			return
		}
	}
	if (len(columns) > 0 || visibilityTS != nil) && m == nil {
		err = moerr.NewInvalidInputNoCtx("nil mpool for object column materialization")
		return
	}
	if visibilityTS != nil && sels != nil {
		err = moerr.NewInvalidInputNoCtx("selected-row materialization cannot return a block-coordinate visibility mask")
		return
	}
	var commitTSColumn *uint16
	if visibilityTS != nil {
		column := uint16(objectio.SEQNUM_COMMITTS)
		commitTSColumn = &column
	}
	ioVectors, fromCache, err := readColumnsData(
		ctx,
		columns,
		typs,
		fs,
		location,
		commitTSColumn,
		m,
		policy,
	)
	if err != nil {
		return deleteMask, false, err
	}
	defer objectio.ReleaseIOVector(&ioVectors)
	defer func() {
		if err != nil {
			deleteMask.Release()
			deleteMask = objectio.NullBitmap
		}
	}()

	if visibilityTS != nil {
		var commits vector.Vector
		if err = objectio.MustVectorToCached(
			&commits,
			ioVectors.Entries[len(columns)].CachedData,
		); err != nil {
			return
		}
		defer commits.Free(nil)
		if commits.GetType().Oid != types.T_TS || commits.IsConstNull() {
			err = moerr.NewInvalidInputNoCtx("object commit-ts column is unavailable")
			return
		}

		deleteMask = objectio.GetReusableBitmap()
		for i := 0; i < commits.Length(); i++ {
			if commits.IsNull(uint64(i)) {
				err = moerr.NewInvalidInputNoCtxf("object commit-ts row %d is null", i)
				return
			}
			commit := vector.GetFixedAtNoTypeCheck[types.TS](&commits, i)
			if commit.GT(visibilityTS) {
				deleteMask.Add(uint64(i))
			}
		}
	}

	for i := range columns {
		if sels == nil {
			err = objectio.CopyCachedVectorAll(
				destinations[i],
				ioVectors.Entries[i].CachedData,
				m,
			)
		} else {
			err = objectio.CopyCachedVectorRows(
				destinations[i],
				ioVectors.Entries[i].CachedData,
				sels,
				m,
			)
		}
		if err != nil {
			logutil.Errorf("LoadColumnsDataInto %s error: %v", location.String(), err)
			return
		}
	}
	return
}

// LoadColumnDataBySearch executes a fixed supported varlen search while the
// cache entry is pinned. The cache-backed Vector remains private to ObjectIO.
func LoadColumnDataBySearch(
	ctx context.Context,
	column uint16,
	typ types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	search *objectio.ReadFilterSearch,
	sorted bool,
	visibilityTS *types.TS,
	m *mpool.MPool,
	policy fileservice.Policy,
) (sels []int64, fromCache bool, err error) {
	if m == nil {
		return nil, false, moerr.NewInvalidInputNoCtx("nil mpool for object column search")
	}
	var commitTSColumn *uint16
	if visibilityTS != nil {
		column := uint16(objectio.SEQNUM_COMMITTS)
		commitTSColumn = &column
	}
	ioVectors, fromCache, err := readColumnsData(
		ctx,
		[]uint16{column},
		[]types.Type{typ},
		fs,
		location,
		commitTSColumn,
		m,
		policy,
	)
	if err != nil {
		return nil, false, err
	}
	defer objectio.ReleaseIOVector(&ioVectors)

	sels, err = objectio.SearchCachedVector(
		ioVectors.Entries[0],
		search,
		sorted,
	)
	if err != nil {
		return nil, false, err
	}
	if visibilityTS != nil {
		sels, err = objectio.FilterCachedRowsByCommitTS(
			ioVectors.Entries[1].CachedData,
			sels,
			*visibilityTS,
		)
		if err != nil {
			return nil, false, err
		}
	}
	return
}

// LoadColumnDataByTopN computes vector TopN while the FileService cache entry
// is pinned. The cached Vector remains private to ObjectIO and only row
// coordinates plus distances escape this function.
func LoadColumnDataByTopN(
	ctx context.Context,
	column uint16,
	typ types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	selectRows []int64,
	orderByLimit *objectio.IndexReaderTopOp,
	m *mpool.MPool,
	policy fileservice.Policy,
) (sels []int64, dists []float64, fromCache bool, err error) {
	if m == nil {
		return nil, nil, false, moerr.NewInvalidInputNoCtx("nil mpool for object column topn")
	}
	ioVectors, fromCache, err := readColumnsData(
		ctx,
		[]uint16{column},
		[]types.Type{typ},
		fs,
		location,
		nil,
		m,
		policy,
	)
	if err != nil {
		return nil, nil, false, err
	}
	defer objectio.ReleaseIOVector(&ioVectors)

	sels, dists, err = objectio.SearchCachedVectorTopN(
		ctx,
		ioVectors.Entries[0],
		selectRows,
		orderByLimit,
	)
	return sels, dists, fromCache, err
}

// LoadColumnDataBySearchAndCheckTS searches a varlen column and checks whether
// any selected row's requested commit timestamp lies in (from, to].
func LoadColumnDataBySearchAndCheckTS(
	ctx context.Context,
	column uint16,
	typ types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	search *objectio.ReadFilterSearch,
	sorted bool,
	commitTSColumn uint16,
	from, to types.TS,
	m *mpool.MPool,
	policy fileservice.Policy,
) (matched bool, usable bool, fromCache bool, err error) {
	if m == nil {
		err = moerr.NewInvalidInputNoCtx("nil mpool for object column search")
		return
	}
	ioVectors, fromCache, err := readColumnsData(
		ctx,
		[]uint16{column},
		[]types.Type{typ},
		fs,
		location,
		&commitTSColumn,
		m,
		policy,
	)
	if err != nil {
		return false, false, false, err
	}
	defer objectio.ReleaseIOVector(&ioVectors)

	sels, err := objectio.SearchCachedVector(
		ioVectors.Entries[0],
		search,
		sorted,
	)
	if err != nil {
		return false, false, false, err
	}
	if len(sels) == 0 {
		return false, true, fromCache, nil
	}
	matched, usable, err = objectio.AnyCachedTSInRange(
		ioVectors.Entries[1].CachedData,
		sels,
		from,
		to,
	)
	return
}

func LoadColumnsData2(
	ctx context.Context,
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
	var ioVectors fileservice.IOVector
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	if ioVectors, err = objectio.ReadOneBlock(ctx, &dataMeta, name.UnsafeString(), location.ID(), cols, typs, nil, fs, policy); err != nil {
		return
	}
	vectors = make([]containers.Vector, len(cols))
	defer func() {
		if needCopy || err != nil {
			// needCopy: caller owns copied vectors; IOVector can be freed now.
			// err != nil: clean up IOVector internally so callers don't have
			// to call release on error paths.
			objectio.ReleaseIOVector(&ioVectors)
			return
		}
		// needCopy=false, success: caller must call release to free IOVector
		// after it is done with the zero-copy vectors.
		release = func() {
			objectio.ReleaseIOVector(&ioVectors)
		}
	}()
	var obj any
	for i := range cols {
		obj, err = objectio.DecodeCached(ioVectors.Entries[i].CachedData)
		if err != nil {
			for _, col := range vectors {
				if col != nil {
					col.Close()
				}
			}
			vectors = nil
			return
		}

		var vec containers.Vector
		if needCopy {
			if vec, err = containers.CloneVector(
				obj.(*vector.Vector),
				vPool.GetMPool(),
				vPool,
			); err != nil {
				for _, col := range vectors {
					if col != nil {
						col.Close()
					}
				}
				vectors = nil
				return
			}
		} else {
			vec = containers.ToTNVector(obj.(*vector.Vector), nil)
		}
		vectors[i] = vec
	}
	return
}

func LoadTombstoneColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	cacheVectors containers.Vectors, // cacheVectors.Allocated() must be 0
	m *mpool.MPool,
	policy fileservice.Policy,
) (meta objectio.ObjectDataMeta, release func(), err error) {
	meta, release, _, err = LoadColumnsData(
		ctx, cols, typs, fs, location, cacheVectors, m, policy,
	)
	return
}

func LoadColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	cacheVectors containers.Vectors, // Allocated() must be 0
	m *mpool.MPool,
	policy fileservice.Policy,
) (release func(), fromCache bool, err error) {
	_, release, fromCache, err = LoadColumnsData(
		ctx, cols, typs, fs, location, cacheVectors, m, policy,
	)
	return
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
	return LoadColumnsData2(ctx, cols, typs, fs, location, policy, needCopy, vPool)
}

func LoadOneBlock(
	ctx context.Context,
	fs fileservice.FileService,
	key objectio.Location,
	metaType objectio.DataMetaType,
) (*batch.Batch, uint16, error) {
	sortKey := uint16(math.MaxUint16)
	meta, err := objectio.FastLoadObjectMeta(ctx, &key, false, fs)
	if err != nil {
		return nil, sortKey, err
	}
	data := meta.MustGetMeta(metaType)
	if data.BlockHeader().Appendable() {
		sortKey = data.BlockHeader().SortKey()
	}
	idxes := make([]uint16, data.BlockHeader().ColumnCount())
	for i := range idxes {
		idxes[i] = uint16(i)
	}
	bat, err := objectio.ReadOneBlockAllColumns(ctx, &data, key.Name().String(),
		uint32(key.ID()), idxes, fileservice.SkipAllCache, fs)
	return bat, sortKey, err
}

func LoadOneBlockWithIndex(
	ctx context.Context,
	fs fileservice.FileService,
	idxes []uint16,
	key objectio.Location,
	metaType objectio.DataMetaType,
) (*batch.Batch, uint16, error) {
	sortKey := uint16(math.MaxUint16)
	meta, err := objectio.FastLoadObjectMeta(ctx, &key, false, fs)
	if err != nil {
		return nil, sortKey, err
	}
	data := meta.MustGetMeta(metaType)
	if data.BlockHeader().Appendable() {
		sortKey = data.BlockHeader().SortKey()
	}
	bat, err := objectio.ReadOneBlockAllColumns(ctx, &data, key.Name().String(),
		uint32(key.ID()), idxes, fileservice.SkipAllCache, fs)
	return bat, sortKey, err
}
