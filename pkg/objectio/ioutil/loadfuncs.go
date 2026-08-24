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

// GetMetaForLocation validates that an object has the requested metadata and
// that the block belongs to it. Callers use this before metadata access so a
// malformed or mismatched location becomes an error instead of a Must* panic.
func GetMetaForLocation(
	meta objectio.ObjectMeta,
	location objectio.Location,
	metaType objectio.DataMetaType,
) (dataMeta objectio.ObjectDataMeta, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			dataMeta = nil
			err = moerr.NewInvalidInputNoCtxf(
				"object metadata for block %d is malformed: %v", location.ID(), recovered,
			)
		}
	}()
	dataMeta = meta.MustGetMeta(metaType)
	if dataMeta == nil {
		return nil, moerr.NewInvalidInputNoCtxf(
			"object has no metadata of type %d", metaType,
		)
	}
	blockID := uint32(location.ID())
	startID := uint32(dataMeta.BlockHeader().StartID())
	if blockID < startID || blockID-startID >= dataMeta.BlockCount() {
		return nil, moerr.NewInvalidInputNoCtxf(
			"object block %d is outside metadata range [%d,%d)",
			location.ID(), startID, uint64(startID)+uint64(dataMeta.BlockCount()),
		)
	}
	// Force the selected block header and the declared metadata span to be
	// decoded while the recovery guard above is active. Range-valid block-index
	// entries can still point at truncated block metadata.
	block := dataMeta.GetBlockMeta(blockID)
	metaColumnCount := block.GetMetaColumnCount()
	if block.GetColumnCount() > metaColumnCount {
		return nil, moerr.NewInvalidInputNoCtxf(
			"object block %d declares %d columns but only %d metadata slots",
			location.ID(), block.GetColumnCount(), metaColumnCount,
		)
	}
	if metaColumnCount > 0 {
		_ = block.ColumnMeta(metaColumnCount - 1).DataType()
	}
	return dataMeta, nil
}

func GetDataMetaForLocation(
	meta objectio.ObjectMeta,
	location objectio.Location,
) (objectio.ObjectDataMeta, error) {
	return GetMetaForLocation(meta, location, objectio.SchemaData)
}

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
	if ctx == nil || fs == nil {
		return nil, nil, false, moerr.NewInvalidInputNoCtx(
			"column load requires context and file service",
		)
	}
	if len(columns) != len(typs) || len(cacheVectors) < len(columns) {
		return nil, nil, false, moerr.NewInvalidInputNoCtxf(
			"column load has %d columns, %d types, and %d cache vectors",
			len(columns), len(typs), len(cacheVectors),
		)
	}
	name := location.Name().UnsafeString()
	var meta objectio.ObjectMeta
	var vectors fileservice.IOVector
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	if dataMeta, err = GetDataMetaForLocation(meta, location); err != nil {
		return
	}
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
	if len(vectors.Entries) != len(columns) {
		objectio.ReleaseIOVector(&vectors)
		return nil, nil, false, moerr.NewInternalErrorNoCtxf(
			"object block returned %d columns, expected %d",
			len(vectors.Entries), len(columns),
		)
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
	if ctx == nil || fs == nil {
		return ioVectors, false, moerr.NewInvalidInputNoCtx(
			"object column read requires context and file service",
		)
	}
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
		readColumns = make([]uint16, len(columns), len(columns)+2)
		copy(readColumns, columns)
		readColumns = append(readColumns, *extraTSColumn, objectio.SEQNUM_ABORT)
		readTypes = make([]types.Type, len(typs), len(typs)+2)
		copy(readTypes, typs)
		readTypes = append(readTypes, objectio.TSType, types.T_bool.ToType())
	}

	name := location.Name().UnsafeString()
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return ioVectors, false, err
	}
	dataMeta, err := GetDataMetaForLocation(meta, location)
	if err != nil {
		return ioVectors, false, err
	}
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
		var aborts vector.Vector
		if err = objectio.MustVectorToCached(
			&aborts,
			ioVectors.Entries[len(columns)+1].CachedData,
		); err != nil {
			return
		}
		defer aborts.Free(nil)
		hasAborts := !aborts.IsConstNull()
		if hasAborts && (aborts.GetType().Oid != types.T_bool || aborts.Length() != commits.Length()) {
			err = moerr.NewInvalidInputNoCtx("object abort column is unavailable")
			return
		}

		deleteMask = objectio.GetReusableBitmap()
		for i := 0; i < commits.Length(); i++ {
			if commits.IsNull(uint64(i)) {
				err = moerr.NewInvalidInputNoCtxf("object commit-ts row %d is null", i)
				return
			}
			if hasAborts && aborts.IsNull(uint64(i)) {
				err = moerr.NewInvalidInputNoCtxf("object abort row %d is null", i)
				return
			}
			commit := vector.GetFixedAtNoTypeCheck[types.TS](&commits, i)
			if commit.GT(visibilityTS) ||
				(hasAborts && vector.GetFixedAtNoTypeCheck[bool](&aborts, i)) {
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
		sels, err = objectio.FilterCachedRowsByCommitTSAndAbort(
			ioVectors.Entries[1].CachedData,
			ioVectors.Entries[2].CachedData,
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
		[]uint16{column, commitTSColumn, objectio.SEQNUM_ABORT},
		[]types.Type{typ, objectio.TSType, types.T_bool.ToType()},
		fs,
		location,
		nil,
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
	matched, usable, err = objectio.AnyCachedTSInRangeWithAbort(
		ioVectors.Entries[1].CachedData,
		ioVectors.Entries[2].CachedData,
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
	if ctx == nil || fs == nil || (needCopy && vPool == nil) {
		return nil, nil, moerr.NewInvalidInputNoCtx(
			"TN column load requires context, file service, and a vector pool when copying",
		)
	}
	// A nil type slice is the long-standing fast path for objects without
	// schema changes. Types are required only when ReadOneBlock must synthesize
	// a missing column.
	if len(typs) != 0 && len(cols) != len(typs) {
		return nil, nil, moerr.NewInvalidInputNoCtxf(
			"object column count %d does not match type count %d", len(cols), len(typs),
		)
	}
	name := location.Name()
	var meta objectio.ObjectMeta
	var ioVectors fileservice.IOVector
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	dataMeta, err := GetDataMetaForLocation(meta, location)
	if err != nil {
		return nil, nil, err
	}
	if ioVectors, err = objectio.ReadOneBlock(ctx, &dataMeta, name.UnsafeString(), location.ID(), cols, typs, nil, fs, policy); err != nil {
		return
	}
	if len(ioVectors.Entries) != len(cols) {
		objectio.ReleaseIOVector(&ioVectors)
		return nil, nil, moerr.NewInternalErrorNoCtxf(
			"object block returned %d columns, expected %d", len(ioVectors.Entries), len(cols),
		)
	}
	blockMeta := dataMeta.GetBlockMeta(uint32(location.ID()))
	expectedRows := int(blockMeta.GetRows())
	specialLayout := objectio.ResolveSpecialColumnLayout(blockMeta)
	storedColumnType := func(seqnum uint16) (types.T, bool) {
		if seqnum >= objectio.SEQNUM_UPPER {
			var ok bool
			seqnum, ok = specialLayout.Resolve(seqnum)
			if !ok {
				return types.T_any, false
			}
		}
		if seqnum >= blockMeta.GetMetaColumnCount() {
			return types.T_any, false
		}
		oid := types.T(blockMeta.ColumnMeta(seqnum).DataType())
		return oid, oid != types.T_any
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
	cleanVectors := func() {
		for _, col := range vectors {
			if col != nil {
				col.Close()
			}
		}
		vectors = nil
	}
	var obj any
	for i := range cols {
		obj, err = objectio.DecodeCached(ioVectors.Entries[i].CachedData)
		if err != nil {
			cleanVectors()
			return
		}
		decoded, ok := obj.(*vector.Vector)
		if !ok || decoded == nil {
			err = moerr.NewInternalErrorNoCtxf(
				"decoded object column %d has type %T, expected vector", i, obj,
			)
			cleanVectors()
			return
		}
		if decoded.Length() != expectedRows {
			err = moerr.NewInvalidInputNoCtxf(
				"decoded object column %d has %d rows, expected %d",
				cols[i], decoded.Length(), expectedRows,
			)
			cleanVectors()
			return
		}
		if len(typs) != 0 {
			// Width, scale, and other logical metadata can differ across schema
			// versions while the persisted physical representation remains
			// compatible. Match the existing Union boundary and validate OID.
			if decoded.GetType().Oid != typs[i].Oid {
				err = moerr.NewInvalidInputNoCtxf(
					"decoded object column %d has type %s, expected %s",
					cols[i], decoded.GetType().String(), typs[i].String(),
				)
				cleanVectors()
				return
			}
		} else if expectedType, ok := storedColumnType(cols[i]); !ok || decoded.GetType().Oid != expectedType {
			err = moerr.NewInvalidInputNoCtxf(
				"decoded object column %d has type %s inconsistent with metadata type %s",
				cols[i], decoded.GetType().String(), expectedType.String(),
			)
			cleanVectors()
			return
		}

		var vec containers.Vector
		if needCopy {
			if vec, err = containers.CloneVector(
				decoded,
				vPool.GetMPool(),
				vPool,
			); err != nil {
				cleanVectors()
				return
			}
		} else {
			vec = containers.ToTNVector(decoded, nil)
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
	bat, sortKey, _, err := LoadOneBlockWithSpecialLayout(ctx, fs, key, metaType)
	return bat, sortKey, err
}

// LoadOneBlockWithSpecialLayout returns special-column positions in the loaded
// compact batch. Callers that expose logical rows must use this metadata to
// remove storage-only columns without guessing from vector types or the
// physical metadata sequence number.
func LoadOneBlockWithSpecialLayout(
	ctx context.Context,
	fs fileservice.FileService,
	key objectio.Location,
	metaType objectio.DataMetaType,
) (*batch.Batch, uint16, objectio.SpecialColumnLayout, error) {
	bat, sortKey, layout, _, err := LoadOneBlockWithColumnLayout(ctx, fs, key, metaType)
	return bat, sortKey, layout, err
}

// LoadOneBlockWithColumnLayout loads every physically present column in the
// order in which the vectors were written and returns the corresponding
// metadata sequence number for each vector. Sequence numbers are stable across
// schema changes and can therefore contain gaps after DROP COLUMN; they are not
// interchangeable with positions in the returned compact batch.
//
// The returned sort-key and special-column positions are translated to compact
// batch positions. Callers that need to map user columns across schema versions
// must use columnSeqnums instead of assuming position == sequence number.
func LoadOneBlockWithColumnLayout(
	ctx context.Context,
	fs fileservice.FileService,
	key objectio.Location,
	metaType objectio.DataMetaType,
) (
	bat *batch.Batch,
	sortKey uint16,
	layout objectio.SpecialColumnLayout,
	columnSeqnums []uint16,
	err error,
) {
	sortKey = uint16(math.MaxUint16)
	layout = objectio.SpecialColumnLayout{
		PhysicalAddr: objectio.InvalidSpecialColumnPosition,
		CommitTS:     objectio.InvalidSpecialColumnPosition,
		Abort:        objectio.InvalidSpecialColumnPosition,
	}
	if ctx == nil || fs == nil {
		return nil, sortKey, layout, nil, moerr.NewInvalidInputNoCtx(
			"object block load requires context and file service",
		)
	}
	meta, err := objectio.FastLoadObjectMeta(ctx, &key, false, fs)
	if err != nil {
		return nil, sortKey, layout, nil, err
	}
	data, err := GetMetaForLocation(meta, key, metaType)
	if err != nil {
		return nil, sortKey, layout, nil, err
	}
	header := data.BlockHeader()
	blockID := uint32(key.ID())
	block := data.GetBlockMeta(blockID)
	columnCount := int(block.GetColumnCount())
	columnSeqnums = make([]uint16, columnCount)
	seenWriterPositions := make([]bool, columnCount)
	found := 0
	for seqnum := uint16(0); seqnum < block.GetMetaColumnCount(); seqnum++ {
		column := block.ColumnMeta(seqnum)
		if column.DataType() == 0 {
			continue
		}
		writerPosition := int(column.Idx())
		if writerPosition < 0 || writerPosition >= columnCount || seenWriterPositions[writerPosition] {
			return nil, sortKey, layout, nil, moerr.NewInternalErrorNoCtxf(
				"object block %d has invalid writer position %d for sequence %d",
				blockID, writerPosition, seqnum,
			)
		}
		seenWriterPositions[writerPosition] = true
		columnSeqnums[writerPosition] = seqnum
		found++
	}
	if found != columnCount {
		return nil, sortKey, layout, nil, moerr.NewInternalErrorNoCtxf(
			"object block %d declares %d columns but maps %d",
			blockID, columnCount, found,
		)
	}

	translatePosition := func(physical uint16) uint16 {
		if physical == objectio.InvalidSpecialColumnPosition {
			return objectio.InvalidSpecialColumnPosition
		}
		for position, seqnum := range columnSeqnums {
			if seqnum == physical {
				return uint16(position)
			}
		}
		return objectio.InvalidSpecialColumnPosition
	}
	physicalLayout := objectio.ResolveSpecialColumnLayout(block)
	layout = objectio.SpecialColumnLayout{
		PhysicalAddr: translatePosition(physicalLayout.PhysicalAddr),
		CommitTS:     translatePosition(physicalLayout.CommitTS),
		Abort:        translatePosition(physicalLayout.Abort),
	}
	if header.Appendable() {
		physicalSortKey := header.SortKey()
		if position := translatePosition(physicalSortKey); position != objectio.InvalidSpecialColumnPosition {
			sortKey = position
		} else if physicalSortKey != math.MaxUint16 {
			return nil, sortKey, layout, nil, moerr.NewInternalErrorNoCtxf(
				"appendable object block %d references missing sort-key sequence %d",
				blockID, physicalSortKey,
			)
		}
	}
	bat, err = objectio.ReadOneBlockAllColumns(
		ctx, &data, key.Name().String(), blockID, columnSeqnums, fileservice.SkipAllCache, fs,
	)
	if err != nil {
		return nil, sortKey, layout, nil, err
	}
	return bat, sortKey, layout, columnSeqnums, nil
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
	data, err := GetMetaForLocation(meta, key, metaType)
	if err != nil {
		return nil, sortKey, err
	}
	if data.BlockHeader().Appendable() {
		sortKey = data.BlockHeader().SortKey()
	}
	bat, err := objectio.ReadOneBlockAllColumns(ctx, &data, key.Name().String(),
		uint32(key.ID()), idxes, fileservice.SkipAllCache, fs)
	return bat, sortKey, err
}
