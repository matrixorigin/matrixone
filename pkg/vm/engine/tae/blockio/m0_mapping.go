// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// m0ColumnRead is a per-call projection of an already validated object. It is
// not a classifier or a source of schema-evolution authority. Its caller still
// owns relation/snapshot selection and the complete typed-prefix validation.
// No missing normal column in this mapping is sent to ObjectIO for NULL fill.
type m0ColumnRead struct {
	columns   []uint16
	types     []types.Type
	positions []int
	nulls     []int
	rows      int
}

func mapM0Columns(
	ctx context.Context, columns []uint16, typs []types.Type,
	fs fileservice.FileService, location objectio.Location,
	readLayouts ...*m0read.ObjectReadLayout,
) (*m0ColumnRead, error) {
	if len(readLayouts) == 0 || readLayouts[0] == nil {
		return nil, nil
	}
	proof := readLayouts[0]
	if proof.BlockScoped && proof.BlockID != location.ID() {
		return nil, m0read.ErrMissingColumnWithoutProof
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(columns) != len(typs) {
		return nil, moerr.NewInvalidInputNoCtx("M0 read column/type count mismatch")
	}
	if proof.ObjectName != location.Name().String() {
		return nil, m0read.ErrMissingColumnWithoutProof
	}
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return nil, err
	}
	data := meta.MustGetMeta(objectio.SchemaData)
	if uint32(location.ID()) >= data.BlockCount() {
		return nil, moerr.NewInvalidInputNoCtx("M0 read block outside object")
	}
	blk := data.GetBlockMeta(uint32(location.ID()))
	if proof.Evolution && (uint32(blk.GetMaxSeqnum())+1 != proof.WrittenSize || uint32(blk.GetColumnCount()) < proof.WrittenSize) {
		return nil, m0read.ErrMissingColumnWithoutProof
	}
	mapping := &m0ColumnRead{rows: int(blk.GetRows())}
	for i, seq := range columns {
		if seq >= objectio.SEQNUM_UPPER {
			mapping.columns = append(mapping.columns, seq)
			mapping.types = append(mapping.types, typs[i])
			mapping.positions = append(mapping.positions, i)
			continue
		}
		if proof.Evolution && uint32(seq) >= proof.SchemaSize {
			return nil, m0read.ErrMissingColumnWithoutProof
		}
		missing := seq > blk.GetMaxSeqnum() || blk.ColumnMeta(seq).DataType() == 0
		if missing {
			allowed := proof.Evolution && uint32(seq) >= proof.WrittenSize && uint32(seq) < proof.SchemaSize
			for _, legacy := range proof.LegacyNullColumns {
				allowed = allowed || seq == legacy
			}
			if !allowed {
				return nil, m0read.ErrMissingColumnWithoutProof
			}
			mapping.nulls = append(mapping.nulls, i)
			continue
		}
		if proof.Evolution && uint32(seq) >= proof.WrittenSize {
			return nil, m0read.ErrMissingColumnWithoutProof
		}
		if blk.ColumnMeta(seq).DataType() != uint8(typs[i].Oid) {
			return nil, m0read.ErrMissingColumnWithoutProof
		}
		mapping.columns = append(mapping.columns, seq)
		mapping.types = append(mapping.types, typs[i])
		mapping.positions = append(mapping.positions, i)
	}
	if len(mapping.nulls) == 0 {
		return nil, nil
	}

	return mapping, nil
}

func loadColumns(
	ctx context.Context, columns []uint16, typs []types.Type,
	fs fileservice.FileService, location objectio.Location,
	cache containers.Vectors, mp *mpool.MPool, policy fileservice.Policy,
	readLayouts ...*m0read.ObjectReadLayout,
) (func(), bool, error) {
	mapping, err := mapM0Columns(ctx, columns, typs, fs, location,
		readLayouts...,
	)
	if err != nil {
		return nil, false, err
	}
	if mapping == nil {
		return ioutil.LoadColumns(ctx, columns, typs, fs, location, cache, mp, policy)
	}
	if len(cache) < len(columns) {
		return nil, false, moerr.NewInvalidInputNoCtx("M0 read cache vector count mismatch")
	}
	physical := containers.NewVectors(len(mapping.columns))
	var releasePhysical func()
	fromCache := false
	if len(mapping.columns) > 0 {
		releasePhysical, fromCache, err = ioutil.LoadColumns(
			ctx, mapping.columns, mapping.types, fs, location, physical, mp, policy,
		)
		if err != nil {
			return nil, false, err
		}
	}
	for i, pos := range mapping.positions {
		cache[pos] = physical[i]
		physical[i] = vector.Vector{} // Transfer the vector, not its backing lease.
	}
	for _, pos := range mapping.nulls {
		cache[pos] = *vector.NewConstNull(typs[pos], mapping.rows, mp)
	}
	return func() {
		cache.Free(mp)
		if releasePhysical != nil {
			releasePhysical()
		}
	}, fromCache, nil
}

func loadColumnsDataInto(
	ctx context.Context, columns []uint16, typs []types.Type,
	fs fileservice.FileService, location objectio.Location,
	destinations []*vector.Vector, sels []int64, visibilityTS *types.TS,
	mp *mpool.MPool, policy fileservice.Policy,
	readLayouts ...*m0read.ObjectReadLayout,
) (objectio.Bitmap, bool, error) {
	mapping, err := mapM0Columns(ctx, columns, typs, fs, location,
		readLayouts...,
	)
	if err != nil {
		return objectio.NullBitmap, false, err
	}
	if mapping == nil {
		if sels == nil && visibilityTS == nil {
			fromCache, err := ioutil.LoadColumnsDataIntoReadonly(ctx, columns, typs, fs, location, destinations, mp, policy)
			return objectio.NullBitmap, fromCache, err
		}
		return ioutil.LoadColumnsDataInto(ctx, columns, typs, fs, location, destinations, sels, visibilityTS, mp, policy)
	}
	if visibilityTS != nil || len(destinations) != len(columns) || mp == nil {
		return objectio.NullBitmap, false, moerr.NewInvalidInputNoCtx("invalid M0 persisted materialization contract")
	}
	for i, dst := range destinations {
		if dst == nil || *dst.GetType() != typs[i] {
			return objectio.NullBitmap, false, moerr.NewInvalidInputNoCtx("M0 destination type mismatch")
		}
	}
	rows := mapping.rows
	if sels != nil {
		for _, row := range sels {
			if row < 0 || row >= int64(rows) {
				return objectio.NullBitmap, false, moerr.NewInvalidInputNoCtx("M0 selected row outside block")
			}
		}
		rows = len(sels)
	}
	physical := make([]*vector.Vector, len(mapping.positions))
	for i, pos := range mapping.positions {
		physical[i] = destinations[pos]
	}
	mask, fromCache, err := ioutil.LoadColumnsDataInto(
		ctx, mapping.columns, mapping.types, fs, location, physical, sels, nil, mp, policy,
	)
	if err != nil {
		return mask, false, err
	}
	for _, pos := range mapping.nulls {
		null := vector.NewConstNull(typs[pos], rows, mp)
		err = destinations[pos].UnionBatch(null, 0, rows, nil, mp)
		null.Free(mp)
		if err != nil {
			mask.Release()
			return objectio.NullBitmap, false, err
		}
	}
	return mask, fromCache, nil
}

// LoadColumns2 preserves the TN loader's ownership contract: callers close
// returned vectors before releasing their backing I/O lease. NULL vectors are
// engine-owned and never serialized through the ObjectIO cache.
func LoadColumns2(
	ctx context.Context, columns []uint16, typs []types.Type,
	fs fileservice.FileService, location objectio.Location,
	policy fileservice.Policy, needCopy bool, pool *containers.VectorPool,
	readLayouts ...*m0read.ObjectReadLayout,
) ([]containers.Vector, func(), error) {
	mapping, err := mapM0Columns(ctx, columns, typs, fs, location,
		readLayouts...,
	)
	if err != nil {
		return nil, nil, err
	}
	if mapping == nil {
		return ioutil.LoadColumns2(ctx, columns, typs, fs, location, policy, needCopy, pool)
	}
	physical, release, err := ioutil.LoadColumns2(
		ctx, mapping.columns, mapping.types, fs, location, policy, needCopy, pool,
	)
	if err != nil {
		return nil, nil, err
	}
	result := make([]containers.Vector, len(columns))
	for i, pos := range mapping.positions {
		result[pos] = physical[i]
	}
	for _, pos := range mapping.nulls {
		result[pos] = containers.ToTNVector(vector.NewConstNull(typs[pos], mapping.rows, pool.GetMPool()), pool.GetMPool())
	}
	return result, release, nil
}

// loadColumnTopN keeps cached scoring for physical columns. An authorized
// missing suffix is materialized by Engine and scored by the same TopN logic;
// physical ObjectIO is never asked to interpret schema evolution.
func loadColumnTopN(ctx context.Context, column uint16, typ types.Type, fs fileservice.FileService, location objectio.Location, rows []int64, top *objectio.IndexReaderTopOp, mp *mpool.MPool, policy fileservice.Policy,
	readLayouts ...*m0read.ObjectReadLayout,
) ([]int64, []float64, bool, error) {
	mapping, err := mapM0Columns(ctx, []uint16{column}, []types.Type{typ}, fs, location,
		readLayouts...,
	)
	if err != nil {
		return nil, nil, false, err
	}
	if mapping == nil {
		return ioutil.LoadColumnDataByTopN(ctx, column, typ, fs, location, rows, top, mp, policy)
	}
	// A one-column mapping can be non-nil only when that column is an
	// authorized NULL suffix. Reuse its validated row count without a second
	// metadata load or a physical payload read.
	value := vector.NewConstNull(typ, mapping.rows, mp)
	defer value.Free(mp)
	selected, distances, err := objectio.TopNVector(ctx, rows, value, top)
	return selected, distances, false, err
}
